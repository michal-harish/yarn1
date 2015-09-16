package org.apache.yarn1;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class YarnMaster implements AMRMClientAsync.CallbackHandler {

    private static final Logger log = LoggerFactory.getLogger(YarnMaster.class);

    /**
     * Static Main method will be executed in the ApplicationMaster container as
     * a result of the submission by YarnClient. It is a default wrapper which will
     * actually create an instance of the class passed as the first argument
     */
    public static void main(String[] args) throws Exception {
        try {
            Configuration config = new Yarn1Configuration();
            Class<? extends YarnMaster> appClass = Class.forName(config.get("yarn.master.class")).asSubclass(YarnMaster.class);
            Boolean restartCompletedContainers = config.getBoolean("yarn.container.autorestart", false);
            log.info("Starting Master Instance: " + appClass.getName() + " wutg container.autorestart = " + restartCompletedContainers);
            Constructor<? extends YarnMaster> constructor = appClass.getConstructor(Configuration.class);
            YarnMaster master = null;
            try {
                master = constructor.newInstance(config);
                master.initializeAsYarn(restartCompletedContainers);
                /**
                 * The application master instance now has an opportunity to
                 * request containers it needs in the onStartUp(args), e.g.to request 16 containers,
                 * with priority 1, 4 cores and 1024Mb of memory:
                 * for(int i=0; i< 16; i++) {
                 *  requestContainer(1, MyAppWorker.class,1024, 4);
                 * }
                 */
                master.onStartUp(args);
                while (master.numTasks.get() > master.numCompletedTasks.get()) {
                    synchronized(master.killed) {
                        master.killed.wait(10000);
                    }
                    if (master.killed.get()) {
                        try {
                            master.rmClient.unregisterApplicationMaster(FinalApplicationStatus.KILLED, "", "");
                        } finally {
                            System.exit(3);
                        }
                    }
                }
                master.rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
            } catch (Throwable e) {
                e.printStackTrace();
                System.exit(2);
            } finally {
                if (master != null) master.onCompletion();
            }
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private AMRMClientAsync<ContainerRequest> rmClient;
    private NMClient nmClient;
    private Boolean continuousService;
    final protected Configuration config;
    final private String appName;
    final private LinkedHashMap<YarnContainer, ContainerRequest> containersToAllocate = Maps.newLinkedHashMap();
    final private AtomicInteger numTasks = new AtomicInteger(0);
    final private AtomicInteger numCompletedTasks = new AtomicInteger(0);
    final private AtomicBoolean killed = new AtomicBoolean(false);
    final private Map<ContainerId, YarnContainer> runningContainers = Maps.newConcurrentMap();

    /**
     * Default constructor can be used for local execution
     */
    public YarnMaster(Configuration config) {
        this.appName = this.getClass().getSimpleName();
        this.config = config;
    }
    /**
     * protected constructor with config is run form the above main() method which will be run inside the yarn
     * app master container
     */
    public void initializeAsYarn(Boolean continuousService) throws IOException, YarnException {
        this.continuousService = continuousService;
        rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(config);
        nmClient = NMClient.createNMClient();
        nmClient.init(config);
        rmClient.start();
        rmClient.registerApplicationMaster("", 0, "");
        nmClient.start();
        YarnClient.distributeJar(config, appName);
        // TODO pass host port and url for tracking to a generic guice servlet
    }

    protected void onStartUp(String[] args) throws Exception {
        /** To be implemented by application... **/
    }

    @Override
    public float getProgress() {
        /** To be overriden by application... - will be invoked at regular intervals**/
        if (numTasks.get() == 0) {
            return 0f;
        } else {
            return ((float) (numCompletedTasks.get())) / ((float) numTasks.get());
        }
    }

    protected void onCompletion() {
        /** To be implemented by application... **/
    }

    @Override
    public void onShutdownRequest() {
        synchronized(killed) {
            killed.set(true);
            killed.notify();
        }
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
        // TODO suspend - resume behaviour
        log.warn("NODE STATUS UPDATE NOT IMPLEMENTED");
    }

    @Override
    public void onError(Throwable e) {
        log.error("Application Master received onError event", e);
        synchronized(killed) {
            killed.set(true);
            killed.notify();
        }
    }

    final private void requestContainer(YarnContainer spec) {
        ContainerRequest containerAsk = new ContainerRequest(spec.capability, null, null, spec.priority);
        synchronized (containersToAllocate) {
            containersToAllocate.put(spec, containerAsk);
        }
        rmClient.addContainerRequest(containerAsk);
        numTasks.incrementAndGet();
    }

    final protected void requestContainerGroup(int numContainers, YarnContainerRequest spec) throws Exception {
        YarnContainerRequest[] requests = new YarnContainerRequest[numContainers];
        for (int i = 0; i < numContainers; i++) {
            requests[i] = spec;
        }
        requestContainerGroup(requests);
    }
    final protected void requestContainerGroup(YarnContainerRequest[] requests) throws Exception {
        for(YarnContainerRequest spec: requests) {
            log.info("Requesting container (" + spec.memoryMb + " x " + spec.numCores + ")" + Arrays.asList(spec.args));
            requestContainer(
                    new YarnContainer(config, spec.priority, spec.memoryMb, spec.numCores, appName, spec.mainClass, spec.args)
            );
        }
        // wait for allocation before requesting other groups
        synchronized (containersToAllocate) {
            while (containersToAllocate.size() > 0 && !killed.get()) {
                containersToAllocate.wait(250);
            }
        }

    }

    @Override
    final public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            try {
                Map.Entry<YarnContainer, ContainerRequest> selected = null;
                synchronized (containersToAllocate) {
                    for(Map.Entry<YarnContainer, ContainerRequest> entry: containersToAllocate.entrySet()) {
                        YarnContainer spec = entry.getKey();
                        if (spec.isSatisfiedBy(container)) {
                            if (selected == null) {
                                selected = entry;
                            } else {
                                YarnContainer selectedSoFar = selected.getKey();
                                if (selectedSoFar.capability.getMemory() > spec.capability.getMemory()
                                        || selectedSoFar.capability.getVirtualCores() >= spec.capability.getVirtualCores()
                                        || selectedSoFar.priority.getPriority() >= spec.priority.getPriority()) {
                                    selected = entry;
                                }
                            }
                        }
                    }
                    if (selected != null) {
                        rmClient.removeContainerRequest(selected.getValue());
                        containersToAllocate.remove(selected.getKey());
                    }
                }
                if (selected != null) {
                    YarnContainer spec = selected.getKey();
                    log.info("Launching Container " + container.getNodeHttpAddress() + " " + container + " to " + spec.mainClass);
                    log.info(getContainerUrl(container));
                    nmClient.startContainer(container, spec.createContainerLaunchContext());
                    runningContainers.put(container.getId(), spec);
                    log.info("Number of running containers = " + runningContainers.size());
                } else {
                    log.warn("Could not resolve allocated container with outstanding requested specs: " + getContainerUrl(container)
                            + ", container spec: " + container.getResource() +", priority:" + container.getPriority());
                    rmClient.releaseAssignedContainer(container.getId());
                    //FIXME rmClient seems to be re-requesting all the previously requested containers
                }

            } catch (Throwable ex) {
                log.error("Error while assigning allocated container", ex);
                synchronized(killed) {
                    killed.set(true);
                    killed.notify();
                }
            }
        }
    }

    private String getContainerUrl(Container container) {
        return "http://" + container.getNodeHttpAddress() + "/node/containerlogs/" + container.getId();
    }

    @Override
    final public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            YarnContainer completedSpec = runningContainers.remove(status.getContainerId());
            if (completedSpec != null) {
                log.info("Completed container " + status.getContainerId()
                        + ", (exit status " + status.getExitStatus() + ")  " + status.getDiagnostics());
                if (continuousService) {
                    log.info("Auto-restarting container " + completedSpec);
                    requestContainer(completedSpec);
                }
                numCompletedTasks.incrementAndGet();
            }
        }
    }


}

package org.apache.yarn1;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
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
            Class<? extends YarnMaster> appClass = Class.forName(args[0]).asSubclass(YarnMaster.class);
            Boolean autorestart = Boolean.valueOf(args[1]);
            String[] originalArgs = Arrays.copyOfRange(args, 2, args.length);
            log.info("Starting Master Instance: " + appClass.getName());
            YarnMaster master = appClass.newInstance();
            try {
                master.initialize(autorestart);
                /**
                 * The application master instance now has an opportunity to
                 * request containers it needs in the onStartUp(args), e.g.to request 16 containers,
                 * with priority 1, 4 cores and 1024Mb of memory:
                 * for(int i=0; i< 16; i++) {
                 *  requestContainer(1, MyAppWorker.class,1024, 4);
                 * }
                 */
                master.onStartUp(originalArgs);
                master.waitForCompletion();
            } catch (Throwable e) {
                e.printStackTrace();
                System.exit(2);
            } finally {
                master.onCompletion();
            }
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private Configuration conf;
    private AMRMClientAsync<ContainerRequest> rmClient;
    private NMClient nmClient;
    private Boolean continuousService;
    final private String appName;
    final private List<YarnContainer> containersToAllocate = Lists.newLinkedList();
    final private AtomicInteger numTasks = new AtomicInteger(0);
    final private AtomicInteger numCompletedTasks = new AtomicInteger(0);
    final private AtomicBoolean killed = new AtomicBoolean(false);
    final private Map<ContainerId, YarnContainer> runningContainers = Maps.newConcurrentMap();

    /**
     * Default constructor is run form the above main() method ...
     * appClass.newInstance(), see that appClass must be an extension of this
     * class(YarnMaster)
     */
    public YarnMaster() {
        // TODO pass host port and url for tracking to a generic guice servlet
        this.appName = this.getClass().getSimpleName();
        conf = new YarnConfiguration();
        rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(conf);
        nmClient = NMClient.createNMClient();
        nmClient.init(conf);
    }

    private void initialize(Boolean continuousService) throws IOException, YarnException {
        this.continuousService = continuousService;
        rmClient.start();
        rmClient.registerApplicationMaster("", 0, "");
        nmClient.start();
        YarnClient.distributeJar(conf, appName);
    }

    protected void onStartUp(String[] args) throws Exception {
        /** To be implemented by application... **/
    }

    protected void onCompletion() {
        /** To be implemented by application... **/
    }

    final private void requestContainer(YarnContainer spec) {
        ContainerRequest containerAsk = new ContainerRequest(spec.capability, null, null, spec.priority);
        synchronized (containersToAllocate) {
            containersToAllocate.add(spec);
        }
        rmClient.addContainerRequest(containerAsk);
        numTasks.incrementAndGet();
    }

    final protected void requestContainerGroup(
            int numContainers, Class<?> mainClass, String[] args, int priority, int memoryMb, int numCores)
            throws Exception {
        log.info("Requesting container group: " + numContainers + " x (" + memoryMb + " x " + numCores + ")");
        for (int i = 0; i < numContainers; i++) {
            YarnContainer spec = new YarnContainer(conf, priority, memoryMb, numCores, appName, mainClass, args);
            requestContainer(spec);
        }
        // wait for allocation before requesting other groups
        synchronized (containersToAllocate) {
            while (containersToAllocate.size() > 0 && !killed.get()) {
                containersToAllocate.wait(250);
            }
        }

    }

    private void waitForCompletion() throws Exception {
        while (numTasks.get() > numCompletedTasks.get()) {
            if (killed.get()) {
                try {
                    rmClient.unregisterApplicationMaster(FinalApplicationStatus.KILLED, "", "");
                } finally {
                    System.exit(3);
                }
            }
            Thread.sleep(1000);
        }
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    }

    @Override
    final public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            try {
                YarnContainer selectedSpec = null;
                synchronized (containersToAllocate) {
                    int selected = -1;
                    for (int h = 0; h < containersToAllocate.size(); h++) {
                        YarnContainer spec = containersToAllocate.get(h);
                        if (spec.isSatisfiedBy(container)) {
                            if (selected == -1) {
                                selected = h;
                            } else {
                                YarnContainer currentlySelected = containersToAllocate.get(selected);
                                if (currentlySelected.capability.getMemory() > spec.capability.getMemory()
                                        || currentlySelected.capability.getVirtualCores() >= spec.capability.getVirtualCores()
                                        || currentlySelected.priority.getPriority() >= spec.priority.getPriority()) {
                                    selected = h;
                                }
                            }
                        }
                    }
                    if (selected > -1) {
                        selectedSpec = containersToAllocate.remove(selected);
                    }
                }
                if (selectedSpec != null) {
                    log.info("Launching Container " + container.getNodeHttpAddress() + " " + container + " to " + selectedSpec.mainClass);
                    log.info(getContainerUrl(container));
                    nmClient.startContainer(container, selectedSpec.createContainerLaunchContext());
                    runningContainers.put(container.getId(), selectedSpec);
                    log.info("Number of running containers = " + runningContainers.size());
                } else {
                    log.warn("Could not resolve allocated container with outstanding requested specs: " + getContainerUrl(container));
                    log.warn("Container spec: " + container.getResource() +", priority:" + container.getPriority());
                    rmClient.releaseAssignedContainer(container.getId());
                    //FIXME rmClient seems to be re-requesting all the previously requested containers
                }

            } catch (Throwable ex) {
                log.error("Error while assigning allocated container", ex);
                killed.set(true);
            }
        }
    }

    private String getContainerUrl(Container container) {
        return "http://" + container.getNodeHttpAddress() + "/node/containerlogs/" + container.getId();
    }

    @Override
    final public float getProgress() {
        float x;
        if (continuousService) {
            x = 0f;
            //TODO provide a way to report "lag" for continuous services
        } else if (numTasks.get() == 0) {
            x = 0f;
        } else {
            x = ((float) (numCompletedTasks.get())) / ((float) numTasks.get());
        }
        return x;
    }

    @Override
    final public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            YarnContainer completedSpec = runningContainers.remove(status.getContainerId());
            if (completedSpec != null) {
                log.info("Completed container " + status.getContainerId());
                if (continuousService) {
                    log.info("Auto-restarting container " + completedSpec);
                    requestContainer(completedSpec);
                }
                numCompletedTasks.incrementAndGet();
            }
        }
    }

    @Override
    final public void onShutdownRequest() {
        killed.set(true);
    }

    @Override
    final public void onNodesUpdated(List<NodeReport> updatedNodes) {
        // TODO suspend - resume behaviour
        log.warn("NODE STATUS UPDATE NOT IMPLEMENTED");
    }

    @Override
    final public void onError(Throwable e) {
        log.error("Application Master received onError event", e);
        killed.set(true);
    }

}

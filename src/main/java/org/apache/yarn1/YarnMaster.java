package org.apache.yarn1;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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
            log.info("INSTANTIATING MASTER " + appClass.getName());
            /**
             * The application master instance now has an opportunity to
             * request containers it needs in the constructor.
             *
             * For example to request 16 containers, with priority 1, 4 cores and
             * 1024Mb of memory:
             *
             * for(int i=0; i< 16; i++) {
             *  requestContainer(1, MyAppWorker.class,1024, 4);
             * }
             */

            //appClass.getConstructor(String[].class).newInstance(Arrays.copyOfRange(args, 1, args.length));
            YarnMaster master = appClass.newInstance();
            try {
                master.onStartUp(args);
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
    final private String appName;
    final private List<YarnContainer> containersToAllocate = Lists.newLinkedList();
    final private AtomicInteger numRequestedContainers = new AtomicInteger(0);
    final private AtomicInteger numCompletedContainers = new AtomicInteger(0);
    final private AtomicBoolean killed = new AtomicBoolean(false);

    /**
     * Default constructor is run form the above main() method ...
     * appClass.newInstance(), see that appClass must be an extension of this
     * class(YarnMaster)
     */
    public YarnMaster() {
        // TODO pass host port and url for tracking to a generic guice servlet
        appName =  this.getClass().getSimpleName();
        try {
            conf = new YarnConfiguration();
            rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
            rmClient.init(conf);
            rmClient.start();
            rmClient.registerApplicationMaster("", 0, "");
            nmClient = NMClient.createNMClient();
            nmClient.init(conf);
            nmClient.start();
            YarnClient.distributeJar(conf, appName);
        } catch (Throwable e) {
            log.error("Could not instantiate Application Master", e);
            System.exit(2);
        }
    }

    protected void onStartUp(String[] args) throws Exception {
        /** To be implemented by application... **/
    }

    protected void onCompletion() {
        /** To be implemented by application... **/
    }

    final protected void requestContainerGroup(
            int numContainers, Class<?> mainClass, String[] args, int priority, int memoryMb, int numCores)
            throws Exception {
        log.info("Requesting container group: " + numContainers + " x (" + memoryMb + " x " + numCores + ")");
        for (int i = 0; i < numContainers; i++) {
            YarnContainer spec = new YarnContainer(conf, priority, memoryMb, numCores, appName, mainClass, args);
            ContainerRequest containerAsk = new ContainerRequest(spec.capability, null, null, spec.priority);
            synchronized (containersToAllocate) {
                containersToAllocate.add(spec);
            }
            numRequestedContainers.incrementAndGet();
            rmClient.addContainerRequest(containerAsk);

        }
        // wait for allocation before requesting other groups
        synchronized (containersToAllocate) {
            while (containersToAllocate.size() > 0 && !killed.get()) {
                containersToAllocate.wait(250);
            }
        }

    }

    private void waitForCompletion() throws Exception {
        while (numRequestedContainers.get() > numCompletedContainers.get()) {
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
                    log.info("http://"+container.getNodeHttpAddress()+"/node/containerlogs/" + container.getId());
                            nmClient.startContainer(container, selectedSpec.createContainerLaunchContext());

                } else {
                    log.warn("Could not resolve allocated container with outstanding requested specs " + container.getId());
                }

            } catch (Throwable ex) {
                log.error("Error while assigning allocated container", ex);
                killed.set(true);
            }
        }
    }

    @Override
    final public float getProgress() {
        float x;
        if (numRequestedContainers.get() == 0) {
            x = 0f;
        } else {
            x = ((float) (numCompletedContainers).get()) / ((float) numRequestedContainers.get());
        }
        return x;
    }

    @Override
    final public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            log.info("Completed container " + status.getContainerId());
            numCompletedContainers.incrementAndGet();
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

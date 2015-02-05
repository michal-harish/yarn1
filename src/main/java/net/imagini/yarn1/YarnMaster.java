package net.imagini.yarn1;


import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class YarnMaster implements AMRMClientAsync.CallbackHandler {

    public static void submitApplicationMaster(Configuration conf, Class<? extends YarnMaster> mainClass,
            String[] args) throws Exception {
        YarnClientWrapper.createApplication(conf, mainClass, args);
    }

    public static void main(String[] args) throws Exception {
        try {
            Class<? extends YarnMaster> appClass = Class.forName(args[0]).asSubclass(YarnMaster.class);
            YarnMaster.args = Arrays.asList(Arrays.copyOfRange(args, 2, args.length));
            YarnMaster master = appClass.newInstance();
            master.allocateContainers();
            master.submitContainerRequests();
            master.waitForCompletion();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private final Logger log;

    public YarnMaster() {
        this.log = LoggerFactory.getLogger(this.getClass());
        // TODO pass host port and url for tracking to a generic guice servlet
        try {
            conf = new YarnConfiguration();
            rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
            rmClient.init(conf);
            rmClient.start();
            rmClient.registerApplicationMaster("", 0, "");
            nmClient = NMClient.createNMClient();
            nmClient.init(conf);
            nmClient.start();
        } catch (Throwable e) {
            log.error("Could not instantiate Application Master", e);
            System.exit(2);
        }
    }

    private static List<String> args;
    private Configuration conf;
    private AMRMClientAsync<ContainerRequest> rmClient;
    private NMClient nmClient;
    final private List<YarnContainerSpec> containerRequests = Lists.newLinkedList();
    final private List<YarnContainerSpec> containersToAllocate = Lists.newLinkedList();
    final private AtomicInteger numRequestedContainers = new AtomicInteger(0);
    final private AtomicInteger numCompletedContainers = new AtomicInteger(0);
    final private AtomicBoolean killed = new AtomicBoolean(false);

    public void allocateContainers() throws Exception {
        // to be implemented by application
    }

    final protected void submitContainer(int priority, Class<?> mainClass, int memoryMb, int vCores) {
        containerRequests.add(new YarnContainerSpec(Resource.newInstance(memoryMb, vCores), Priority.newInstance(priority), mainClass));
    }

    /**
     * Need to group all required containers by their specification as only
     * container requests with the same spec can be acquired together - seems to
     * be a buggy behaviour in Yarn
     */
    private void submitContainerRequests() throws InterruptedException {
        Map<YarnContainerSpec, Integer> specGroups = Maps.newHashMap();
        for (YarnContainerSpec spec : containerRequests) {
            specGroups.put(spec, specGroups.containsKey(spec) ? specGroups.get(spec) + 1 : 1);
        }

        for (Entry<YarnContainerSpec, Integer> specGroupEntry : specGroups.entrySet()) {
            YarnContainerSpec spec = specGroupEntry.getKey();
            Integer numContainers = specGroupEntry.getValue();
            log.info("REQUESTING CONTAINERS: " + numContainers + " x " + spec);
            for (int i = 0; i < numContainers; i++) {
                ContainerRequest containerAsk = new ContainerRequest(spec.getCapability(), null, null, spec.getPriority());
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
            Thread.sleep(250);
        }
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            try {

                YarnContainerSpec selectedSpec = null;
                synchronized (containersToAllocate) {
                    int selected = -1;
                    for (int h = 0; h < containersToAllocate.size(); h++) {
                        YarnContainerSpec spec = containersToAllocate.get(h);
                        if (spec.isSatisfiedBy(container)) {
                            if (selected == -1) {
                                selected = h;
                            } else {
                                YarnContainerSpec currentlySelected = containersToAllocate.get(selected);
                                if (currentlySelected.getCapability().getMemory() > spec.getCapability().getMemory()
                                        || currentlySelected.getCapability().getVirtualCores() >= spec.getCapability()
                                                .getVirtualCores()
                                        || currentlySelected.getPriority().getPriority() >= spec.getPriority().getPriority()) {
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
                    log.info("Assigned container " + container + " to " + selectedSpec.getMainClass().getSimpleName());
                    YarnContextWrapper context = new YarnContextWrapper(log, conf, this.getClass().getSimpleName(),
                            selectedSpec.getMainClass(), args);
                    nmClient.startContainer(container, context.getContainer(false));

                } else {
                    throw new IllegalStateException("Could not assign allocated container " + container);
                }

            } catch (Throwable ex) {
                log.error("Error launching container " + container.getId() + " " + ex);
            }
        }
    }

    @Override
    final public float getProgress() {
        return Math.max(0f, ((float) (numCompletedContainers).get()) / ((float) numRequestedContainers.get()));
    }

    @Override
    final public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            log.info("Completed container " + status.getContainerId());
            numCompletedContainers.incrementAndGet();
        }
    }

    @Override
    public void onShutdownRequest() {
        killed.set(true);
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
        // TODO suspend - resume behaviour
        log.warn("NODE STATUS UPDATE NOT IMPLEMENTED");
    }

    @Override
    public void onError(Throwable e) {
        log.error("Application Master received onError event", e);
        killed.set(true);
    }

}

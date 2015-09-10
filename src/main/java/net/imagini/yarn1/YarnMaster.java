package net.imagini.yarn1;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class YarnMaster implements AMRMClientAsync.CallbackHandler {

    /**
     * This method should be called by the implementing application static main
     * method. It does all the work around creating a yarn application and
     * submitting the request to the yarn resource manager. The class given in
     * the appClass argument will be run inside the yarn-allocated master
     * container.
     */
    public static void submitApplicationMaster(Configuration conf, Class<? extends YarnMaster> appClass, String[] args) throws Exception {

        Logger log = LoggerFactory.getLogger(YarnMaster.class);
        String queue = conf.get("master.queue");
        Priority masterPriority = Priority.newInstance(conf.getInt("master.priority", 0));
        boolean keepContainers = conf.getBoolean("master.keepContainers", false);
        int masterMemoryMb = conf.getInt("master.memory.mb", 128);
        int masterNumCores = conf.getInt("master.num.cores", 1);
        int masterTimeout = conf.getInt("master.timeout.s", 3600);

        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        for (NodeReport report : yarnClient.getNodeReports(NodeState.RUNNING)) {
            log.info("Node report:" + report.getNodeId() + " @ " + report.getHttpAddress() + " | " + report.getCapability());
        }

        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        ApplicationId appId = appResponse.getApplicationId();
        if (appId == null) {
            System.exit(2);
        }

        List<String> newArgs = Lists.newLinkedList();
        newArgs.add(appClass.getName());
        newArgs.add(String.valueOf(masterTimeout));
        for (String arg : args) {
            newArgs.add(arg);
        }
        YarnContext containerSpec = new YarnContext(conf, appClass.getSimpleName(), YarnMaster.class, newArgs);

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
        appContext.setApplicationName(appClass.getSimpleName());
        appContext.setResource(Resource.newInstance(masterMemoryMb, masterNumCores));
        appContext.setAMContainerSpec(containerSpec.getContainer(true));

        appContext.setPriority(masterPriority);
        appContext.setQueue(queue);

        yarnClient.submitApplication(appContext);

        long start = System.currentTimeMillis();
        ApplicationReport report = yarnClient.getApplicationReport(appId);
        log.info("Tracking URL: " + report.getTrackingUrl());
        float lastProgress = -0.0f;
        while (true) {
            Thread.sleep(1000);
            try {
                report = yarnClient.getApplicationReport(appId);
                if (lastProgress != report.getProgress()) {
                    lastProgress = report.getProgress();
                    log.info(report.getApplicationId() + " " + report.getProgress() + " "
                            + (System.currentTimeMillis() - report.getStartTime()) + "(ms) " + report.getDiagnostics());
                }
                if (!report.getFinalApplicationStatus().equals(FinalApplicationStatus.UNDEFINED)) {
                    log.info(report.getApplicationId() + " " + report.getFinalApplicationStatus());
                    log.info("Tracking url: " + report.getTrackingUrl());
                    log.info("Finish time: " + ((System.currentTimeMillis() - report.getStartTime()) / 1000) + "(s)");
                    break;
                }
            } finally {
                if (System.currentTimeMillis() - start > masterTimeout * 1000) {
                    yarnClient.killApplication(appId);
                    break;
                }
            }
        }

        yarnClient.stop();
    }

    /**
     * Static Main method will be executed in the ApplicationMaster container as
     * a result of the above submission. It is a default wrapper which will
     * actually create an instance of the class passed as the first argument
     */
    public static void main(String[] args) throws Exception {
        try {
            Class<? extends YarnMaster> appClass = Class.forName(args[0]).asSubclass(YarnMaster.class);
            YarnMaster.args = Arrays.asList(Arrays.copyOfRange(args, 2, args.length));
            YarnMaster master = appClass.newInstance();
            try {
                /**
                 * The application master instance now has an opportunity to
                 * request containers it needs
                 **/
                master.allocateContainers();
                /**
                 * within the implementation of allocateContainers, the
                 * application should call submit containers for all the
                 * resources it requires.
                 **/

                master.submitContainerRequests();
                master.waitForCompletion();
            } catch (Throwable e) {
                e.printStackTrace();
                System.exit(2);
            } finally {
                master.onCompletion();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Default constructor is run form the above main() method ...
     * appClass.newInstance(), see that appClass must be an extension of this
     * class(YarnMaster)
     */
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

    private final Logger log;
    private static List<String> args;
    private Configuration conf;
    private AMRMClientAsync<ContainerRequest> rmClient;
    private NMClient nmClient;
    final private List<YarnSpec> containerRequests = Lists.newLinkedList();
    final private List<YarnSpec> containersToAllocate = Lists.newLinkedList();
    final private AtomicInteger numRequestedContainers = new AtomicInteger(0);
    final private AtomicInteger numCompletedContainers = new AtomicInteger(0);
    final private AtomicBoolean killed = new AtomicBoolean(false);

    public void allocateContainers() throws Exception {
        /**
         * To be implemented by application...
         * 
         * For example to request 16 containers, with priority 1, 4 cores and
         * 1024Mb of memory:
         * 
         * for(int i=0; i< 16; i++) { requestContainer(1, MyAppWorker.class,
         * 1024, 4); }
         * 
         */
    }

    protected void onCompletion() {
        /** To be implemented by application... **/
    }

    final protected void requestContainer(int priority, Class<?> mainClass, int memoryMb, int vCores) {
        containerRequests.add(new YarnSpec(Resource.newInstance(memoryMb, vCores), Priority.newInstance(priority), mainClass));
    }

    /**
     * Need to group all required containers by their specification as only
     * container requests with the same spec can be acquired together - seems to
     * be a buggy behaviour in Yarn
     */
    private void submitContainerRequests() throws InterruptedException {
        Map<YarnSpec, Integer> specGroups = Maps.newHashMap();
        for (YarnSpec spec : containerRequests) {
            specGroups.put(spec, specGroups.containsKey(spec) ? specGroups.get(spec) + 1 : 1);
        }

        for (Entry<YarnSpec, Integer> specGroupEntry : specGroups.entrySet()) {
            YarnSpec spec = specGroupEntry.getKey();
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
    final public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            try {

                YarnSpec selectedSpec = null;
                synchronized (containersToAllocate) {
                    int selected = -1;
                    for (int h = 0; h < containersToAllocate.size(); h++) {
                        YarnSpec spec = containersToAllocate.get(h);
                        if (spec.isSatisfiedBy(container)) {
                            if (selected == -1) {
                                selected = h;
                            } else {
                                YarnSpec currentlySelected = containersToAllocate.get(selected);
                                if (currentlySelected.getCapability().getMemory() > spec.getCapability().getMemory()
                                        || currentlySelected.getCapability().getVirtualCores() >= spec.getCapability().getVirtualCores()
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
                    YarnContext context = new YarnContext(conf, this.getClass().getSimpleName(), selectedSpec.getMainClass(), args);
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
        float x ;
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

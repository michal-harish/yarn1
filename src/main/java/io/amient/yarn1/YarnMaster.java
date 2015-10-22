package io.amient.yarn1;

/**
 * Yarn1 - Java Library for Apache YARN
 * Copyright (C) 2015 Michal Harish
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p/>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p/>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import com.google.common.collect.Maps;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class YarnMaster {

    private static final Logger log = LoggerFactory.getLogger(YarnMaster.class);

    static final int DEFAULT_MASTER_MEMORY_MB = 256;
    static final int DEFAULT_MASTER_CORES = 1;
    static final int DEFAULT_MASTER_PRIORITY = 0;

    /**
     * Static Main method will be executed in the ApplicationMaster container as
     * a result of the submission by YarnClient. It is a default wrapper which will
     * actually create an instance of the class passed as the first argument
     */
    public static void main(String[] args) throws Exception {
        Properties config = YarnClient.getAppConfiguration();
        run(config, args);
    }

    public static void run(Properties config, String[] args) throws Exception {
        try {

            log.info("Yarn1 App Configuration:");
            for (Object param : config.keySet()) {
                log.info(param.toString() + " = " + config.get(param).toString());
            }
            Class<? extends YarnMaster> masterClass = Class.forName(config.getProperty("yarn1.master.class")).asSubclass(YarnMaster.class);
            log.info("Starting Master Instance: " + masterClass.getName());
            Constructor<? extends YarnMaster> constructor = masterClass.getConstructor(Properties.class);
            YarnMaster master = null;
            try {
                master = constructor.newInstance(config);
                master.initialize();
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
                    synchronized (master.killed) {
                        master.killed.wait(10000);
                    }
                    if (master.killed.get()) {
                        try {
                            master.rmClient.unregisterApplicationMaster(FinalApplicationStatus.KILLED, "", "");
                        } finally {
                            System.exit(103);
                        }
                    }
                }
                master.conclude();
            } catch (Throwable e) {
                e.printStackTrace();
                System.exit(102);
            } finally {
                if (master != null) master.onCompletion();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(101);
        }
    }

    final protected Properties appConfig;
    final private YarnConfiguration yarnConfig;
    final private String applicationName;
    final private String masterClassName;
    final private LinkedHashMap<YarnContainerContext, ContainerRequest> containersToAllocate = Maps.newLinkedHashMap();
    final private AtomicInteger numTasks = new AtomicInteger(0);
    final private AtomicInteger numCompletedTasks = new AtomicInteger(0);
    final private AtomicBoolean killed = new AtomicBoolean(false);
    final private Map<ContainerId, YarnContainerContext> runningContainers = Maps.newConcurrentMap();
    final private Map<ContainerId, YarnContainerContext> completedContainers = Maps.newConcurrentMap();
    final private AMRMClientAsync.CallbackHandler listener = new AMRMClientAsync.CallbackHandler() {
        @Override
        public float getProgress() {
            return YarnMaster.this.getProgress();
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {
            log.warn("TODO - NODE STATUS UPDATE NOT IMPLEMENTED");
        }

        @Override
        public void onShutdownRequest() {
            synchronized (killed) {
                killed.set(true);
                killed.notify();
            }
        }

        @Override
        public void onError(Throwable e) {
            log.error("Application Master received onError event", e);
            synchronized (killed) {
                killed.set(true);
                killed.notify();
            }
        }

        @Override
        final public void onContainersAllocated(List<Container> containers) {
            for (Container container : containers) {
                try {
                    Map.Entry<YarnContainerContext, ContainerRequest> selected = null;
                    synchronized (containersToAllocate) {
                        for (Map.Entry<YarnContainerContext, ContainerRequest> entry : containersToAllocate.entrySet()) {
                            YarnContainerContext spec = entry.getKey();
                            if (spec.isSatisfiedBy(container)) {
                                if (selected == null) {
                                    selected = entry;
                                } else {
                                    YarnContainerContext selectedSoFar = selected.getKey();
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
                        YarnContainerContext spec = selected.getKey();
                        log.info("Launching Container " + container.getNodeHttpAddress() + " " + container + " to " + spec.mainClass);
                        nmClient.startContainer(container, spec.createContainerLaunchContext());
                        spec.assignContainer(container);
                        log.info(spec.getLogsUrl());
                        runningContainers.put(container.getId(), spec);
                        onContainerLaunched(container.getId().toString(), spec);
                        log.info("Number of running containers = " + runningContainers.size());
                    } else {
                        log.warn("Could not resolve allocated container with outstanding requested spec: " + container.getResource() + ", priority:" + container.getPriority());
                        rmClient.releaseAssignedContainer(container.getId());
                        //FIXME rmClient still seems to be re-requesting all the previously requested containers
                    }

                } catch (Throwable ex) {
                    log.error("Error while assigning allocated container", ex);
                    synchronized (killed) {
                        killed.set(true);
                        killed.notify();
                    }
                }
            }
        }

        @Override
        final public void onContainersCompleted(List<ContainerStatus> statuses) {
            for (ContainerStatus status : statuses) {
                YarnContainerContext completedSpec = runningContainers.remove(status.getContainerId());
                if (completedSpec != null) {
                    log.info("Completed container " + status.getContainerId()
                            + ", (exit status " + status.getExitStatus() + ")  " + status.getDiagnostics());
                    completedSpec.assignStatus(status);
                    completedContainers.put(status.getContainerId(), completedSpec);
                    if (restartEnabled) {
                        if (status.getExitStatus() > 0 && completedSpec.incrementAndGetNumFailures() > restartFailedRetries) {
                            log.warn("Container failed more than " + restartFailedRetries + " times, killing application");
                            synchronized (killed) {
                                killed.set(true);
                                killed.notify();
                            }
                        } else {
                            log.info("Auto-restarting container " + completedSpec);
                            requestContainer(completedSpec);
                        }
                    }
                    numCompletedTasks.incrementAndGet();
                    onContainerCompleted(status.getContainerId().toString(), completedSpec, status.getExitStatus());
                }
            }
        }
    };

    private AMRMClientAsync<ContainerRequest> rmClient;
    private NMClient nmClient;
    private Boolean restartEnabled;
    private Integer restartFailedRetries;
    private ApplicationId appId;
    private URL trackingUrl = null;
    final public int masterMemoryMb;
    final public int masterCores;
    final public int masterPriority;
    final public boolean localMode;
    private ExecutorService executor = null;

    /**
     * Default constructor can be used for local execution
     */
    public YarnMaster(Properties appConfig) {
        this.appConfig = appConfig;
        this.masterClassName = this.getClass().getName();
        this.applicationName = appConfig.getProperty("yarn1.application.name", masterClassName);
        yarnConfig = new YarnConfiguration();
        localMode = Boolean.valueOf(appConfig.getProperty("yarn1.local.mode", "false"));
        masterMemoryMb = Integer.valueOf(appConfig.getProperty("yarn1.master.memory.mb", String.valueOf(YarnMaster.DEFAULT_MASTER_MEMORY_MB)));
        masterCores = Integer.valueOf(appConfig.getProperty("yarn1.master.num.cores", String.valueOf(YarnMaster.DEFAULT_MASTER_CORES)));
        masterPriority = Integer.valueOf(appConfig.getProperty("yarn1.master.priority", String.valueOf(YarnMaster.DEFAULT_MASTER_PRIORITY)));
        if (appConfig.containsKey("yarn1.client.tracking.url")) {
            try {
                trackingUrl = new URL(appConfig.getProperty("yarn1.client.tracking.url"));
            } catch (MalformedURLException e) {
                log.warn("Invalid client tracking url", e);
            }
        }
    }

    private void initialize() throws Exception {
        this.restartEnabled = Boolean.valueOf(appConfig.getProperty("yarn1.restart.enabled", "false"));
        this.restartFailedRetries = Integer.valueOf(appConfig.getProperty("yarn1.restart.failed.retries", "5"));
        URL url = getTrackingURL();
        log.info("APPLICATION TRACKING URL: " + url);
        if (localMode) {
            executor = Executors.newCachedThreadPool();
        } else {
            this.appId = ApplicationId.newInstance(
                    Long.parseLong(appConfig.getProperty("am.timestamp")),
                    Integer.parseInt(appConfig.getProperty("am.id")));
            log.info("APPLICATION ID: " + appId.toString());
            rmClient = AMRMClientAsync.createAMRMClientAsync(100, listener);
            rmClient.init(yarnConfig);
            nmClient = NMClient.createNMClient();
            nmClient.init(yarnConfig);
            rmClient.start();

            rmClient.registerApplicationMaster("", 0, url == null ? null : url.toString());
            nmClient.start();
            YarnClient.distributeResources(yarnConfig, appConfig, applicationName);
        }
    }

    private void conclude() throws IOException, YarnException {
        if (localMode) {
            executor.shutdownNow();
        } else {
            rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
        }
    }

    final protected java.net.URL getTrackingURL() {
        return (trackingUrl != null) ? trackingUrl : provideTrackingURL(null, 0);
    }

    /**
     * delegate tracking url (and the server backing it) to the application
     * @param prefHost preferred hostname, if null then the choice is down to the implementation
     * @param prefPort preferred port, if -1 or 0 then the choice is down to the implementation
     * @return
     */
    protected java.net.URL provideTrackingURL(String prefHost, int prefPort) {
        /** To be implemented by application... **/
        return null;
    }

    protected void onStartUp(String[] args) throws Exception {
        /** To be implemented by application... **/
    }

    protected void onContainerLaunched(String containerId, YarnContainerContext spec) {
        /** To be implemented by application... **/
    }

    protected float getProgress() {
        /** To be overriden by application... - will be invoked at regular intervals**/
        if (numTasks.get() == 0) {
            return 0f;
        } else {
            return ((float) (numCompletedTasks.get())) / ((float) numTasks.get());
        }
    }

    protected void onContainerCompleted(String containerId, YarnContainerContext spec, int exitStatus) {
        /** To be implemented by application... **/
    }

    protected void onCompletion() {
        /** To be implemented by application... **/
    }

    final protected void requestContainerGroup(int numContainers, YarnContainerRequest spec) throws Exception {
        YarnContainerRequest[] requests = new YarnContainerRequest[numContainers];
        for (int i = 0; i < numContainers; i++) {
            requests[i] = spec;
        }
        requestContainerGroup(requests);
    }

    final protected void requestContainerGroup(YarnContainerRequest[] requests) throws Exception {
        for (YarnContainerRequest spec : requests) {
            int totalContainerMemoryMb = spec.directMemMb + spec.heapMemMb;
            log.info("Requesting container (" + totalContainerMemoryMb + "Mb x " + spec.numCores + ")" + Arrays.asList(spec.args));
            String jvmArgs = "-XX:MaxDirectMemorySize=" + spec.directMemMb + "m -Xmx" + spec.heapMemMb + "m -Xms" + spec.heapMemMb + "m " + appConfig.getProperty("yarn1.jvm.args", "");
            String[] args = new String[spec.args.length + 1];
            args[0] = spec.mainClass.getName();
            int i=0;
            for(String specArg: spec.args) args[++i] = specArg;
            requestContainer(
                    new YarnContainerContext(yarnConfig, appConfig, jvmArgs, spec.priority, totalContainerMemoryMb, spec.numCores, applicationName, YarnContainer.class, args)
            );
        }
        // wait for allocation before requesting other groups
        synchronized (containersToAllocate) {
            while (containersToAllocate.size() > 0 && !killed.get()) {
                containersToAllocate.wait(250);
            }
        }
    }

    private void requestContainer(YarnContainerContext spec) {
        if (localMode) {
            try {
                Runnable task = YarnContainer.prepareRunnable(appConfig, spec.args);
                executor.submit(task);
                onContainerLaunched(String.valueOf(task.hashCode()), spec);
                numTasks.incrementAndGet();
            } catch (Throwable e) {
                log.error("Failed to launch local task thread", e);
            }
        } else {
            ContainerRequest containerAsk = new ContainerRequest(spec.capability, null, null, spec.priority);
            synchronized (containersToAllocate) {
                containersToAllocate.put(spec, containerAsk);
            }
            rmClient.addContainerRequest(containerAsk);
            numTasks.incrementAndGet();
        }

    }

    public Map<ContainerId, YarnContainerContext> getRunningContainers() {
        return Collections.unmodifiableMap(runningContainers);
    }

    public Map<ContainerId, YarnContainerContext> getCompletedContainers() {
        return Collections.unmodifiableMap(completedContainers);
    }

}
package org.apache.yarn1;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class YarnMaster {

    private static final Logger log = LoggerFactory.getLogger(YarnMaster.class);

    /**
     * Static Main method will be executed in the ApplicationMaster container as
     * a result of the submission by YarnClient. It is a default wrapper which will
     * actually create an instance of the class passed as the first argument
     */
    public static void main(String[] args) throws Exception {
        try {
            Properties config = YarnClient.getAppConfiguration();
            log.info("Yarn1 App Configuration:");
            for (Object param : config.keySet()) {
                log.info(param.toString() + " = " + config.get(param).toString());
            }
            Class<? extends YarnMaster> appClass = Class.forName(config.getProperty("yarn1.master.class")).asSubclass(YarnMaster.class);
            log.info("Starting Master Instance: " + appClass.getName());
            Constructor<? extends YarnMaster> constructor = appClass.getConstructor(Properties.class);
            YarnMaster master = null;
            try {
                master = constructor.newInstance(config);
                master.initializeAsYarn();
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
                master.rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
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

    private AMRMClientAsync<ContainerRequest> rmClient;
    private NMClient nmClient;
    private Boolean restartEnabled;
    private Integer restartFailedRetries;
    final protected Properties appConfig;
    final private YarnConfiguration yarnConfig;
    final private String appName;
    final private LinkedHashMap<YarnContainer, ContainerRequest> containersToAllocate = Maps.newLinkedHashMap();
    final private AtomicInteger numTasks = new AtomicInteger(0);
    final private AtomicInteger numCompletedTasks = new AtomicInteger(0);
    final private AtomicBoolean killed = new AtomicBoolean(false);
    final private Map<ContainerId, YarnContainer> runningContainers = Maps.newConcurrentMap();

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
                    Map.Entry<YarnContainer, ContainerRequest> selected = null;
                    synchronized (containersToAllocate) {
                        for (Map.Entry<YarnContainer, ContainerRequest> entry : containersToAllocate.entrySet()) {
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
                                + ", container spec: " + container.getResource() + ", priority:" + container.getPriority());
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
                YarnContainer completedSpec = runningContainers.remove(status.getContainerId());
                if (completedSpec != null) {
                    log.info("Completed container " + status.getContainerId()
                            + ", (exit status " + status.getExitStatus() + ")  " + status.getDiagnostics());
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
                }
            }
        }
    };

    /**
     * Default constructor can be used for local execution
     */
    public YarnMaster(Properties appConfig) {
        this.appName = this.getClass().getName();
        this.appConfig = appConfig;
        this.yarnConfig = new YarnConfiguration();
    }

    private void initializeAsYarn() throws Exception {
        this.restartEnabled = Boolean.valueOf(appConfig.getProperty("yarn1.restart.enabled", "false"));
        this.restartFailedRetries = Integer.valueOf(appConfig.getProperty("yarn1.restart.failed.retries", "5"));
        rmClient = AMRMClientAsync.createAMRMClientAsync(100, listener);
        rmClient.init(yarnConfig);
        nmClient = NMClient.createNMClient();
        nmClient.init(yarnConfig);
        rmClient.start();
        rmClient.registerApplicationMaster("", 0, "");
        nmClient.start();
        YarnClient.distributeResources(yarnConfig, appConfig, appName);
    }

    protected void onStartUp(String[] args) throws Exception {
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

    protected void onCompletion() {
        /** To be implemented by application... **/
    }


    private void requestContainer(YarnContainer spec) {
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
        for (YarnContainerRequest spec : requests) {
            int totalContainerMemoryMb = spec.directMemMb + spec.heapMemMb;
            log.info("Requesting container (" + totalContainerMemoryMb + "Mb x " + spec.numCores + ")" + Arrays.asList(spec.args));
            String jvmArgs = "-XX:MaxDirectMemorySize=" + spec.directMemMb + "m -Xmx" + spec.heapMemMb + "m -Xms" + spec.heapMemMb + "m " + appConfig.getProperty("yarn1.jvm.args", "");
            requestContainer(
                    new YarnContainer(yarnConfig, appConfig, jvmArgs, spec.priority, totalContainerMemoryMb, spec.numCores, appName, spec.mainClass, spec.args)
            );
        }
        // wait for allocation before requesting other groups
        synchronized (containersToAllocate) {
            while (containersToAllocate.size() > 0 && !killed.get()) {
                containersToAllocate.wait(250);
            }
        }
    }

    private String getContainerUrl(Container container) {
        return "http://" + container.getNodeHttpAddress() + "/node/containerlogs/" + container.getId();
    }

}

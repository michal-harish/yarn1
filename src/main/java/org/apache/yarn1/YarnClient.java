package org.apache.yarn1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by mharis on 12/09/15.
 */
public class YarnClient {

    private static final Logger log = LoggerFactory.getLogger(YarnClient.class);

    /**
     *
     * @return
     */
    public static Configuration getConfiguration() {
        return new Yarn1Configuration();
    }

    /**
     * This method should be called by the implementing application static main
     * method. It does all the work around creating a yarn application and
     * submitting the request to the yarn resource manager. The class given in
     * the appClass argument will be run inside the yarn-allocated master
     * container.
     */
    public static void submitApplicationMaster(
            Configuration conf,
            Class<? extends YarnMaster> appClass,
            String[] args,
            Boolean awaitCompletion
    ) throws Exception {

        String yarnConfigPath = conf.get("yarn1.site", "/etc/hadoop");
        conf.addResource(new FileInputStream(yarnConfigPath + "/core-site.xml"));
        conf.addResource(new FileInputStream(yarnConfigPath + "/hdfs-site.xml"));
        conf.addResource(new FileInputStream(yarnConfigPath + "/yarn-site.xml"));

        conf.set("yarn1.master.class", appClass.getName());
        String appName = appClass.getName();
        String queue = conf.get("yarn1.queue");
        int masterPriority = conf.getInt("yarn1.master.priority", 0);
        int masterMemoryMb = conf.getInt("yarn1.master.memory.mb", 256);
        int masterNumCores = conf.getInt("yarn1.master.num.cores", 1);
        boolean keepContainers = conf.getBoolean("yarn1.keepContainers", false);
        /**
         * keepKontainers has 2 meanings:
         * 1) yarn uses it to keep containers across attempts
         * 2) yarn1 uses it to autorestart failed containers
         */

        final org.apache.hadoop.yarn.client.api.YarnClient yarnClient = org.apache.hadoop.yarn.client.api.YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        for (NodeReport report : yarnClient.getNodeReports(NodeState.RUNNING)) {
            log.debug("Node report:" + report.getNodeId() + " @ " + report.getHttpAddress() + " | " + report.getCapability());
        }
        //TODO check if appName already running and config yarn.master.failifexists

        log.info("Submitting application master class " + appClass.getName() + " with keepContainers = " + keepContainers);

        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        final ApplicationId appId = appResponse.getApplicationId();
        if (appId == null) {
            System.exit(2);
        }

        YarnClient.distributeResources(conf, appName);

        YarnContainer masterContainer = new YarnContainer(
                conf, masterPriority, masterMemoryMb, masterNumCores, appName, YarnMaster.class, args);

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
        appContext.setApplicationName(appName);
        appContext.setResource(masterContainer.capability);
        appContext.setPriority(masterContainer.priority);
        appContext.setQueue(queue);
        appContext.setAMContainerSpec(masterContainer.createContainerLaunchContext());

        yarnClient.submitApplication(appContext);

        ApplicationReport report = yarnClient.getApplicationReport(appId);
        log.info("Tracking URL: " + report.getTrackingUrl());

        if (awaitCompletion) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    if (!yarnClient.isInState(Service.STATE.STOPPED)) {
                        log.info("Killing yarn application in shutdown hook");
                        try {
                            yarnClient.killApplication(appId);
                        } catch (Throwable e) {
                            e.printStackTrace(System.out);
                        } finally {
                            //yarnClient.stop();
                        }
                    }
                }
            });

            float lastProgress = -0.0f;
            while (true) {
                Thread.sleep(1000);
                try {
                    report = yarnClient.getApplicationReport(appId);
                    if (lastProgress != report.getProgress()) {
                        lastProgress = report.getProgress();
                        log.info(report.getApplicationId() + " " + (report.getProgress() * 100.00) + "% "
                                + (System.currentTimeMillis() - report.getStartTime()) + "(ms) " + report.getDiagnostics());
                    }
                    if (!report.getFinalApplicationStatus().equals(FinalApplicationStatus.UNDEFINED)) {
                        log.info(report.getApplicationId() + " " + report.getFinalApplicationStatus());
                        log.info("Tracking url: " + report.getTrackingUrl());
                        log.info("Finish time: " + ((System.currentTimeMillis() - report.getStartTime()) / 1000) + "(s)");
                        break;
                    }
                } catch (Throwable e) {
                    log.error("Master Heart Beat Error - terminating", e);
                    yarnClient.killApplication(appId);
                    Thread.sleep(2000);
                }
            }
            yarnClient.stop();

            if (!report.getFinalApplicationStatus().equals(FinalApplicationStatus.SUCCEEDED)) {
                System.exit(1);
            }
        }
        yarnClient.stop();
    }

    /**
     * Distribute all dependencies in a single jar both from Client to Master as well as Master to Container(s)
     *
     * @param conf
     * @param appName
     * @throws IOException
     */
    public static void distributeResources(Configuration conf, String appName) throws IOException {
        final FileSystem distFs = FileSystem.get(conf);

        //distribute configuration
        final Path dstConfig = new Path(distFs.getHomeDirectory(), appName + ".configuration");
        final FSDataOutputStream fs = distFs.create(dstConfig);
        final DataOutput os = new DataOutputStream(fs);
        conf.write(os);
        fs.close();
        log.info("Updated resource " + dstConfig);

        //distribute main jar
        final String localPath = YarnClient.class.getProtectionDomain().getCodeSource().getLocation().getFile().replace(".jar/", ".jar");
        final Path src;
        final String jarName = appName + ".jar";
        if (localPath.endsWith(".jar")) {
            log.info("Distributing local jar : " + localPath);
            src = new Path(localPath);
        } else {
            String localArchive = localPath + appName + ".jar";
            log.info("Archiving and distributing local classes from current working directory " + localArchive);
            try {
                String archiveCommand = "jar cMf " + localArchive + " -C " + localPath + " ./";
                FileSystem.getLocal(conf).delete(new Path(localArchive), false);
                Process archivingProcess = Runtime.getRuntime().exec(archiveCommand);
                if (archivingProcess.waitFor() != 0) {
                    throw new IOException("Failed to executre tar -C command on: " + localPath);
                }

                src = new Path(localArchive);

            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        //distribute jar
        final Path dst = new Path(distFs.getHomeDirectory(), jarName);
        log.info("Updating resource " + dst + " ...");
        distFs.copyFromLocalFile(false, true, src, dst);
        FileStatus scFileStatus = distFs.getFileStatus(dst);
        log.info("Updated resource " + dst + " " + scFileStatus.getLen());

    }

}

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

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class YarnClient {

    private static final Logger log = LoggerFactory.getLogger(YarnClient.class);

    /**
     * This method should be called by the implementing application static main
     * method. It does all the work around creating a yarn application and
     * submitting the request to the yarn resource manager. The class given in
     * the appClass argument will be run inside the yarn-allocated master
     * container.
     */
    public static void submitApplicationMaster(
            Properties appConfig,
            Class<? extends YarnMaster> appClass,
            String[] args,
            Boolean awaitCompletion
    ) throws Exception {
        log.info("Yarn1 App Configuration:");
        for (Object param : appConfig.keySet()) {
            log.info(param.toString() + " = " + appConfig.get(param).toString());
        }
        log.info("------------------------");

        String yarnConfigPath = appConfig.getProperty("yarn1.site", "/etc/hadoop");
        appConfig.setProperty("yarn1.master.class", appClass.getName());
        String appName = appClass.getName();
        String queue = appConfig.getProperty("yarn1.queue");
        int masterPriority = Integer.valueOf(appConfig.getProperty("yarn1.master.priority", "0"));
        int masterMemoryMb = Integer.valueOf(appConfig.getProperty("yarn1.master.memory.mb", "256"));
        int masterNumCores = Integer.valueOf(appConfig.getProperty("yarn1.master.num.cores", "1"));

        Configuration yarnConfig = new YarnConfiguration();
        yarnConfig.addResource(new FileInputStream(yarnConfigPath + "/core-site.xml"));
        yarnConfig.addResource(new FileInputStream(yarnConfigPath + "/hdfs-site.xml"));
        yarnConfig.addResource(new FileInputStream(yarnConfigPath + "/yarn-site.xml"));
        for (Map.Entry<Object, Object> entry : appConfig.entrySet()) {
            yarnConfig.set(entry.getKey().toString(), entry.getValue().toString());
        }

        final org.apache.hadoop.yarn.client.api.YarnClient yarnClient = org.apache.hadoop.yarn.client.api.YarnClient.createYarnClient();
        yarnClient.init(yarnConfig);
        yarnClient.start();

        for (NodeReport report : yarnClient.getNodeReports(NodeState.RUNNING)) {
            log.debug("Node report:" + report.getNodeId() + " @ " + report.getHttpAddress() + " | " + report.getCapability());
        }

        log.info("Submitting application master class " + appClass.getName());

        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        final ApplicationId appId = appResponse.getApplicationId();
        if (appId == null) {
            System.exit(111);
        } else {
            appConfig.setProperty("am.timestamp", String.valueOf(appId.getClusterTimestamp()));
            appConfig.setProperty("am.id", String.valueOf(appId.getId()));
        }

        YarnClient.distributeResources(yarnConfig, appConfig, appName);

        String masterJvmArgs = appConfig.getProperty("yarn1.master.jvm.args", "");
        YarnContainer masterContainer = new YarnContainer(
                yarnConfig, appConfig, masterJvmArgs, masterPriority, masterMemoryMb, masterNumCores, appName, YarnMaster.class, args);

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setApplicationName(appName);
        appContext.setResource(masterContainer.capability);
        appContext.setPriority(masterContainer.priority);
        appContext.setQueue(queue);
        appContext.setApplicationType(appConfig.getProperty("yarn1.application.type", "YARN"));
        appContext.setAMContainerSpec(masterContainer.createContainerLaunchContext());

        log.info("Master container spec: " + masterContainer.capability);

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
                            log.error("Failed to kill yarn application - please check YARN Resource Manager", e);
                        }
                    }
                }
            });

            float lastProgress = -0.0f;
            while (true) {
                try {
                    Thread.sleep(10000);
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
                System.exit(112);
            }
        }
        yarnClient.stop();
    }

    /**
     * Distribute all dependencies in a single jar both from Client to Master as well as Master to Container(s)
     */
    public static void distributeResources(Configuration yarnConf, Properties appConf, String appName) throws IOException {
        final FileSystem distFs = FileSystem.get(yarnConf);
        final FileSystem localFs = FileSystem.getLocal(yarnConf);
        try {

            //distribute configuration
            final Path dstConfig = new Path(distFs.getHomeDirectory(), appName + ".configuration");
            final FSDataOutputStream fs = distFs.create(dstConfig);
            appConf.store(fs, "Yarn1 Application Config for " + appName);
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
                try {
                    String localArchive = localPath + appName + ".jar";
                    localFs.delete(new Path(localArchive), false);
                    log.info("Unpacking compile scope dependencies: " + localPath);
                    executeShell("mvn -f " + localPath + "/../.. generate-resources");
                    log.info("Preparing application main jar " + localArchive);
                    executeShell("jar cMf " + localArchive + " -C " + localPath + " ./");
                    src = new Path(localArchive);

                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }

            byte[] digest;
            final MessageDigest md = MessageDigest.getInstance("MD5");
            try (InputStream is = new FileInputStream(src.toString())) {
                DigestInputStream dis = new DigestInputStream(is, md);
                byte[] buffer = new byte[8192];
                int numOfBytesRead;
                while ((numOfBytesRead = dis.read(buffer)) > 0) {
                    md.update(buffer, 0, numOfBytesRead);
                }
                digest = md.digest();
            }
            log.info("Local check sum: " + Hex.encodeHexString(digest));

            final Path dst = new Path(distFs.getHomeDirectory(), jarName);
            Path remoteChecksumFile = new Path(distFs.getHomeDirectory(), jarName + ".md5");
            boolean checksumMatches = false;
            if (distFs.isFile(remoteChecksumFile)) {
                try (InputStream r = distFs.open(remoteChecksumFile)) {
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    int nRead;
                    byte[] data = new byte[1024];
                    while ((nRead = r.read(data, 0, data.length)) != -1) {
                        buffer.write(data, 0, nRead);
                    }
                    buffer.flush();
                    byte[] remoteDigest = buffer.toByteArray();
                    log.info("Remote check sum: " + Hex.encodeHexString(remoteDigest));
                    checksumMatches = Arrays.equals(digest, remoteDigest);

                }
            }
            if (!checksumMatches) {
                log.info("Updating resource " + dst + " ...");
                distFs.copyFromLocalFile(false, true, src, dst);
                try (FSDataOutputStream remoteChecksumStream = distFs.create(remoteChecksumFile)) {
                    log.info("Updating checksum " + remoteChecksumFile + " ...");
                    remoteChecksumStream.write(digest);
                }
                FileStatus scFileStatus = distFs.getFileStatus(dst);
                log.info("Updated resource " + dst + " " + scFileStatus.getLen());
            }
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
    }

    public static Properties getAppConfiguration() {
        try {
            Properties properties = new Properties() {{
                load(new FileInputStream("yarn1.configuration"));
            }};
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                properties.setProperty(entry.getKey().toString(), entry.getValue().toString());
            }
            return properties;
        } catch (Exception e) {
            log.error("Failed to load localised configuration for yarn1 context", e);
            return null;
        }
    }

    private static void executeShell(String command) throws IOException, InterruptedException {
        Process shellProcess = Runtime.getRuntime().exec(command);
        if (shellProcess.waitFor() != 0) {
            throw new IOException("Failed to command: " + command);
        }
    }

}

package net.imagini.yarn1;


import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class YarnClientWrapper {

    public static void createApplication(Configuration conf, Class<? extends YarnMaster> appClass, String[] args)
            throws Exception {

        Logger log = LoggerFactory.getLogger(appClass);
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
        YarnContextWrapper containerSpec = new YarnContextWrapper(log, conf, appClass.getSimpleName(), YarnMaster.class, newArgs);

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
        appContext.setApplicationName(appClass.getSimpleName());
        appContext.setResource(Resource.newInstance(masterMemoryMb, masterNumCores));
        appContext.setAMContainerSpec(containerSpec.getContainer(true));

        appContext.setPriority(masterPriority);
        appContext.setQueue(queue);

        yarnClient.submitApplication(appContext);

        long start = System.currentTimeMillis();
        log.info("Tracking URL: " + yarnClient.getApplicationReport(appId).getTrackingUrl());
        while (true) {
            Thread.sleep(1000);
            try {
                ApplicationReport report = yarnClient.getApplicationReport(appId);

                log.info(report.getApplicationId() + " " + report.getProgress() + " " + (System.currentTimeMillis() - report.getStartTime())
                        + "(ms) " + report.getDiagnostics());
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

}

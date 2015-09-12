package org.apache.yarn1.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.yarn1.YarnClient;
import org.apache.yarn1.YarnMaster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;

public class HelloYarn1Master extends YarnMaster {

    private static final Logger log = LoggerFactory.getLogger(HelloYarn1Master.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.addResource(new FileInputStream("/opt/envs/stag/etc/hadoop/core-site.xml"));
        conf.addResource(new FileInputStream("/opt/envs/stag/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new FileInputStream("/opt/envs/stag/etc/hadoop/yarn-site.xml"));

        YarnClient.submitApplicationMaster(conf, 0, "developers", false, HelloYarn1Master.class, args);
    }

    @Override
    protected void onStartUp(String[] args) throws Exception {
        requestContainerGroup(1, HelloYarn1WorkerA.class, args, 3, 1024, 1);
        requestContainerGroup(1, HelloYarn1WorkerB.class, args, 2, 512, 1);
    }

    @Override
    protected void onCompletion() {
        log.info("ALL CONTAINERS COMPLETED");
    }


}

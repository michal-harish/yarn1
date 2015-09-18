package org.apache.yarn1.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.yarn1.YarnClient;
import org.apache.yarn1.YarnContainerRequest;
import org.apache.yarn1.YarnMaster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class HelloYarn1Master extends YarnMaster {

    private static final Logger log = LoggerFactory.getLogger(HelloYarn1Master.class);

    public HelloYarn1Master(Configuration config) throws FileNotFoundException {
        super(config);
    }

    public static void main(final String[] args) throws Exception {
        Configuration config = new Configuration() {
            {
                if (args.length == 1) {
                    String yarnConfigPath = args[0];
                    System.out.println("Using yarn and hadoop config path: " + yarnConfigPath);
                    addResource(new FileInputStream(yarnConfigPath + "/core-site.xml"));
                    addResource(new FileInputStream(yarnConfigPath + "/hdfs-site.xml"));
                    addResource(new FileInputStream(yarnConfigPath + "/yarn-site.xml"));
                }
                set("yarn.name", "HelloYarn1");
                set("yarn.master.priority", "0");
                set("yarn.keepContainers", "false");
                set("yarn.master.queue", "developers");
            }
        };
        YarnClient.submitApplicationMaster(config, HelloYarn1Master.class, args, true);
    }

    @Override
    protected void onStartUp(String[] args) throws Exception {
        requestContainerGroup(2, new YarnContainerRequest(HelloYarn1WorkerA.class, args, 3, 1024, 1));
        requestContainerGroup(2, new YarnContainerRequest(HelloYarn1WorkerB.class, args, 2, 512, 1));
    }

    @Override
    protected void onCompletion() {
        log.info("ALL CONTAINERS COMPLETED");
    }


}

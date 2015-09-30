package org.apache.yarn1.example;

import org.apache.yarn1.YarnClient;
import org.apache.yarn1.YarnContainerRequest;
import org.apache.yarn1.YarnMaster;

import java.io.FileNotFoundException;
import java.util.Properties;

public class HelloYarn1Master extends YarnMaster {

    public HelloYarn1Master(Properties config) throws FileNotFoundException {
        super(config);
    }

    public static void main(final String[] args) throws Exception {
        Properties config = new Properties() {
            {
                if (args.length == 1) {
                    setProperty("yarn1.site", args[0]);
                }
                setProperty("yarn1.master.priority", "1");
                setProperty("yarn1.keepContainers", "false");
                setProperty("yarn1.queue", "developers");
                setProperty("yarn1.task.priority", "0");
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
        System.out.println("ALL CONTAINERS COMPLETED");
    }


}

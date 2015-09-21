package org.apache.yarn1.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.yarn1.YarnClient;
import org.apache.yarn1.YarnContainerRequest;
import org.apache.yarn1.YarnMaster;

import java.io.FileNotFoundException;

public class HelloYarn1Master extends YarnMaster {

    public HelloYarn1Master(Configuration config) throws FileNotFoundException {
        super(config);
    }

    public static void main(final String[] args) throws Exception {
        Configuration config = new Configuration() {
            {
                if (args.length == 1) {
                    set("yarn1.site", args[0]);
                }
                set("yarn1.master.priority", "0");
                set("yarn1.keepContainers", "false");
                set("yarn1.master.queue", "developers");
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

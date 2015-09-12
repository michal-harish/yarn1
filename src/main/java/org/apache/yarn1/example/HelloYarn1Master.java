package org.apache.yarn1.example;

import org.apache.yarn1.YarnMaster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloYarn1Master extends YarnMaster {

    private static final Logger log = LoggerFactory.getLogger(HelloYarn1Master.class);

    @Override
    protected void onStartUp(String[] args) throws Exception {
        requestContainerGroup(1, HelloYarn1WorkerA.class, args, 0, 1024, 1);
        requestContainerGroup(1, HelloYarn1WorkerB.class, args, 0, 1024, 1);
    }

    @Override
    protected void onCompletion() {
        log.info("ALL CONTAINERS COMPLETED");
    }


}

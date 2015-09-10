package net.imagini.yarn1.example;

import java.io.FileInputStream;
import java.util.Arrays;

import net.imagini.yarn1.YarnMaster;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloYarn1 extends YarnMaster {

    private static final Logger log = LoggerFactory.getLogger(HelloYarn1.class);

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        if (Arrays.asList(args).contains("--stag")) {
            conf.addResource(new FileInputStream("/opt/envs/stag/etc/hadoop/core-site.xml"));
            conf.addResource(new FileInputStream("/opt/envs/stag/etc/hadoop/hdfs-site.xml"));
            conf.addResource(new FileInputStream("/opt/envs/stag/etc/hadoop/yarn-site.xml"));
        }
        conf.set("master.queue", "developers");
        conf.setInt("master.priority", 0);
        conf.setLong("master.timeout.s", 600L);

        submitApplicationMaster(conf, HelloYarn1.class, args);
    }


    public HelloYarn1() throws Exception {
        super();
    }
    public void allocateContainers() {
        for(int i = 0; i < 800; i++) {
            requestContainer(0, HelloYarn1WorkerA.class, 1024, 1);
        }
        for(int i = 0; i < 12; i++) {
            requestContainer(0, HelloYarn1WorkerB.class, 2048, 4);
        }
    }


    @Override
    protected void onCompletion() {}


}

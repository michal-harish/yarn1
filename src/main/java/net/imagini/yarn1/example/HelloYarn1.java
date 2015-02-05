package net.imagini.yarn1.example;

import java.io.FileInputStream;
import java.util.Arrays;

import net.imagini.yarn1.YarnMaster;

import org.apache.hadoop.conf.Configuration;

public class HelloYarn1 extends YarnMaster {

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

    public void allocateContainers() throws Exception {
        submitContainer(0, HelloYarn1WorkerA.class, 1024, 1);
        submitContainer(0, HelloYarn1WorkerB.class, 2048, 4);
        submitContainer(0, HelloYarn1WorkerA.class, 1024, 1);
    }

}

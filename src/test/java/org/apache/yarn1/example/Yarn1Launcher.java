package org.apache.yarn1.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.yarn1.YarnClient;

import java.io.FileInputStream;

/**
 * Created by mharis on 11/09/15.
 */
public class Yarn1Launcher {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(new FileInputStream("/opt/envs/stag/etc/hadoop/core-site.xml"));
        conf.addResource(new FileInputStream("/opt/envs/stag/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new FileInputStream("/opt/envs/stag/etc/hadoop/yarn-site.xml"));

        YarnClient.submitApplicationMaster(conf, 0, "developers", HelloYarn1Master.class, args);
    }
}

package net.imagini.yarn1.example;

import java.io.FileInputStream;
import java.util.Arrays;

import net.imagini.yarn1.YarnMaster;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloYarn1 extends YarnMaster implements Watcher {

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

    private ZooKeeper zk;

    public HelloYarn1() throws Exception {
        super();
        zk = new ZooKeeper(getDefaultZooKeeperConnect(), 6000, this);
        zk.create( "/HelloYarn1", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }
    public void allocateContainers() {
        log.info("CONNECTED TO ZOOKEEPER: " + getDefaultZooKeeperConnect());
//        for(int i = 0; i < 16; i++) {
//            requestContainer(0, HelloYarn1WorkerA.class, 1024, 1);
//        }
        requestContainer(0, HelloYarn1WorkerB.class, 2048, 4);
    }

    @Override
    public void process(WatchedEvent event) {
        // TODO Auto-generated method stub
    }

    @Override
    protected void onCompletion() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            log.error("Could not close zookeeper connection", e);
        }
    }


}

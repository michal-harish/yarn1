# Simple YARN Executor

This is a YARN helper for submitting master and requesting containers by simply giving a class. At the moment this is based on hadoop/yarn 2.3.0-cdh5.0.3

1. [Features](#features)
3. [Quick Start](#quickstart)
2. [Configuration](#configuration) 	
4. [Operations](#operations)
5. [Development](#development)


<a name="features">
##Features 		 
</a>

- Easy deployment of distributed application by extending YarnMaster class 
- Requesting containers from the instance of YarnMaster also simply by giving container class
- Archiving classes and distribution done under the hood (dependency classes are included after mvn compile)
- Allows launching of fully distributed YARN application directly from IDE by using the launcher in the test package

<a name="quickstart">
## Quick start with included example application
</a>

This is in package `org.apache.yarn1.example`

### Running as `yarn` program
1. mvn clean package
2. yarn jar target/yarn1-example.jar

### Running from IDE
1. mvn clean compile - if your IDE doesn't support maven dependency plugin)
2. run Yarn1Launcher under the test sources as Java Application in your IDE with 1 application argument `<path_to_yarn_config>`

 
<a name="configuration">
## Configuration
</a>

parameter                   | default       | description
----------------------------|---------------|---------------------------------------------------------------------------
**yarn1.site**              | `/etc/hadoop` | Local path where the application is launched pointing to yarn (and hdfs-hadoop configuration) files. This path should contain at least these files: `yarn-site.xml`, `hdfs-site.xml`, `core-site.xml`
**yarn1.queue**             | -             | YARN scheduling queue name
**yarn1.keepContainers**    | `false`       | If set to `true` any failed container will be auto matically restarted (and also any containers that haven't failed will be kept across attempts).
yarn1.master.priority       | `0`           | Priority for the Application Master (`0-10`)
yarn1.master.memory.mb      | `256`         | Memory in megabytes for the Application Master
yarn1.master.num.cores      | `1`           | Number of virtual cores for the Application Master
yarn1.classpath             | -             | Optional colon-separated list of extra jars and paths available on YARN nodes locally for all containers and application master $CLASSPATH. This allows for large dependency libraries to be declared in scope `provided` and will not be distributed as part of container main jar, e.g. `opt/scala/scala-library-2.10.4.jar:/opt/scala/kafka_2.10-0.8.2.1.jar`.

<a name="operations">
## Operations
</a> 
### Quick setup of local YARN cluster
1. download 2.3.0-cdh5.0.3 bundle from cloudera, unapck and cd into the content
2. start yarn managers ./sbin/yarn-daemon.sh start resourcemanager && ./sbin/yarn-daemon.sh start
3. start hadoop cluster ./sbin/hadoop-daemon.sh start namenode && ./sbin/hadoop-daemon.sh start secondarynamenode && ./sbin/hadoop-daemon.sh start datanode
WITH SINGLE-NODE LOCAL YARN CLUSTER


<a name="development">
## Development
</a>

- onNodesUpdated behaviour
- distribute non-provided dependencies programatically, i.e. without having to run mvn compile first


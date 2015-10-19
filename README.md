# Simple YARN Executor

- **Author**: 2015 © Michal Harish (michal.harish AT gmail.com) 
- **License**: [GNU LGPL-3.0](LICENSE) 

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
1. `mvn clean package`
2. `./scripts/submit.example <PATH_TO_HADOOP_YARN_CONFIG>`  - see the script for details

### Running from IDE
1. mvn clean compile - if your IDE doesn't support maven dependency plugin)
2. run Yarn1Launcher under the test sources as Java Application in your IDE with 1 application argument `<path_to_yarn_config>`

### Developing your own YARN application

One option is to simply copy the yarn1 classes and dependencies into your project, the other is to add it as a git submodule:

```bash
    git submodule --add
    git submodule update --init    
```

```pom.xml
<modules>
    ...
    <module>yarn1</module>
    ...
</modules>
<plugins>
    ...
    <plugin>
        <groupId>org.codehaus.mojo</groupId>
        <artifactId>build-helper-maven-plugin</artifactId>
        <executions>
            <execution>
                <id>add-source</id>
                <phase>generate-sources</phase>
                <goals>
                    <goal>add-source</goal>
                </goals>
                <configuration>
                    <sources>
                        <source>yarn1/src/main/java</source>
                    </sources>
                </configuration>
            </execution>
            <execution>
                <id>add-test-source</id>
                <phase>generate-test-sources</phase>
                <goals>
                    <goal>add-test-source</goal>
                </goals>
                <configuration>
                    <sources>
                        <source>yarn1/src/test/java</source>
                    </sources>
                </configuration>
            </execution>
        </executions>
    </plugin>
    <plugin>
        <artifactId>maven-dependency-plugin</artifactId>
        <version>2.10</version>
        <executions>
            <execution>
                <id>extract</id>
                <phase>compile</phase>
                <goals>
                    <goal>unpack-dependencies</goal>
                </goals>
                <configuration>
                    <outputDirectory>target/classes</outputDirectory>
                    <includeScope>runtime</includeScope>
                    <includes>**/*.class,**/*.xml,**/*.properties</includes>
                </configuration>
            </execution>
        </executions>
    </plugin>
</plugins>
```
 
<a name="configuration">
## Configuration
</a>

parameter                       | default       | description
--------------------------------|---------------|---------------------------------------------------------------------------
**yarn1.site**                  | `/etc/hadoop` | Local path where the application is launched pointing to yarn (and hdfs-hadoop configuration) files. This path should contain at least these files: `yarn-site.xml`, `hdfs-site.xml`, `core-site.xml`
**yarn1.restart.enabled**       | `false`       | If set to `true` any completed or failed containers will be automatically restarted.
**yarn1.restart.failed.retries**| 5             | If restart.enabled is `true` any container that completes with non-zero exit status more than `failed.retries` time will cause the entire application to fail
yarn1.application.type          | `YARN`        | Application type to be registered with the Resource Manager
yarn1.client.tracking.url       | -             | Optional tracking url that is available on the client machine, i.e. not on Yarn AM but running locally prior to submission of the job 
yarn1.classpath                 | -             | Optional colon-separated list of extra jars and paths available on YARN nodes locally for all containers and application master $CLASSPATH. This allows for large dependency libraries to be declared in scope `provided` and will not be distributed as part of container main jar, e.g. `opt/scala/scala-library-2.10.4.jar:/opt/scala/kafka_2.10-0.8.2.1.jar`.
yarn1.jvm.args                  | -             | Extra JVM arguments besides the main memory which is managed under the hood as calculated from direct+heap memory as given in each container request, , e.g. `-XX:+UseSerialGC -XX:NewRatio=3`
yarn1.queue                     | -             | YARN scheduling queue name for master as well as containers
yarn1.env.<VARIABLE>            | -             | Optional Environment Variable(s) for each task
yarn1.master.priority           | `0`           | Priority for the Application Master (`0-10`)
yarn1.master.memory.mb          | `256`         | Memory in megabytes for the Application Master
yarn1.master.num.cores          | `1`           | Number of virtual cores for the Application Master
yarn1.master.jvm.args           | -             | Optional JVM arguments for the master. e.g. `-Xmx:512m`
yarn1.local.mode                | `false`       | Debugging option - set tot `true` to run the master and all tasks in a local process executor

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
- distributing jar uses main class name on the target so the identical jar will be copied multiple times when several components are launched 
- hdfs could be completely avoided - also hdfs.homeDirectory() of the current user is used right now
- use standard HADOOP_YARN_HOME environmental variable instead of combination of args and yarn1.site config
- YARN deployment requires 'mvn' command for unpacking of compile scope dependencies and 'jar' command for creating the main jar 
- expose running containers list and onContainerAllocated for application to register its own servers etc.
- YarnMaster.onNodesUpdated behaviour



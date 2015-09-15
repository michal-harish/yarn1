package org.apache.yarn1;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class YarnContainer {

    final private String jarName;
    final public Resource capability;
    final public Priority priority;
    final public Class<?> mainClass;
    final public String mainClassName;
    final private Configuration conf;

    final private String[] args;
    private static final Logger log = LoggerFactory.getLogger(YarnContainer.class);

    public YarnContainer(
            Configuration conf, int priority, int memoryMb, int numCores, String appName, Class<?> mainClass, String[] args
    ) throws Exception {
        this.conf = conf;
        this.conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        this.conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        this.jarName = appName + ".jar";
        this.mainClass = mainClass;
        this.mainClassName = mainClass.getName().replace("$", "");
        this.capability = Resource.newInstance(memoryMb, numCores);
        this.priority = Priority.newInstance(priority);
        this.args = args;
    }

    public ContainerLaunchContext createContainerLaunchContext() throws IOException {
        ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
        context.setEnvironment(prepareEnvironment());
        context.setCommands(prepareCommands());
        context.setLocalResources(prepareLocalResources());
        Map<String, ByteBuffer> serviceData = Maps.newHashMap();
        context.setServiceData(serviceData);
        return context;
    }

    public boolean isSatisfiedBy(Container container) {
        return container.getResource().getMemory() >= capability.getMemory()
                && container.getResource().getVirtualCores() >= capability.getVirtualCores()
                && container.getPriority().getPriority() >= priority.getPriority();
    }

    private Map<String, String> prepareEnvironment() {
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$());
        classPathEnv.append(":"); // ApplicationConstants.CLASS_PATH_SEPARATOR
        classPathEnv.append(Environment.PWD.$());
        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classPathEnv.append(":");// ApplicationConstants.CLASS_PATH_SEPARATOR
            classPathEnv.append(c.trim());
        }
        if (conf.get("yarn.classpath") != null) {
            classPathEnv.append(":");
            classPathEnv.append(conf.get("yarn.classpath").trim());
        }
        Map<String, String> env = Maps.newHashMap();
        env.put(Environment.CLASSPATH.name(), classPathEnv.toString());
        log.info("$CLASSPATH = " + classPathEnv.toString() );
        return env;

    }

    private List<String> prepareCommands() {
        String command = "java -cp $CLASSPATH:./" + jarName + " " + mainClassName + " " + StringUtils.join(" ", args);
        command += " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout";
        command += " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
        log.info("$COMMAND = " + command);
        return Arrays.asList(command);
    }

    private Map<String, LocalResource> prepareLocalResources() throws IOException {
        Map<String, LocalResource> localResources = Maps.newHashMap();
        FileSystem distFs = FileSystem.get(conf);

        prepareLocalResourceFile(localResources, jarName, jarName, distFs);
        prepareLocalResourceFile(localResources, "yarn1.configuration", jarName.replace(".jar", ".configuration"), distFs);

        distFs.close();
        return localResources;

    }

    private void prepareLocalResourceFile(Map<String, LocalResource> localResources, String fileName, String remoteFileName, FileSystem distFs) throws IOException {
        final Path dst = new Path(distFs.getHomeDirectory(), remoteFileName);
        FileStatus scFileStatus = distFs.getFileStatus(dst);
        final URL yarnUrl = ConverterUtils.getYarnUrlFromURI(dst.toUri());
        LocalResource scRsrc = LocalResource.newInstance(
                yarnUrl, LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileName, scRsrc);
    }
}

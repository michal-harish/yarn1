package org.apache.yarn1;

/**
 * Yarn1 - Java Library for Apache YARN
 * Copyright (C) 2015 Michal Harish
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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
import java.util.Properties;

public class YarnContainer {

    private static final Logger log = LoggerFactory.getLogger(YarnContainer.class);

    final public Class<?> mainClass;
    final public String mainClassName;
    final public String[] args;
    final public String jvmArgs;
    final public Resource capability;
    final public Priority priority;

    final private Configuration yarnConfig;
    final private Properties appConfig;
    final private String jarName;
    private int numFailures = 0;
    private Container container;
    private ContainerStatus status;

    public YarnContainer(
            Configuration yarnConfig, Properties appConfig,
            String jvmArgs,
            int priority, int memoryMb, int numCores, String appName, Class<?> mainClass, String[] args
    ) throws Exception {
        this.appConfig = appConfig;
        this.yarnConfig = yarnConfig;
        this.jvmArgs = jvmArgs;
        this.yarnConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        this.yarnConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

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
        for (String c : yarnConfig.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classPathEnv.append(":");// ApplicationConstants.CLASS_PATH_SEPARATOR
            classPathEnv.append(c.trim());
        }
        if (appConfig.containsKey("yarn1.classpath")) {
            classPathEnv.append(":");
            classPathEnv.append(appConfig.getProperty("yarn1.classpath").trim());
        }
        Map<String, String> env = Maps.newHashMap();
        for(Map.Entry<Object, Object> e: appConfig.entrySet()) {
            String key = e.getKey().toString();
            String value = e.getValue().toString();
            if (key.startsWith("yarn1.env.")) {
                log.info("$" + key.substring(10) + " = " + value);
                env.put(key.substring(10), value);
            }
        }
        env.put(Environment.CLASSPATH.name(), classPathEnv.toString());
        log.info("$CLASSPATH = " + classPathEnv.toString());
        return env;

    }

    private List<String> prepareCommands() {
        String command = "java " + jvmArgs + " -cp $CLASSPATH:./" + jarName + " " + mainClassName + " " + StringUtils.join(" ", args);
        command += " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout";
        command += " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
        log.info("$COMMAND = " + command);
        return Arrays.asList(command);
    }

    private Map<String, LocalResource> prepareLocalResources() throws IOException {
        Map<String, LocalResource> localResources = Maps.newHashMap();
        FileSystem distFs = FileSystem.get(yarnConfig);

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

    public int incrementAndGetNumFailures() {
        this.numFailures +=1;
        return numFailures;
    }

    public void assignContainer(Container container) {
        this.container = container;
    }

    public String getNodeAddress() {
        return container == null ? "N/A" : container.getNodeHttpAddress();
    }
    public String getLogsUrl() {
        return container == null ? "N/A"
                : "http://" + container.getNodeHttpAddress() + "/node/containerlogs/" + container.getId();
    }
    public String getLogsUrl(String out) {
        return container == null ? "N/A" : getLogsUrl() + "/"+out+"/"+out+"/?start=-4096";
    }

    public void assignStatus(ContainerStatus status) {
        this.status = status;
    }

    public ContainerStatus getStatus() {
        return this.status;
    }
}

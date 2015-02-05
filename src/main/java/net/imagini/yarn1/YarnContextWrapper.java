package net.imagini.yarn1;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

public class YarnContextWrapper {

    final private Configuration conf;
    final private String appName;
    final private String localPath;
    final private boolean localPathIsJar;
    final private Class<?> appClass;
    final private FileSystem distFs;
    final private FileSystem localFs;
    final private String appVersion;
    final private List<String> args;
    private Logger log;

    public YarnContextWrapper(Logger log, Configuration conf, String appName, Class<?> appClass, List<String> args) throws Exception {
        this.conf = conf;
        this.log = log;
        this.conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        this.conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        this.distFs = FileSystem.get(conf);
        this.localFs = FileSystem.getLocal(conf);
        this.appClass = appClass;
        this.localPath = appClass.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
        this.localPathIsJar = localPath.endsWith(".jar");
        this.appName = appName;
        // TODO move versioning to the application
        if (localPathIsJar) {
            this.appVersion = Resources.toString(this.getClass().getResource("/version.properties"), Charsets.UTF_8);//
        } else {
            this.appVersion = new String(Files.readAllBytes(Paths.get(localPath + "version.properties")));
        }
        this.args = args;
    }

    public ContainerLaunchContext getContainer(boolean masterContext) throws IOException {
        ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
        context.setEnvironment(prepareEnvironment());
        context.setCommands(prepareCommands(masterContext));
        context.setLocalResources(prepareLocalResource(appName, masterContext));

        // TODO provide way of passing arbitrary input (figure out how they can
        // be accessed on the other end)
        Map<String, ByteBuffer> serviceData = Maps.newHashMap();
        context.setServiceData(serviceData);

        return context;
    }

    private Map<String, String> prepareEnvironment() {
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$());
        classPathEnv.append(":"); // ApplicationConstants.CLASS_PATH_SEPARATOR
        classPathEnv.append(Environment.PWD.$());
        if (localPathIsJar) {
            classPathEnv.append(":"); // ApplicationConstants.CLASS_PATH_SEPARATOR
            classPathEnv.append(new Path(localPath).getName());
        }
        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classPathEnv.append(":");// ApplicationConstants.CLASS_PATH_SEPARATOR
            classPathEnv.append(c.trim());
        }
        Map<String, String> env = Maps.newHashMap();
        env.put(Environment.CLASSPATH.name(), classPathEnv.toString());
        return env;

    }

    private List<String> prepareCommands(boolean isMasterContext) {
        //ls -l net/imagini/yarn1 >> /var/log/helloyarn.log && 
        String command = "java " + appClass.getName() + " " + StringUtils.join(" ", args);
//        command += " >> /var/log/helloyarn.log 2>1";
        command += " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout";
        command += " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
        return Arrays.asList(command);
    }

    private Map<String, LocalResource> prepareLocalResource(String relativePrefix, boolean masterContext) throws IOException {
        Map<String, LocalResource> localResources = Maps.newHashMap();
        if (localPathIsJar) {
            addLocalFile("", relativePrefix, localResources);
        } else {
            addAllClassAndPropertiesFiles(localPath.toString(), relativePrefix, localResources);
            // TODO if not running from a fat jar then we need to add all dependencies
        }
        return localResources;

    }

    private void addAllClassAndPropertiesFiles(String startPath, String relativePrefix, Map<String, LocalResource> localResources)
            throws IOException {
        java.nio.file.Path dir = FileSystems.getDefault().getPath(startPath);
        DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(dir);
        for (java.nio.file.Path path : stream) {
            if (path.toFile().isDirectory()) {
                addAllClassAndPropertiesFiles(path.toString(), relativePrefix, localResources);
            } else if (path.getFileName().toString().matches(".*\\.(class|properties)$")) {
                String relativeLocalFile = path.toString().substring(localPath.toString().length());
                addLocalFile(relativeLocalFile, relativePrefix, localResources);
            }
        }
        stream.close();
    }

    private void addLocalFile(String relativeLocalPath, String relativePrefix, Map<String, LocalResource> localResources)
            throws IOException {
        Path localFile = new Path(localPath + relativeLocalPath);
        String relativeDestFile = localPathIsJar ? new Path(localPath).getName() : relativeLocalPath;
        Path dst = new Path(distFs.getHomeDirectory(), relativePrefix + "/" + appVersion + "/" + relativeDestFile);
        URL yarnUrl = ConverterUtils.getYarnUrlFromURI(dst.toUri());
        long localModTime = localFs.getFileStatus(localFile).getModificationTime();
        if (!distFs.exists(dst) || (distFs.getFileStatus(dst).getModificationTime() < localModTime)) {
            log.info("Updating resource " + yarnUrl.getFile());
            distFs.copyFromLocalFile(localFile, dst);
        }
        FileStatus scFileStatus = distFs.getFileStatus(dst);
        LocalResource scRsrc = LocalResource.newInstance(yarnUrl, LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(relativeDestFile, scRsrc);
    }
}

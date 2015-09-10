package net.imagini.yarn1;

import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

public class YarnContext {

    final private Configuration conf;
    final private String appName;
    final private String localPath;
    final private boolean localPathIsJar;
    final private Class<?> appClass;
    final private FileSystem distFs;
    final private String appVersion;
    final private List<String> args;
    private static final Logger log = LoggerFactory.getLogger(YarnContext.class);

    public YarnContext(Configuration conf, String appName, Class<?> appClass, List<String> args) throws Exception {
        this.conf = conf;
        this.conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        this.conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        this.distFs = FileSystem.get(conf);
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
        context.setCommands(prepareCommands());
        context.setLocalResources(prepareLocalResource(appName, masterContext));
        distFs.close();
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
        } else {
            classPathEnv.append(":"); // ApplicationConstants.CLASS_PATH_SEPARATOR
            classPathEnv.append(new Path(localPath + appVersion + ".gz").getName());
        }
        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classPathEnv.append(":");// ApplicationConstants.CLASS_PATH_SEPARATOR
            classPathEnv.append(c.trim());
        }
        Map<String, String> env = Maps.newHashMap();
        env.put(Environment.CLASSPATH.name(), classPathEnv.toString());
        return env;

    }

    private List<String> prepareCommands() {
        String command = "tar -xzf "+appVersion+".gz && java " + appClass.getName() + " " + StringUtils.join(" ", args);
        command += " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout";
        command += " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
        return Arrays.asList(command);
    }

    private Map<String, LocalResource> prepareLocalResource(String relativePrefix, boolean masterContext) throws IOException {
        Map<String, LocalResource> localResources = Maps.newHashMap();
        if (localPathIsJar) {
            String jarName = new Path(localPath).getName();
            Path dst = new Path(distFs.getHomeDirectory(), relativePrefix + "/" + jarName);
            distFs.copyFromLocalFile(new Path(localPath), dst);
            FileStatus scFileStatus = distFs.getFileStatus(dst);
            URL yarnUrl = ConverterUtils.getYarnUrlFromURI(dst.toUri());
            log.info("Updating resource " + yarnUrl.getFile());
            LocalResource scRsrc = LocalResource.newInstance(yarnUrl, LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION,
                    scFileStatus.getLen(), scFileStatus.getModificationTime());
            localResources.put(jarName, scRsrc);
        } else {
            String localArchive = localPath + appVersion+".gz";
            try {
                if (masterContext) {
                    log.info("Archiving " + localArchive);
                    Process tarProcess = Runtime.getRuntime().exec("tar -C " + localPath + " -czf " + localArchive + " ./");
                    if (tarProcess.waitFor() != 0) {
                        throw new IOException("Failed to executre tar -C command on: " + localPath);
                    }
                }
                Path dst = new Path(distFs.getHomeDirectory(), relativePrefix + "/" + appVersion + ".gz");
                distFs.copyFromLocalFile(new Path(localArchive), dst);
                FileStatus scFileStatus = distFs.getFileStatus(dst);
                URL yarnUrl = ConverterUtils.getYarnUrlFromURI(dst.toUri());
                log.info("Updating resource " + yarnUrl.getFile() + " " + scFileStatus.getLen());
                LocalResource scRsrc = LocalResource.newInstance(yarnUrl, LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
                localResources.put(appVersion+".gz", scRsrc);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        return localResources;

    }
}

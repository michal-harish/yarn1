package org.apache.yarn1;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by mharis on 15/09/15.
 * <p/>
 * This is an internal class used to distribute Configuration given to the YarnClient when submittin an application.
 * It distributed by YarnClient.distributeResources and instantiated by both Master and Containers.
 */
public class Yarn1Configuration extends org.apache.hadoop.yarn.conf.YarnConfiguration {

    Yarn1Configuration() {
        this("yarn1.configuration");
    }

    public Yarn1Configuration(String filename) {
        super();
        FileInputStream fs = null;
        try {
            fs = new FileInputStream(filename);
            readFields(new DataInputStream(fs));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                }
            }
        }
    }

}

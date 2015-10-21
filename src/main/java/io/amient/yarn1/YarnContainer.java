package io.amient.yarn1;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by mharis on 19/10/15.
 */
final public class YarnContainer {

    public static void main(String[] args) {
        try {
            try {
                Properties config = YarnClient.getAppConfiguration();
                Runnable taskInstance = prepareRunnable(config, args);
                taskInstance.run();
            } catch (Throwable e) {
                e.printStackTrace();
                System.exit(22);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(21);
        }
    }

    public static Runnable prepareRunnable(Properties config, String[] args) throws Exception {
        Class<? extends Runnable> mainClass = Class.forName(args[0]).asSubclass(Runnable.class);
        Constructor<? extends Runnable> c = mainClass.getConstructor(Properties.class, String[].class);
        String[] originalArgs = Arrays.copyOfRange(args, 1, args.length);
        return c.newInstance(config, originalArgs);
    }

}

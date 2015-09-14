package org.apache.yarn1;

/**
 * Created by mharis on 14/09/15.
 */
public class YarnContainerRequest {
    public final Class<?> mainClass;
    public final String[] args;
    public final int priority;
    public final int memoryMb;
    public final int numCores;

    public YarnContainerRequest(Class<?> mainClass, String[] args, int priority, int memoryMb, int numCores) {
        this.mainClass = mainClass;
        this.args = args;
        this.priority = priority;
        this.memoryMb = memoryMb;
        this.numCores = numCores;
    }
}

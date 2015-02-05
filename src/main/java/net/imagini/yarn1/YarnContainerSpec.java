package net.imagini.yarn1;


import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

public class YarnContainerSpec {
    private Resource capability;
    private Priority priority;
    private Class<?> mainClass;

    public YarnContainerSpec(Resource capability, Priority priority,
            Class<?> mainClass) {
        this.capability = capability;
        this.priority = priority;
        this.mainClass = mainClass;
    }

    @Override
    public String toString() {
        return "priority " + priority.getPriority() + ", "+ capability.getMemory() +"Mb, " + capability.getVirtualCores() + "vcores"; 
    }

    @Override
    public int hashCode() {
        return capability.hashCode() + priority.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof YarnContainerSpec) {
            return ((YarnContainerSpec) obj).capability.getMemory() == capability.getMemory()
                    && ((YarnContainerSpec) obj).capability.getVirtualCores() == capability.getVirtualCores()
                    && ((YarnContainerSpec) obj).priority.equals(priority);
        } else {
            return false;
        }
    }

    public Class<?> getMainClass() {
        return mainClass;
    }

    public boolean isSatisfiedBy(Container container) {
        return container.getResource().getMemory() >= capability.getMemory()
                && container.getResource().getVirtualCores() >= capability.getVirtualCores()
                && container.getPriority().getPriority() >= priority.getPriority();
    }

    public Resource getCapability() {
        return capability;
    }

    public Priority getPriority() {
        return priority;
    }
}

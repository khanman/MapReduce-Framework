package mr.common;

import java.io.Serializable;

/**
 * 
 * Object used for storing the system and JVM specifications.
 * 
 * 
 * 
 */
@SuppressWarnings("serial")
public class SystemSpecs implements Serializable {

    /**
     * Get the number of logical CPU cores
     * 
     * @return count of CPU cores
     */
    public int getAvailableCpuCores() {
        return availableCpuCores;
    }

    /**
     * Set the number of logical CPU cores
     * 
     * @param availableCpuCores - count of CPU cores
     */
    public void setAvailableCpuCores(int availableCpuCores) {
        this.availableCpuCores = availableCpuCores;
    }

    /**
     * Get the maximum available memory of JVM
     * 
     * @return maximum available memory in bytes
     */
    public long getMaxMemoryAvailable() {
        return maxMemoryAvailable;
    }

    /**
     * Set the maximum available memory of JVM
     * 
     * @param maxMemoryAvailable
     */
    public void setMaxMemoryAvailable(long maxMemoryAvailable) {
        this.maxMemoryAvailable = maxMemoryAvailable;
    }

    @Override
    public String toString() {
        return "SystemSpecs [availableCpuCores=" + availableCpuCores
                + ", maxMemoryAvailable=" + maxMemoryAvailable + "]";
    }

    private int availableCpuCores;
    private long maxMemoryAvailable;

}

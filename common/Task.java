package mr.common;

import java.io.File;
import java.io.Serializable;
import java.net.SocketAddress;

/**
 * <pre>
 * Task represents a unit of work used by the MR framework. Currently, the
 * framework supports three types of tasks which are, 
 * 
 * 1)Map Task
 * 2)Reduce Task
 * 3)End Task
 * </pre>
 * 
 * 
 * 
 */
public class Task implements Comparable<Task>, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * <pre>
     * TYPE represents the Type of this task. Each type is associated with a
     * default priority passed as argument to the constructor. The task types
     * and priorities are listed below. 
     *  
     * 1)TYPE.MAP, priority = 5 
     * 2)TYPE.REDUCE,priority = 6
     * 3)TYPE.END, priority = 0
     * 
     * </pre>
     * @author Magesh Ramachandran
     * 
     */
    public enum TYPE {
        MAP(5), REDUCE(6), END(0);
        private int defaultPriority;

        TYPE(int priority) {
            this.defaultPriority = priority;
        }
    };

    /**
     * Constructor
     * 
     * @param type {@link Task.TYPE} type of the task
     * @param taskId: the id of the task
     */
    public Task(TYPE type, String taskId) {
        this.taskType = type;
        this.priority = type.defaultPriority;
        this.taskId = taskId;
    }

    private TYPE taskType;
    private long creationTime;
    private int priority;
    private String taskId;
    private SocketAddress[] reducers;
    private int numberOfMapHosts;

    /**
     * Get the number of hosts(TaskTrackers) running the map tasks
     * 
     * @return number of hosts running the map tasks
     */
    public int getNumberOfMapHosts() {
        return numberOfMapHosts;
    }

    /**
     * Set the number of hosts(TaskTrackers) running the map tasks
     * 
     * @param numberOfMappers: number of hosts running map tasks
     */
    public void setNumberOfMapHosts(int numberOfMappers) {
        this.numberOfMapHosts = numberOfMappers;
    }

    /**
     * Get the array of SocketAddress of all 'reducer' TaskTrackers
     * 
     * @return array of {@link SocketAddress}
     */
    public SocketAddress[] getReducers() {
        return reducers;
    }

    /**
     * Set the array of SocketAddress of all 'reducer' TaskTrackers
     * 
     * @param reducers: array of {@link SocketAddress}
     */
    public void setReducers(SocketAddress[] reducers) {
        this.reducers = reducers;
    }

    // Only used by map tasks
    private transient File splitFileForTask;

    /**
     * Get the temporary file associated with this map task
     * 
     * @return the temporary file corresponding to this map task
     */
    public File getSplitFileForTask() {
        return splitFileForTask;
    }

    /**
     * Set the temporary split file associated with this map task
     * 
     * @param splitFileForTask: the temporary file associated with this map task
     */
    public void setSplitFileForTask(File splitFileForTask) {
        this.splitFileForTask = splitFileForTask;
    }

    /**
     * Get the task id of this task
     * 
     * @return task id of this task
     */
    public String getTaskId() {
        return this.taskId;
    }

    /**
     * Get the Type of this task
     * 
     * @return {@link Task.TYPE}
     */
    public TYPE getType() {
        return this.taskType;
    }

    /**
     * Get the creation time of this task
     * 
     * @return creation time in milliseconds
     */
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * Set the creation time of this task
     * 
     * @param creationTime: creation time in milliseconds
     */
    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    /**
     * Get the priority of this task
     * 
     * @return integer representing the priority of this task
     */
    public int getPriority() {
        return priority;
    }

    /**
     * Set the priority of this task
     * 
     * @param priority: integer representing the priority of this task
     */
    public void setPriority(int priority) {
        this.priority = priority;
    }

    /**
     * Sorted by the priority, higher priority Tasks sort first. If two tasks have the
     * same priority, then the task with the earliest creation time is sorted first
     */
    public int compareTo(Task other) {
        int diff = other.priority - this.priority;
        if (diff == 0) {
            diff = (int) (this.creationTime - other.creationTime);
        }
        return diff;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((taskId == null) ? 0 : taskId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Task other = (Task) obj;
        if (taskId == null) {
            if (other.taskId != null)
                return false;
        } else if (!taskId.equals(other.taskId))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Task [currentTask=" + taskType + ", creationTime="
                + creationTime + ", priority=" + priority + ", taskId="
                + taskId + "]";
    }

}

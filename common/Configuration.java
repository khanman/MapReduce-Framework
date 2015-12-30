package mr.common;

import java.io.Serializable;

import mr.worker.Mapper;
import mr.worker.Reducer;

/**
 * Configuration object, contains the job specific data supplied by the client program
 * which is required by the Map Reduce framework during execution.
 * 
 * The job specific data from the client program such as the 'mapper class', reducer
 * class', 'number of reducers to use' are stored in an instance of the
 * {@link Configuration} object and sent to all the workers {@link TaskTracker} using
 * serialization.
 * 
 * 
 * 
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class Configuration implements Serializable {

    private String mapperClass;
    private String reducerClass;

    private int splitSize;
    private int numberOfReducers = 1;

    /**
     * Get the number of reducers
     * 
     * @return number of reducers
     */
    public int getNumberOfReducers() {
        return numberOfReducers;
    }

    /**
     * Set the number of reducers
     * 
     * @param numberOfReducers: the number of reducers for the current job
     */
    public void setNumberOfReducers(int numberOfReducers) {
        this.numberOfReducers = numberOfReducers;
    }

    /**
     * Get the split size of the file split
     * 
     * @return split size in bytes
     */
    public int getSplitSize() {
        return splitSize;
    }

    /**
     * Set the split size in bytes
     * 
     * @param splitSize number of bytes to use for input split
     */
    public void setSplitSize(int splitSize) {
        this.splitSize = splitSize;
    }

    /**
     * Get the String representation of the mapper class
     * 
     * @return: String representation of the mapper class
     */
    public String getMapperClass() {
        return mapperClass;
    }

    /**
     * Set the mapper class to be used for the current job
     * 
     * @see Class
     * @see Mapper
     * 
     * @param mapperClass {@link Class} of the mapper class
     */
    public void setMapperClass(Class<? extends Mapper> mapperClass) {
        this.mapperClass = mapperClass.getName();
    }

    /**
     * Get the string representation of the reducer class
     * 
     * @return: String representation of the reducer class
     */
    public String getReducerClass() {
        return reducerClass;
    }

    /**
     * Set the mapper class to be used for the current job
     * 
     * @see Class
     * @see Reducer
     * 
     * @param reducerClass {@link class} of the reducer class
     */
    public void setReducerClass(Class<? extends Reducer> reducerClass) {
        this.reducerClass = reducerClass.getName();
    }
}

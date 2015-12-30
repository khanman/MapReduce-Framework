package mr.master;

import java.io.File;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import mr.common.Configuration;
import mr.worker.Mapper;
import mr.worker.Reducer;

/**
 * <pre>
 * Program that initiates the map reduce process.
 *  
 * By using the {@link ResourceManager}, performs the following actions
 * 1) Establishes connection with the TaskTracker nodes
 * 2) Assigns tasks (and file splits if applicable) to to each TaskTracker node.
 * 3) Waits till all Tasks are processed (or till any task failure). *
 * </pre>
 * 
 * @see ResourceManager
 * 
 * @author Magesh Ramachandran
 * 
 */
@SuppressWarnings({ "rawtypes" })
public class ApplicationMaster {

    private String configFileName;
    private String inputPath;
    private String outputPath;
    private Configuration config;

    // To get the jar file path
    private Class mapperClass;
    public static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

    public ApplicationMaster() {
        config = new Configuration();
    }

    /**
     * Set the number of reducers for the job. This value might be overridden by the
     * application logic to allow for at least one mapper
     * 
     * @param numberOfReducers
     */
    public void setNumberOfReducers(int numberOfReducers) {
        config.setNumberOfReducers(numberOfReducers);
    }

    /**
     * Set the mapper class
     * 
     * @param mapperClass {@link Class}
     * @see Mapper
     */
    public void setMapperClass(Class<? extends Mapper> mapperClass) {
        this.mapperClass = mapperClass;
        config.setMapperClass(mapperClass);
    }

    /**
     * Get the client jar file
     * Source:http://stackoverflow.com/questions/320542/how-to-get-the-path-of-a
     * -running-jar-file
     * 
     * @param mapperClass
     */
    private File getJarFilePathFromClass(Class mapperClass) {
        try {
            File jarFile =
                    new File(mapperClass.getProtectionDomain().getCodeSource()
                            .getLocation().toURI().getPath());
            return jarFile;
        } catch (URISyntaxException e) {
            LOG.fatal("Error while getting the jar file from class", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the mapper class name
     * 
     * @return name of the mapper class
     */
    public String getMapperClass() {
        return config.getMapperClass();
    }

    /**
     * Set the reducer class
     * 
     * @param reducerClass {@link Class}
     * @see Reducer
     */
    public void setReducerClass(Class<? extends Reducer> reducerClass) {
        config.setReducerClass(reducerClass);
    }

    /**
     * Get the reducer class name
     * 
     * @return name of the reducer class
     */
    public String getReducerClass() {
        return config.getReducerClass();
    }

    /**
     * sets the name with path of the configuration file
     * 
     * @param configFileName
     */
    public void setConfigFile(String configFileName) {
        this.configFileName = configFileName;

    }

    /**
     * Get the name of the configuration file
     * 
     * @return the config file name
     */
    public String getConfigFile() {
        return configFileName;
    }

    /**
     * Set the input file path
     * 
     * @param path
     */
    public void setInputPath(String path) {
        this.inputPath = path;

    }

    /**
     * Get the input file path
     * 
     * @return
     */
    public String getInputPath() {
        return inputPath;
    }

    /**
     * Set the output path
     * 
     * @param path
     */
    public void setOutputPath(String path) {
        this.outputPath = path;

    }

    /**
     * Get the output path
     * 
     * @return the output path
     */
    public String getOutputPath() {
        return outputPath;
    }

    /**
     * Initiates the map-reduce process and waits for completion.
     * 
     * @return true if the execution was successful, otherwise false
     */
    public boolean waitForCompletion() {
        File jarFile = getJarFilePathFromClass(this.mapperClass);
        ResourceManager resourceManager =
                new ResourceManager(inputPath, jarFile, config);
        return resourceManager.start();
    }

}

package mr.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import mr.common.Constants.Config;
import mr.common.Constants.Errors;

/**
 * Parses the configuration file and create a map for the supported attributes. Include
 * convenience methods for retrieving key attributes from the attribute map
 * 
 * 
 * 
 */
public class CfgParser {

    private String registryHostName;
    private int registryListenerPort;
    private int registryRequesterPort;
    private String classpath;
    private long maxUsableMemory;
    private String jvmHeapSizeStr;
    private long jvmHeapSizeInBytes;
    private int numberOfTaskSlots;
    private static final long mb = (1024 * 1024);
    private static final long gb = (1024 * 1024 * 1024);

    /**
     * Get the number of task slots per NodeManager
     * 
     * @return number of task slots
     */
    public int getNumberOfTaskSlots() {
        return numberOfTaskSlots;
    }

    /**
     * Get the jvm heap size to be used for each TaskTracker
     * 
     * @return string representing the jvm heap size (examples: 2g, 2048m)
     * 
     */
    public String getJvmHeapSizeStr() {
        return jvmHeapSizeStr;
    }

    /**
     * Get the jvm HeapSize to be used in bytes
     * 
     * @return Heap size as the number of bytes
     */
    public long getJvmHeapSizeInBytes() {
        return jvmHeapSizeInBytes;
    }

    /**
     * Get maximum usable memory from the configuration file
     * 
     * @return maximum usable memory in bytes
     */
    public long getMaxUsableMemory() {
        return maxUsableMemory;
    }

    // Default file split size
    /* 64MB */
    private int splitSize = 67108864;

    private static CfgParser cfgParser = null;
    private Map<String, String> configMap = new HashMap<String, String>();

    public static final String DEFAULT_CFG_FILE = "cfg" + File.separator
            + "config.txt";

    /**
     * Constructor
     * 
     * @param configFileName - configuration file name (if not given, looks in the current
     *            path for "cfg/config.txt"
     */
    private CfgParser(String configFileName) {
        parse(configFileName);
        updateState();
    }

    /**
     * Create a new instance of CfgParser or returns an existing instance if one is
     * already available
     * 
     * @param cfgFileName - configuration file name, if null uses the default path
     * @return {@link CfgParser}
     */
    public static CfgParser getInstance(String cfgFileName) {
        if (cfgParser == null) {
            cfgParser = new CfgParser(cfgFileName);
        }
        return cfgParser;
    }

    /**
     * Parses the given configuration file and updates the configMap. If no file name is
     * given, parses the configuration file in the default location. The configuration
     * file has one key-value per line delimited by a "="
     * 
     * @param fName - Configuration file name
     */
    public void parse(String fName) {
        BufferedReader br = null;
        String fileName = (fName != null) ? fName : DEFAULT_CFG_FILE;

        try {
            FileReader fr = new FileReader(fileName);
            br = new BufferedReader(fr);
            String line = null;

            while ((line = br.readLine()) != null) {
                String[] splits = line.split("=");

                if (splits.length != 2) {
                    throw new RuntimeException(Errors.INVALID_CONFIG_FORMAT);
                }
                String attributeKey = splits[0].trim().toLowerCase();
                String attributeValue = splits[1].trim();

                // Only adds the supported attribute to the configMap
                if (Config.SUPPORTED_ATTRIBUTES.contains(attributeKey)) {
                    configMap.put(attributeKey, attributeValue);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    /**
     * Updates the attributes of this object with the values from the ConfigMap. Default
     * values are assigned for attributes that are not available in the config file.
     */
    private void updateState() {

        registryHostName = configMap.get(Config.REGISTRY_HOST_NAME);
        String registryListenerPort =
                configMap.get(Config.REGISTRY_LISTENER_PORT);
        String registryRequesterPort =
                configMap.get(Config.REGISTRY_REQUESTER_PORT);
        String splitSizeString = configMap.get(Config.SPLIT_SIZE);

        registryListenerPort =
                (registryListenerPort == null) ? "8000" : registryListenerPort;
        registryRequesterPort =
                (registryRequesterPort == null) ? "8001"
                        : registryRequesterPort;

        String numOfSlots = configMap.get(Config.NUMBER_OF_TASK_SLOTS);

        if (numOfSlots != null) {
            this.numberOfTaskSlots = Integer.parseInt(numOfSlots);
        }

        this.registryListenerPort = Integer.parseInt(registryListenerPort);
        this.registryRequesterPort = Integer.parseInt(registryRequesterPort);

        if (splitSizeString != null) {
            this.splitSize = Integer.parseInt(splitSizeString);
        }

        this.classpath = configMap.get(Config.LOAD_CLASSPATH);
        this.jvmHeapSizeStr = configMap.get(Config.JVM_HEAP_SIZE);

        if (jvmHeapSizeStr == null) {
            // Default value
            this.jvmHeapSizeStr = "2g";
        }

        String maxUsableMem = configMap.get(Config.MAX_USABLE_MEMORY);
        this.maxUsableMemory = convertMemoryStringToSizeInBytes(maxUsableMem);
        this.jvmHeapSizeInBytes =
                convertMemoryStringToSizeInBytes(jvmHeapSizeStr);
    }

    /**
     * 
     * @param memoryString - converts the given string representing memory units to its
     *            corresponding value in bytes
     *            
     * @return - value in bytes corresponding to the given memory string, Returns 0 if
     *         there was an error during conversion
     * 
     */
    private long convertMemoryStringToSizeInBytes(String memoryString) {
        long sizeInBytes = 0;

        try {

            if (memoryString != null) {
                int strLen = memoryString.length();
                char memoryUnit = memoryString.charAt(strLen - 1);
                sizeInBytes =
                        Long.parseLong(memoryString.substring(0, strLen - 1));

                if (memoryUnit == 'g') {
                    sizeInBytes = sizeInBytes * gb;
                } else if (memoryUnit == 'm') {
                    sizeInBytes = sizeInBytes * mb;
                }
            }
        } catch (Exception e) {
            // Ignore
        }
        return sizeInBytes;
    }

    /**
     * Returns the class path of the framework's installation. Used to start Registry and
     * start trackers
     * 
     * @return class path of framework as a string
     */
    public String getClasspath() {
        return classpath;
    }

    /**
     * Get the host name of the registry
     * 
     * @return String representing host name of the registry
     */
    public String getRegistryHostName() {
        return registryHostName;
    }

    /**
     * Get the listener port of the registry
     * 
     * @return listener port number of registry
     */
    public int getRegistryListenerPort() {
        return registryListenerPort;
    }

    /**
     * Get the requester port of the registry
     * 
     * @return requester port of the registry
     */
    public int getRegistryRequesterPort() {
        return registryRequesterPort;
    }

    /**
     * Get the split size to be used by MR framework
     * 
     * @return split size (returns default value of 64MB if not found in the configuration
     *         file)
     */
    public int getSplitSize() {
        return splitSize;
    }

    /**
     * Get the map containing all the configured attributes
     * 
     * @return configuration map
     */
    public Map<String, String> getConfigMap() {
        return configMap;
    }

}

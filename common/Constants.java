package mr.common;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Constants used by the MR framework.
 * 
 * 
 * 
 */
public class Constants {

    /**
     * 
     * Keys related to custom network protocol used by the framework.
     * 
     * @author Magesh Ramachandran
     * 
     */
    public static class NetworkProtocol {
        public static final String REQUEST = "request";
        public static final String REGISTER = "register";
        public static final String UNREGISTER = "unregister";
        public static final String PING = "ping";
        public static final String ACCEPT = "accept";
        public static final String FILE = "file";
        public static final String END = "end";
        public static final String SHUTDOWN = "shutdown";
    }

    /**
     * Error strings used by the framework.
     * 
     * @author Magesh Ramachandran
     * 
     */
    public static class Errors {
        public static final String INVALID_CONFIG_FORMAT =
                "ERROR: invalid config format";
    }

    /**
     * Collection of supported configuration attribute keys.
     * 
     * @author Magesh Ramachandran
     * 
     */
    public static class Config {
        public static final String SPLIT_SIZE = "split_size";
        
        public static final String REGISTRY_HOST_NAME = "registry_host_name";
        
        public static final String REGISTRY_LISTENER_PORT =
                "registry_listener_port";
        
        public static final String REGISTRY_REQUESTER_PORT =
                "registry_requester_port";
        
        public static final String MAX_USABLE_MEMORY = "max_usable_memory";
        
        public static final String JVM_HEAP_SIZE = "jvm_heap_size";
        
        public static final String LOAD_CLASSPATH = "load_classpath";
        
        public static final String NUMBER_OF_TASK_SLOTS =
                "number_of_task_slots";

        private static final String[] supportedAttributes = { SPLIT_SIZE,
                REGISTRY_HOST_NAME, REGISTRY_LISTENER_PORT,
                REGISTRY_REQUESTER_PORT, LOAD_CLASSPATH, MAX_USABLE_MEMORY,
                JVM_HEAP_SIZE, NUMBER_OF_TASK_SLOTS };
       
        public static final Set<String> SUPPORTED_ATTRIBUTES =
                new HashSet<String>(Arrays.asList(supportedAttributes));

    }

}

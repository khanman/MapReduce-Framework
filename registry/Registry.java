package mr.registry;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

import mr.common.CfgParser;
import mr.common.SystemSpecs;

/**
 * Custom registry service that allows worker nodes to register/unregister their
 * InetAddress. It also allows any requesting service to retrieve the addresses of all
 * currently registered nodes.
 * 
 * @author Magesh Ramachandran
 * @author Mansoor Ahmed Khan
 * 
 */
public class Registry {

    private Map<SocketAddress, SystemSpecs> workers =
            new HashMap<SocketAddress, SystemSpecs>();

    public static void main(String[] args) throws IOException {
        Registry registry = new Registry();
        registry.start();
    }

    /**
     * Starts the listener and the service threads which listen for incoming connections
     * for registrations and service registry requests respectively
     * 
     * @throws IOException when there is an error while connecting
     */
    private void start() throws IOException {
        CfgParser cfg = CfgParser.getInstance(null);
        RegistryListener incoming =
                new RegistryListener(workers, cfg.getRegistryListenerPort());
        RegistryService outgoing =
                new RegistryService(workers, cfg.getRegistryRequesterPort());
        Thread incomingThread = new Thread(incoming);
        Thread outgoingThread = new Thread(outgoing);
        incomingThread.start();
        outgoingThread.start();
    }
}

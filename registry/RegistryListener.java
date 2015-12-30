package mr.registry;

import static mr.common.Constants.NetworkProtocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Map;

import mr.common.SystemSpecs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static mr.common.MRUtility.sleep;

/**
 * Listener for registry. Waits for incoming connections and stores the ip address of the
 * registering machines along with their system specifications in a map which is shared
 * with the servicer thread
 * 
 * @author Magesh Ramachandran
 * @author Mansoor Ahmed Khan
 * 
 */
public class RegistryListener implements Runnable {

    private ServerSocket serverSocket;
    private Map<SocketAddress, SystemSpecs> clients;
    public static final Log LOG = LogFactory.getLog(RegistryListener.class);

    public RegistryListener(Map<SocketAddress, SystemSpecs> clients, int port)
            throws IOException {
        this.serverSocket = new ServerSocket(port);
        this.clients = clients;
    }

    /**
     * <pre>
     * Blocks until a worker node connects to the socket. Once the connection
     * is established, one of the following operation is performed
     * 
     * 1) Register - registers the connected node with the registry
     * 2) Unregister - unregisters the connected node from the registry
     * 3) Ping - prints the socket address of the connected node
     * </pre>
     */
    @Override
    public void run() {
        while (true) {
            try {
                System.out.println("Waiting for client on port "
                        + serverSocket.getLocalPort() + "...");
                Socket socket = serverSocket.accept();
                SocketAddress socketAddress = socket.getRemoteSocketAddress();

                System.out.println("Accepted connection with server");

                InputStream inputStream = socket.getInputStream();
                OutputStream outputStream = socket.getOutputStream();
                String mode = getMode(inputStream);

                if (NetworkProtocol.REGISTER.equals(mode)) {
                    register(inputStream, outputStream, socketAddress);
                } else if (NetworkProtocol.UNREGISTER.equals(mode)) {
                    unregister(socketAddress);
                } else if (NetworkProtocol.PING.equals(mode)) {
                    ping(socketAddress);
                }
                socket.close();

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                sleep(100);
                break;
            } catch (IOException e) {
                e.printStackTrace();
                sleep(100);
                break;
            }
        }
    }

    /**
     * Registers the given socket address with the registry
     * @param inputStream {@link InputStream}
     * @param outputStream {@link OutputStream}
     * @param socketAddress {@link SocketAddress}
     * 
     * @throws IOException when there is an error while reading from stream
     * @throws ClassNotFoundException if the class corresponding to the object read from
     *             stream is not available
     */
    private void register(
            InputStream inputStream,
            OutputStream outputStream,
            SocketAddress socketAddress)
            throws IOException,
            ClassNotFoundException {
        ObjectInputStream objectInputStream =
                new ObjectInputStream(inputStream);

        SystemSpecs workerSpecs = (SystemSpecs) objectInputStream.readObject();

        synchronized (clients) {
            if (!clients.containsKey(socketAddress)) {
                clients.put(socketAddress, workerSpecs);
            }
        }

        DataOutputStream dataOutStream = new DataOutputStream(outputStream);
        dataOutStream.writeUTF(NetworkProtocol.ACCEPT);
        dataOutStream.flush();
    }

    /**
     * Unregisters the given address from the registry. Once the given address is
     * unregistered, it will no longer be available for service.
     * 
     * @param socketAddress - {@link SocketAddress}
     */
    private void unregister(SocketAddress socketAddress) {
        System.out.println("Unregistering");
        synchronized (clients) {
            clients.remove(socketAddress);
        }
    }

    /**
     * Get the mode of operation from the stream. The mode is represented by a String.
     * 
     * @return the mode string
     * @throws IOException if there is an error reading from stream
     */
    private String getMode(InputStream inputStream) throws IOException {
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        String mode = dataInputStream.readUTF();

        return mode;
    }

    /**
     * Just prints the socket address to verify if a ping operation was successful. More
     * functionality might be added later
     * 
     * @param socketAddress {@link SocketAddress} of the pinging node
     */
    private void ping(SocketAddress socketAddress) {
        System.out.println("ping from " + socketAddress + "was successful");
    }

}

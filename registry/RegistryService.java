package mr.registry;

import static mr.common.Constants.NetworkProtocol;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Map;

import mr.common.SystemSpecs;
import static mr.common.MRUtility.sleep;

/**
 * Service provider for registry. Waits for incoming connections and serves the worker
 * details and specifications to the requester
 * 
 * @author Magesh Ramachandran
 * @author Mansoor Ahmed Khan
 * 
 */
public class RegistryService implements Runnable {
    private ServerSocket serverSocket;
    private Map<SocketAddress, SystemSpecs> workerAddressMap;

    public RegistryService(Map<SocketAddress, SystemSpecs> workers, int port)
            throws IOException {
        this.serverSocket = new ServerSocket(port);
        this.workerAddressMap = workers;
    }

    /**
     * <pre>
     * Blocks until a requesting node connects to server. Once a connection is
     * established, sends the worker details to the requesting node.
     * </pre>
     * 
     */
    @Override
    public void run() {
        while (true) {
            try {
                Socket socket = serverSocket.accept();
                socket.setSoTimeout(1000);

                InputStream inputStream = socket.getInputStream();
                OutputStream outputStream = socket.getOutputStream();
                String mode = getMode(inputStream);

                if (NetworkProtocol.REQUEST.equals(mode)) {
                    sendRegisteredNodeAddress(outputStream);
                } else {
                    System.out.println("Error: unsupported mode");
                }

            } catch (Exception e) {
                e.printStackTrace();
                sleep(1000);
            }
        }
    }

    /**
     * Get the mode of operation from the stream. The mode is represented by a String.
     * 
     * @return the mode string
     * @throws IOException when there is an error getting data from the input stream
     */
    private String getMode(InputStream inputStream) throws IOException {
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        String mode = dataInputStream.readUTF();

        return mode;
    }

    /**
     * Write the details of the nodes registered to the given output stream
     * 
     * @param outputStream {@link OutputStream}
     * @throws IOException when there is an error writing to the outputStream
     */
    private void sendRegisteredNodeAddress(OutputStream outputStream)
            throws IOException {
        System.out.println("about to send the info to app master");
        synchronized (workerAddressMap) {
            ObjectOutputStream out = new ObjectOutputStream(outputStream);
            out.writeObject(workerAddressMap);
            out.flush();
        }
    }

}

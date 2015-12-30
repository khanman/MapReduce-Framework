package mr.worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import static mr.common.Constants.NetworkProtocol;

/**
 * ShutdownThread is started by every TaskTracker, to listen for a 'shutdown'
 * request on the console input stream. When the shutdown request is received,
 * the shutdown process is initiated which attempts to shutdown the TaskTracker
 * gracefully by clearing the temp files and removing the temp folders
 * 
 * @author Magesh Ramachandran
 * @author Nikhil Mahesh
 * 
 */
public class ShutdownThread extends Thread {
    ServerSocket socket;
    InputStream in;

    /**
     * The socket connection of the TaskTracker and the Console input stream
     * are passed as arguments to the constructor
     * 
     * @param socket {@link ServerSocket} connecting with ApplicationMaster
     * @param in {@InputStream} from console, i.e, System.in
     */
    public ShutdownThread(ServerSocket socket, InputStream in) {
        this.socket = socket;
        this.in = in;
    }

    /**
     * Monitors the Console input for messages. If a 'shutdown' message is
     * received, closes the socket connection between the AppicationMaster and
     * the TaskTracker which launched this Shutdown Thread. The TaskTacker
     * contains logic to handle shutdown once Socket connection is closed.
     */
    @Override
    public void run() {
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line = null;
        try {
            while ((line = br.readLine()) != null) {
                if (NetworkProtocol.SHUTDOWN.equals(line)) {
                    System.out.println("Shutting down");
                    if (socket != null) {
                        socket.close();
                    }
                    break;
                } else {
                    System.out.println("Warning: invalid command");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

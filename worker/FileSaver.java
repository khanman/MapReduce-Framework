package mr.worker;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

import mr.common.Constants.NetworkProtocol;
import mr.common.MRUtility;

/**
 * File saver thread is started by the 'reducer' TaskTracker to facilitate
 * copying of the mapper output files to the reducer's file system. A new file
 * saver thread is started for each 'Map' TaskTracker
 * 
 * @author Magesh Ramachandran
 * @author Nikhil Mahesh
 * 
 */
public class FileSaver implements Runnable {

    private String tempDirectory;
    private Socket socket;

    public FileSaver(Socket socket, String tempDirectory) {
        this.socket = socket;
        this.tempDirectory = tempDirectory;
    }

    /**
     * Blocks till a file is received from the input stream. Any mode message
     * from the input stream other than 'NetworkProtocol.FILE' indicates that
     * all the files were transfered, thus completing the execution of the
     * thread.
     */
    @Override
    public void run() {
        File reducerRoot = new File(tempDirectory);
        if (!reducerRoot.isDirectory()) {
            reducerRoot.mkdir();
        }
        boolean hasMoreFiles = true;
        while (hasMoreFiles) {
            try {
                InputStream inputStream = socket.getInputStream();
                DataInputStream dataInputStream =
                        new DataInputStream(inputStream);
                String mode = dataInputStream.readUTF();
                if (NetworkProtocol.FILE.equals(mode)) {
                    MRUtility.receiveFile(tempDirectory, dataInputStream);
                } else {
                    // If the code reaches here, means that all map output files
                    // from a particular map node were transfered
                    hasMoreFiles = false;
                    System.out.println("All files from "
                            + socket.getRemoteSocketAddress()
                            + " mapper were received");
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

}

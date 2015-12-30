package mr.worker;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import mr.common.Constants.NetworkProtocol;
import mr.common.MRUtility;

/**
 * Shuffler thread sends the map output files to the correct reducer based on
 * the reducer id from the map output file name
 * 
 * 
 */
public class Shuffler implements Runnable {

    private SocketAddress[] reducerAddresses;
    private Socket[] reducers;
    private String dataDir;
    private boolean hasMoreFiles = true;
    private boolean shouldEnd = false;

    /**
     * Used to mark the end of the shuffle phase. Setting shouldEnd to true
     * ensures that the Shuffler thread will exit after finishing the current
     * file read/transfer iteration
     * 
     * @param shouldEnd - true if the shuffle phase must come to an end after
     *            the current iteration, false otherwise
     */
    public void setShouldEnd(boolean shouldEnd) {
        this.shouldEnd = shouldEnd;
    }

    /**
     * Constructor
     * 
     * @param reducers array of {@link SockerAddress} of the nodes handling the
     *            reduce tasks
     * @param dataDir - folder in which the temporary map output files are
     *            stored
     */
    public Shuffler(SocketAddress[] reducers, String dataDir) {
        this.reducerAddresses = reducers;
        this.dataDir = dataDir;
        this.reducers = new Socket[reducers.length];

    }

    /**
     * Obtains a network connection to each node handling the reducer task
     */
    private void obtainConnectionToReducers() {
        for (int i = 0; i < reducerAddresses.length; i++) {
            @SuppressWarnings("resource")
            Socket socket = new Socket();
            try {
                socket.connect(reducerAddresses[i]);
                reducers[i] = socket;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * <p>
     * scans the mapper output directory for files. Completed files are then
     * sent to the corresponding reducer till an 'end of shuffle' message is
     * received, the files are then deleted from map output dir.
     * <p>
     * The completed map output files are in the format completeX_Y_Z, where X
     * is the mapper task id, Y is the spill number for the current task, Z is
     * the reducer task tracker logical id to which the file must be
     * transferred
     */
    @Override
    public void run() {
        obtainConnectionToReducers();
        File dir = new File(dataDir);
        List<String> fileQueue = new ArrayList<String>();
        while (hasMoreFiles) {
            fillCompletedFileQueue(fileQueue, dir);
            while (!fileQueue.isEmpty()) {
                transferFilesToReducer(fileQueue);
            }
            MRUtility.sleep(50);
        }
        sendEndOfShuffleMessageToAllReducers();
    }

    /**
     * Fills the given fileQueue with a list of 'completed' files in the given
     * folder
     * 
     * @param fileQueue {@link List<String>} an empty list
     * @param dir {@link File} representing the directory to be scanned
     */
    private void fillCompletedFileQueue(List<String> fileQueue, File dir) {
        String[] allFiles = dir.list();
        fileQueue.size();
        if (shouldEnd) {
            hasMoreFiles = false;
        }
        for (String fileName : allFiles) {
            if (fileName.contains("complete")) {
                fileQueue.add(fileName);
            }
        }
    }

    /**
     * Transfers all the files in the fileQueue to the corresponding reducer,
     * which is identified from the file name. Once a file is transferred, the
     * file name is removed from the given queue
     * 
     * @param fileQueue {@link List<String>}
     */
    private void transferFilesToReducer(List<String> fileQueue) {
        String fileName = fileQueue.remove(0);
        int reducerId = getReducerIdFromFileName(fileName);
        Socket socket = reducers[reducerId];
        System.out.println("about to tranfer file" + fileName + "to reducer "
                + reducerId);
        try {
            File file = new File(dataDir, fileName);
            OutputStream outputStream = socket.getOutputStream();
            writeTransferModeToStream(outputStream, NetworkProtocol.FILE);
            MRUtility.sendFile(file.getAbsolutePath(), outputStream);
            System.out.println("reducer file sent");
            file.delete();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * Send 'end of shuffle' message to all the reducers to indicate that the
     * shuffle phase associated with the particular mapper has ended
     */
    private void sendEndOfShuffleMessageToAllReducers() {
        for (Socket socket : reducers) {
            OutputStream outputStream;
            try {
                outputStream = socket.getOutputStream();
                writeTransferModeToStream(outputStream, NetworkProtocol.END);

                outputStream.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Get the reducer id from the given file name
     * 
     * @param fileName - file name of format completeX_Y_Z, where Z is the
     *            reducer id
     * @return the logical reducer id
     */
    private int getReducerIdFromFileName(String fileName) {
        System.out.println("file name in getReducerIdFromFileName " + fileName);
        int reducerId =
                Integer.parseInt(fileName.substring(
                        fileName.lastIndexOf("_") + 1, fileName.length()));
        return reducerId;
    }

    /**
     * Write the transfer mode string to the given OutputStream
     * 
     * @param outputStream {@link OutputStream}
     * @param mode - transfer mode indicating the type of data that follows
     *            this message in the given OutputStream
     * @throws IOException
     */
    private void writeTransferModeToStream(
            OutputStream outputStream,
            String mode) throws IOException {
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        dataOutputStream.writeUTF(mode);
        dataOutputStream.flush();
    }

}

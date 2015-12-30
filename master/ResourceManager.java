package mr.master;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import mr.common.CfgParser;
import mr.common.CircularList;
import mr.common.Configuration;
import mr.common.Constants.NetworkProtocol;
import mr.common.MRUtility;
import mr.common.SystemSpecs;
import mr.common.Task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <pre>
 * Manages the life cycle of a map-reduce job. ResourceManager performs the
 * following operations
 * 
 * 1) Connects to registry to obtain TaskTracker node details
 * 2) Establishes connection to all TaskTrackers
 * 3) Allocates reduce task to the TaskTracker nodes
 * 4) Reads the input file and sends the file splits to the mapper nodes
 * 5) Creates a listener thread to monitor task completion and ensure completion of all 
 *    tasks before exiting.
 * </pre>
 * 
 * @author Magesh Ramachandran
 * @author Mansoor Ahmed Khan
 * 
 */
public class ResourceManager {

    private Configuration configuration;
    private String inputFilePath;
    private CfgParser cfg;
    private static int fileSplitId;
    private String inputFileName;
    private PriorityQueue<Task> taskQueue = new PriorityQueue<Task>();
    private SocketAddress[] assignedReducers;
    public static final Log LOG = LogFactory.getLog(ResourceManager.class);
    private File jarFile;

    private Socket[] workers;
    private CircularList<Socket> circularListOfWorkers =
            new CircularList<Socket>();

    /**
     * Constructor, parses the config.txt file that contain runtime parameters
     * 
     * @param inputPath - path of the input file
     * @param jarFile {@link File} the jar file used for execution
     * @param config {@link Configuration}
     */
    public ResourceManager(final String inputPath, final File jarFile,
            final Configuration config) {
        this.cfg = CfgParser.getInstance(null);
        this.inputFilePath = inputPath;
        File inputFile = new File(inputFilePath);
        this.inputFileName = inputFile.getName();
        this.configuration = config;
        this.configuration.setSplitSize(cfg.getSplitSize());
        this.jarFile = jarFile;

    }

    /**
     * <pre>
     * Executes the following tasks in order
     * 
     * 1) Retrieves the worker node addresses from registry
     * 2) Allocates tasks to Nodes
     * 3) Wait till all the tasks have been completed
     * </pre>
     */
    public boolean start() {
        try {
            getNodeAddressFromRegistry();
            allocateTasksToNodes();
            listenForUpdatesTillCompletion();
            return true;
        } catch (Throwable throwable) {
            LOG.fatal("Error while processing, map-reduce failed", throwable);
            return false;
        }
    }

    /**
     * Get the ip addresses of worker nodes from registry and establish a connection with
     * each of the nodes. The connections are persisted in state and are used for
     * data/file transfers.
     */
    @SuppressWarnings("unchecked")
    private void getNodeAddressFromRegistry() {
        try {
            // Get the ip addresses of the worker nodes from registry
            Socket socket =
                    new Socket(cfg.getRegistryHostName(),
                            cfg.getRegistryRequesterPort());

            OutputStream outputStream = socket.getOutputStream();
            MRUtility.writeMessageToStream(outputStream,
                    NetworkProtocol.REQUEST);

            InputStream inFromServer = socket.getInputStream();
            ObjectInputStream objectInputStream =
                    new ObjectInputStream(inFromServer);

            Map<SocketAddress, SystemSpecs> workerAddressMap =
                    (Map<SocketAddress, SystemSpecs>) objectInputStream
                            .readObject();

            adjustNumberOfReducers(workerAddressMap.size());
            socket.close();

            LOG.debug("Retrieved node addresses from registry");
            LOG.debug(workerAddressMap);

            establishConnectionWithWorkerNodes(workerAddressMap);

        } catch (Exception e) {
            LOG.fatal("Exception while getting data from registry", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * <pre>
     * Allocates 3 types of tasks to all nodes in sequence
     *  
     * 1) Allocates reduce tasks to worker nodes and stores the list of allocated reducers in state
     * 2) Allocates map tasks to all unassigned nodes in a round robin manner.
     * 3) Allocates one end task
     * </pre>
     */
    private void allocateTasksToNodes() {
        LOG.debug("Before allocating reduce tasks");
        allocateReduceTasks();
        LOG.debug("Before allocating map tasks");
        allocateMapTasksWithFileSplits();
        LOG.debug("Before allocating end tasks");
        allocateEndTasks();
    }

    /**
     * Establishes a connection with each of the nodes in the given map. The connections
     * are persisted in state and are used for data/file transfers.
     * 
     * @param workerAddressMap: map of SocketAddress of each TaskTracker, with
     *            {@link SystemSpecs} as value (SystemSpecs is not used by the current
     *            implementation)
     * 
     * 
     */
    private void establishConnectionWithWorkerNodes(
            Map<SocketAddress, SystemSpecs> workerAddressMap) {
        int count = 0;
        workers = new Socket[workerAddressMap.size()];

        for (SocketAddress socketAddress : workerAddressMap.keySet()) {
            Socket socket = new Socket();
            try {
                socket.connect(socketAddress);
                OutputStream outputStream = socket.getOutputStream();

                // Transfer jar file to worker nodes
                // todo remove hard 0code
                MRUtility.sendFile(jarFile.getAbsolutePath(), outputStream);

                ObjectOutputStream objectOutputStream =
                        new ObjectOutputStream(outputStream);
                objectOutputStream.writeObject(configuration);
                objectOutputStream.flush();
                workers[count++] = socket;
                circularListOfWorkers.add(socket);

                LOG.debug("Connection to " + socketAddress + " was successful");
            } catch (IOException e) {
                LOG.error("An exception has occured while connecting", e);
            }
        }
    }

    /**
     * Adjusts the number of reducer slots (if required) such that it is always less than
     * total number of worker slots available
     * 
     * @param numberOfNodesAvailable - total number of worker nodes available
     */
    private void adjustNumberOfReducers(int numberOfNodesAvailable) {
        int initalReducerCount = configuration.getNumberOfReducers();
        if (numberOfNodesAvailable < 2) {
            throw new RuntimeException("Error: not enough worker slots!");
        }
        int updatedReducerCount =
                Math.min(initalReducerCount, numberOfNodesAvailable - 1);
        configuration.setNumberOfReducers(updatedReducerCount);
        LOG.debug("reducer count after adjustment" + updatedReducerCount);
    }

    /**
     * Splits the input file into chunks based on split size and transfers it to worker
     * nodes in a round-robin manner along with the corresponding map task
     */
    private void allocateMapTasksWithFileSplits() {
        FileSplitter fileSplitter =
                new FileSplitter(inputFilePath, cfg.getSplitSize());
        while (fileSplitter.hasMoreSplits()) {
            try {
                OutputStream outputStream =
                        circularListOfWorkers.next().getOutputStream();

                Task task = new Task(Task.TYPE.MAP, "m" + fileSplitId);
                // set of reducers are sent to each mappers to facilitate
                // shuffle
                task.setReducers(assignedReducers);
                writeObjectToStream(outputStream, task);
                taskQueue.add(task);
                byte[] splitFileData = fileSplitter.getNextSplit();
                byte[] compressedSplitFileData = compressData(splitFileData);
                writeFileSplitToStream(outputStream, compressedSplitFileData);
                LOG.debug("Split data length " + splitFileData.length);
            } catch (IOException e) {
                LOG.fatal("Error while transferring file splits", e);
                throw new RuntimeException(e);
            }
        }
    }

    //

    /**
     * Compresses the given byte[] data and returns the compressed data
     * 
     * @param data : data to be compressed as a byte array
     * @return compressed byte[] data
     */
    private byte[] compressData(byte[] data) {
        try {
            ByteArrayOutputStream byteArrayOutputStream =
                    new ByteArrayOutputStream(data.length);

            Deflater def = new Deflater(Deflater.BEST_SPEED);
            DeflaterOutputStream zipStream =
                    new DeflaterOutputStream(byteArrayOutputStream, def,
                            data.length);
            zipStream.write(data);
            zipStream.close();
            byte[] compressedData = byteArrayOutputStream.toByteArray();
            return compressedData;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.fatal("File compression failed", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Allocates first n worker nodes with reduce tasks where n is the number of reducers
     */
    private void allocateReduceTasks() {
        int numberOfMapHosts =
                workers.length - configuration.getNumberOfReducers();

        int numberOfReducers = configuration.getNumberOfReducers();
        assignedReducers = new SocketAddress[numberOfReducers];
        for (int i = 0; i < numberOfReducers; i++) {
            try {
                circularListOfWorkers.remove(workers[i]);
                OutputStream outputStream = workers[i].getOutputStream();
                Task task = new Task(Task.TYPE.REDUCE, "r" + i);
                // Used by TaskTracker to receive files from all the mapper
                // nodes
                task.setNumberOfMapHosts(numberOfMapHosts);
                writeObjectToStream(outputStream, task);
                taskQueue.add(task);
                SocketAddress socketAddress =
                        workers[i].getRemoteSocketAddress();

                assignedReducers[i] = socketAddress;
            } catch (IOException e) {
                LOG.fatal("Error while allocating task to a reducer", e);
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * Marks the end of task execution. The END tasks are used to signal the
     * {@link TaskTrackers} that there are no more tasks to be assigned
     */
    private void allocateEndTasks() {
        for (int i = 0; i < workers.length; i++) {
            try {
                OutputStream outputStream;
                outputStream = workers[i].getOutputStream();
                ObjectOutputStream objectOutputStream =
                        new ObjectOutputStream(outputStream);
                Task t = new Task(Task.TYPE.END, "e" + i);
                objectOutputStream.writeObject(t);
                objectOutputStream.flush();

            } catch (IOException e) {
                LOG.fatal("Error while allocating end task", e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Helper method to write the split file data to the output steam.
     * 
     * @param outputStream {@link OutputStream}
     * @param splitFileData byte[] of the entire file split
     * 
     * @throws IOException when error writing to output stream
     */
    private void writeFileSplitToStream(
            OutputStream outputStream,
            byte[] splitFileData) throws IOException {
        DataOutputStream dataOutStream = new DataOutputStream(outputStream);
        // Write file name
        String fileName = inputFileName + '_' + fileSplitId;
        dataOutStream.writeUTF(fileName);
        dataOutStream.flush();

        // Write file length
        dataOutStream.writeLong(splitFileData.length);
        dataOutStream.flush();

        // Write split data
        dataOutStream.write(splitFileData, 0, splitFileData.length);
        dataOutStream.flush();
        fileSplitId++;
    }

    /**
     * Writes the given object to the given output stream
     * 
     * @param outputStream {@link OutputStream}
     * @param object {@link Object}
     * @throws IOException when there is error writing to the stream
     */
    private void writeObjectToStream(OutputStream outputStream, Object object)
            throws IOException {
        ObjectOutputStream objectOutputStream =
                new ObjectOutputStream(outputStream);
        objectOutputStream.writeObject(object);
        objectOutputStream.flush();
    }

    /**
     * Waits for all map and reduce tasks to get completed
     */
    private void listenForUpdatesTillCompletion() {
        LOG.debug("Inside listenForUpdatesTillCompletion");

        Thread[] taskUpdaterThreads = new Thread[workers.length];
        launchTaskMonitorThreads(taskUpdaterThreads);

        LOG.debug("Started monitoring threads");
        for (int i = 0; i < taskUpdaterThreads.length; i++) {
            try {
                taskUpdaterThreads[i].join();
                System.out.println(taskUpdaterThreads[i].getName()
                        + " thread joined");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("all done!");
    }

    /**
     * Launches a monitor thread for each TaskTracker. The task updater thread tracks the
     * task completion status of each task.
     * 
     * @see TaskUpdater
     * 
     * @param allWorkers: empty array of thread which will be filled in this method
     */
    private void launchTaskMonitorThreads(Thread[] taskUpdaterThread) {
        for (int i = 0; i < workers.length; i++) {
            TaskCompletionMonitor taskMonitor =
                    new TaskCompletionMonitor(workers[i], taskQueue);
            taskUpdaterThread[i] =
                    new Thread(taskMonitor, workers[i].getRemoteSocketAddress()
                            .toString());
            taskUpdaterThread[i].start();
        }
    }
}

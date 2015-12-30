package mr.worker;

import static mr.common.MRUtility.sleep;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import mr.common.CfgParser;
import mr.common.Configuration;
import mr.common.Constants.NetworkProtocol;
import mr.common.MRUtility;
import mr.common.SystemSpecs;
import mr.common.Task;

/**
 * 
 * <pre>
 * Executes the following steps in sequence
 * Initialization phase:
 * 1) Launches the Shutdown monitor thread     * 
 * 2) Registers itself with the registry by submitting the system specification to the registry  
 * 
 * Execution phase:
 * 1) Waits for an ApplicationMaster to connect
 * 2) Processes Map/Reduce tasks till completion
 * 3) After tasks completion, cleans up the resources, files used by the tasks
 * 4) repeats step1- step 3 until shutdown request is received
 * 
 * Shutdown Phase:
 * 1) Unregisters itself from the registry
 * 2) Deletes the root folders created during initialization
 * 
 * </pre>
 * 
 * 
 * 
 */
@SuppressWarnings({ "rawtypes" })
public class TaskTracker {

    private String registryHost;
    private int registryPort;
    private Configuration configuration;
    private PriorityQueue<Task> taskExecutionQueue = new PriorityQueue<Task>();
    private PriorityQueue<Task> completedTaskQueue = new PriorityQueue<Task>();
    private Mapper mapper;
    private Reducer reducer;
    private Mapper.Context context;
    Thread taskUpdaterThread;
    Thread taskListenerThread;
    Thread fileTransferThread;
    private boolean hasFileTransferThreadStarted = false;
    private String mtemp_dir;
    private String rtemp_dir;
    private String jartemp_dir;
    private Shuffler shuffler;
    private ServerSocket serverSocket;
    private Socket socket;
    private URLClassLoader classLoader;

    /**
     * Starts the TaskTracker
     * @param args
     */
    public static void main(String[] args) {
        TaskTracker taskTracker = new TaskTracker();
        taskTracker.start();
    }

    /**
     * @see TaskTracker
     */
    public void start() {

        launchShutdownMonitor();
        register();

        try {
            executeTaskTrackerLifeCycle();
        } catch (Throwable e) {
            e.printStackTrace();
            unregister();
        }
    }

    /**
     * Execution phase of the TaskTracker. Performs the following actions in sequence 1)
     * Waits for the Application master to connect 2) Sets the Mapper and Reducer objects
     * 3) Starts a thread to listen for tasks 4) Starts a thread to update completion of
     * the tasks assigned by Application master 5) Executes all the tasks that are in the
     * task queue.
     * 
     * @throws InterruptedException when one of the thread is interrupted
     */
    private void executeTaskTrackerLifeCycle() throws InterruptedException {

        while (true) {
            waitForMaster();
            setupMapperAndReducerObjects();
            startTaskListenerThread();
            startTaskUpdaterThread();
            executeTask();
            taskUpdaterThread.join();
            taskListenerThread.join();

            if (hasFileTransferThreadStarted) {
                fileTransferThread.join();
                hasFileTransferThreadStarted = false;
                shuffler = null;
            }

            cleanup();
            System.out.println("Task execution complete!");
        }
    }

    /**
     * Launches a thread to shutdown the Task tracker. When "shutdown" input is sent to
     * System.in, the shutdown process is initiated
     */
    private void launchShutdownMonitor() {

        try {
            serverSocket = new ServerSocket();
        } catch (IOException e) {
            throw new RuntimeException("Error while creating server socket", e);
        }

        ShutdownThread shutdownMonitor =
                new ShutdownThread(serverSocket, System.in);
        shutdownMonitor.start();
    }

    /**
     * Unregisters from the registry. Invoked on "shutdown"
     */
    private void unregister() {

        try {
            Socket rSocket = new Socket(registryHost, registryPort);

            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }

            OutputStream outputStream = rSocket.getOutputStream();
            MRUtility.writeMessageToStream(outputStream,
                    NetworkProtocol.UNREGISTER);

            rSocket.close();

        } catch (IOException e) {
            System.out.println("Cannot connect to Registry");
        }
    }

    /**
     * <pre>
     * 1) Listens to the inputStream from the {@link ApplicationMaster} for any task.
     * 2) Updates the task execution queue with the tasks read from stream. 
     * 3) Saves the file splits to the temp directory (for map tasks)
     * </pre>
     * 
     * @see TaskListener
     */
    private void startTaskListenerThread() {
        TaskListener taskListener =
                new TaskListener(socket, taskExecutionQueue, mtemp_dir);
        taskListenerThread = new Thread(taskListener);
        taskListenerThread.start();
    }

    /**
     * Updates the ApplicationMaster by sending the completed tasks. Reads and removes
     * tasks from the completed task queue.
     * 
     * @see StatusUpdater
     */
    private void startTaskUpdaterThread() {
        StatusUpdater statusUpdater =
                new StatusUpdater(socket, completedTaskQueue);
        taskUpdaterThread = new Thread(statusUpdater);
        taskUpdaterThread.start();
    }

    /**
     * Gets the next task by priority from the task queue and executes it. If there are no
     * tasks in the queue, sleeps for 100 milliseconds and checks the tasks queue for
     * tasks. The method ends when an END task is fetched and executed.
     * 
     */
    private void executeTask() {
        boolean hasMoreTasks = true;

        while (hasMoreTasks) {
            Task currentTask = getNextTaskFromQueue();
            if (currentTask != null) {
                if (currentTask.getType() == Task.TYPE.MAP) {

                    executeMapTask(currentTask);

                } else if (currentTask.getType() == Task.TYPE.REDUCE) {

                    executeReduceTask(currentTask);

                } else {
                    executeCompleteTask(currentTask);
                    hasMoreTasks = false;
                    continue;
                }
            } else {
                sleep(100);
            }
        }
        System.out.println("Execute task has ended");
    }

    /**
     * Fetches and removes the next task from the task queue by priority
     * 
     * @return {@link Task}
     */
    private Task getNextTaskFromQueue() {
        Task nextTask = null;

        synchronized (taskExecutionQueue) {
            if (!taskExecutionQueue.isEmpty()) {
                System.out.println("taskExecutionQueue" + taskExecutionQueue);
                nextTask = taskExecutionQueue.poll();
            }
        }

        return nextTask;
    }

    /**
     * Executes the given END task by adding it to the completed task queue and sets the
     * "shouldEnd" field of shuffler to true, so that the Shuffler thread can finish
     * executing after transferring the existing map output files.
     * 
     * @param task {@link Task} of Task.TYPE END     * 
     */
    private void executeCompleteTask(Task task) {
        completedTaskQueue.add(task);
        if (hasFileTransferThreadStarted) {
            if (shuffler != null) {
                shuffler.setShouldEnd(true);
            }
        }
    }

    /**
     * Execute the reduce task if there is at least one mapper output file. Once the task
     * has been executed, adds it to the completedTask queue
     * 
     * @param currentTask {@link Task} of Task.TYPE REDUCE
     */
    @SuppressWarnings("unchecked")
    private void executeReduceTask(Task currentTask) {
        boolean hasAtleastOneFile = getFilesFromMapper(currentTask);

        if (hasAtleastOneFile) {
            Reducer.Context reduceContext =
                    reducer.new Context(configuration, rtemp_dir,
                            currentTask.getTaskId());

            reducer.run(reduceContext);
        } else {
            System.out.println("Reduce was not executed in this node");
        }

        System.out.println("Finished reduce task");
        completedTaskQueue.add(currentTask);
    }

    /**
     * Executes a map task. If the Shuffler thread was not started before, launches a new
     * shuffler thread. Once the task has been executed, adds it to the completedTask
     * queue
     * 
     * @param currentTask {@link Task} of Task.TYPE MAP
     */
    @SuppressWarnings("unchecked")
    private void executeMapTask(Task currentTask) {
        context =
                mapper.new Context(configuration,
                        currentTask.getSplitFileForTask(), mtemp_dir);

        mapper.run(context);

        if (!hasFileTransferThreadStarted) {
            // Shuffler thread is responsible for transferring the map output files to the
            // appropriate reducer
            shuffler = new Shuffler(currentTask.getReducers(), mtemp_dir);
            fileTransferThread = new Thread(shuffler);
            fileTransferThread.start();
            hasFileTransferThreadStarted = true;
        }

        synchronized (completedTaskQueue) {
            completedTaskQueue.add(currentTask);
        }
    }

    /**
     * This method is executed only when the TaskTracker is executing a reduce task.
     * Establishes connection to every map TaskTracker to receive the file map output
     * files. Ends after all the map output files have been transfered to this
     * TaskTrackers temp folder.
     * 
     * @param currentTask {@link Task}
     * @return true if atleast one map file was received, otherwise false
     */
    private boolean getFilesFromMapper(Task currentTask) {
        int numberOfMapHosts = currentTask.getNumberOfMapHosts();

        // Establishes connection to all the map TaskTrackers and launches file saver
        // threads
        Map<Socket, Thread> connectionToMapper =
                saveMapTaskOutput(numberOfMapHosts);

        // Waits for all file saver threads to finish.
        for (Map.Entry<Socket, Thread> entry : connectionToMapper.entrySet()) {
            try {
                entry.getValue().join();
                entry.getKey().close();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return checkIfAnyMapperFilesWereWritten();

    }

    /**
     * Checks if any map output file was received by the reducer.
     * 
     * @return true if at least one file was transferred, otherwise false
     */
    private boolean checkIfAnyMapperFilesWereWritten() {
        File file = new File(rtemp_dir);
        if (file.list().length == 0) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * This method is executed only when the TaskTracker is executing a reduce task.
     * Establishes connection with all TaskTracker running map task and launches a
     * FileSaver thread listening for files on all connections.
     * 
     * @param numberOfMapHosts: number of TaskTrackers running the map task
     * @return {@link Map} of mapper's {@link Socket} and the corresponding FileSaver
     *         thread as value
     */
    private Map<Socket, Thread> saveMapTaskOutput(int numberOfMapHosts) {
        Map<Socket, Thread> connectionsToMapper = new HashMap<Socket, Thread>();

        for (int i = 0; i < numberOfMapHosts; i++) {
            System.out.println("Getting file from mapper");

            try {
                Socket socket = serverSocket.accept();
                FileSaver fileSaver = new FileSaver(socket, rtemp_dir);
                Thread thread = new Thread(fileSaver);
                thread.start();
                connectionsToMapper.put(socket, thread);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return connectionsToMapper;
    }

    /**
     * Get the System specifications of the system running the TaskTracker. This
     * information is sent to the registry, and accessed by the ApplicationMaster
     * 
     * @return {@link SystemSpec}
     */
    private SystemSpecs getSystemSpecs() {
        SystemSpecs systemSpecs = new SystemSpecs();
        Runtime runtime = Runtime.getRuntime();

        int availableCpuCores = runtime.availableProcessors();
        long maxMemoryAvailable = runtime.maxMemory();

        systemSpecs.setMaxMemoryAvailable(maxMemoryAvailable);
        systemSpecs.setAvailableCpuCores(availableCpuCores);
        return systemSpecs;
    }

    /**
     * Uses {@link URLClassLoader} to load the Mapper and Reducer class. Once the classes
     * are loaded, creates instances of the mapper and reducer objects, which are stored
     * in the instance variables of this TaskTracker *
     * 
     */
    private void setupMapperAndReducerObjects() {
        String mapperClassStr = configuration.getMapperClass();
        String reducerClassStr = configuration.getReducerClass();

        try {
            mapper =
                    (Mapper) Class.forName(mapperClassStr, true, classLoader)
                            .newInstance();
            reducer =
                    (Reducer) Class.forName(reducerClassStr, true, classLoader)
                            .newInstance();
        } catch (Exception e) {
            // LOG.fatal("Error while setting up mapper and reducer object", e);
            throw new RuntimeException(e);
        }

    }

    /**
     * Waits for an ApplicationMaster to establish a connection. Once the connection is
     * established, receives the jar file and the configuration object from the
     * InputStream.
     * 
     */
    private void waitForMaster() {

        try {
            System.out.println("Waiting for master");

            socket = serverSocket.accept();
            System.out.println("Connected with master");

            InputStream inputStream = socket.getInputStream();
            DataInputStream dataInputStream = new DataInputStream(inputStream);

            File jarFile = MRUtility.receiveFile(jartemp_dir, dataInputStream);
            System.out.println("Jar file received");

            buildClassLoaderFromJar(jarFile);
            getConfigurationFromInputStream(inputStream);

        } catch (SocketException e) {
            // when a shutdown message is received, SocketException is thrown
            // and the shutdown process is initiated
            System.out.println("Shutting down!");
            shutdown();
        } catch (IOException e) {
            // LOG.fatal("Error while reading data from input stream", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the {@link Configuration} object from the input stream.
     * 
     * @param inputStream {@link InputStream}
     * 
     * @throws IOException when there is an error reading from the input stream
     */
    private void getConfigurationFromInputStream(InputStream inputStream)
            throws IOException {
        try {
            ObjectInputStream objectInputStream =
                    new ObjectInputStream(inputStream);
            configuration = (Configuration) objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * <pre>
     * Shuts down this TaskTracker instance by performing the following actions
     * 1) Unregisters this TaskTracker from the registry
     * 2) Cleans up the temporary files written by mappers and reducers
     * 3) Delete temporary folders created by this TaskTracker for saving the intermediate files
     * </pre>
     */
    private void shutdown() {
        unregister();
        cleanup();
        deleteTempFolders();

        System.out.println("Unregistered successfully!");
        System.out.println("Exiting");

        System.exit(0);
    }

    /**
     * Creates the temporary folders unique to this TaskTracker which is subsequently used
     * for saving the intermediate files
     * 
     * @param ipAddress - ipAddress along with port of this task tracker
     */
    private void setupTempFolders(String ipAddress) {
        String nodeId = ipAddress.replace(".", "").replace(":", "~");
        this.mtemp_dir = "mtemp_dir" + File.separator + nodeId;
        this.rtemp_dir = "rtemp_dir" + File.separator + nodeId;
        this.jartemp_dir = "jartemp_dir" + File.separator + nodeId;
        createNewDirIfNotFound(mtemp_dir);
        createNewDirIfNotFound(rtemp_dir);
        createNewDirIfNotFound(jartemp_dir);
    }

    /**
     * Utility method to create a new directory if there is none
     * 
     * @param dataDir - directory to be created
     */
    private void createNewDirIfNotFound(String dataDir) {
        File dir = new File(dataDir);

        if (!dir.isDirectory()) {
            dir.mkdirs();
        }
    }

    /**
     * Creates an instance of {@link URLClassLoader} for the client jar file transfered by
     * the ApplicationMaster. The classLoader instance is used for loading the Mapper and
     * Reducer classes
     * 
     * @param file - {@link File} the client jar file
     */
    private void buildClassLoaderFromJar(File file) {
        try {
            URL[] url = new URL[] { file.toURI().toURL() };
            classLoader =
                    new URLClassLoader(url, this.getClass().getClassLoader());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Registers this TaskTracker with the Registry. After a connection with the registry
     * has been established successfully, the method then creates an unique temp directory
     * needed for executing the mapper and reducer tasks
     *  
     */
    private void register() {
        SystemSpecs systemSpecs = getSystemSpecs();
        try {
            CfgParser cfg = CfgParser.getInstance(null);
            this.registryHost = cfg.getRegistryHostName();
            this.registryPort = cfg.getRegistryListenerPort();
            System.out.println("Registry " + registryHost + " Port "
                    + registryPort);

            Socket registrySocket = new Socket(registryHost, registryPort);
            serverSocket.bind(registrySocket.getLocalSocketAddress());

            setupTempFolders(registrySocket.getLocalSocketAddress().toString());

            OutputStream outputStream = registrySocket.getOutputStream();
            InputStream inputStream = registrySocket.getInputStream();

            MRUtility.writeMessageToStream(outputStream,
                    NetworkProtocol.REGISTER);
            MRUtility.writeObjectToStream(outputStream, systemSpecs);

            if (NetworkProtocol.ACCEPT.equals(MRUtility
                    .getMessageFromStream(inputStream))) {
                System.out.println("Connection accepted");
            }

            registrySocket.close();

        } catch (Exception e1) {
            System.out.println("Cannot connect to Registry");
            throw new RuntimeException(e1);
        }
    }

    /**
     * Cleans up the temporary files. Commented for testing
     */
    private void cleanup() {
        deleteAllFilesInDir(mtemp_dir);
        deleteAllFilesInDir(rtemp_dir);
    }

    /**
     * Deletes the temporary folders created by this TaskTracker
     */
    private void deleteTempFolders() {
        deleteDirIfFound(mtemp_dir);
        deleteDirIfFound(rtemp_dir);
    }

    /**
     * Utility method to delete the given directory if present
     * 
     * @param dataDir - directory to be deleted
     */
    private void deleteDirIfFound(String dataDir) {
        File dir = new File(dataDir);
        if (dir.isDirectory()) {
            dir.delete();
        }
    }

    /**
     * Delete all files in the given directory
     * 
     * @param dataDir - a file directory
     */
    private void deleteAllFilesInDir(String dataDir) {
        File dir = new File(dataDir);
        String[] listOfFiles = dir.list();
        for (String fileName : listOfFiles) {
            File file = new File(dir, fileName);
            file.delete();
        }
    }
}

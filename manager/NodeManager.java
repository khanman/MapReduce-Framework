package mr.manager;

import static mr.common.Constants.NetworkProtocol.PING;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;

import mr.common.CfgParser;
import mr.common.MRUtility;

/**
 * Starts N instances of TaskTracker and an instance of Registry(if applicable) Uses
 * settings from Configuration file to start tasks. Allows graceful shutdown of
 * TaskTracker instances.
 * 
 * 
 */
public class NodeManager {

    private static int numberOfWorkers = 0;
    private static String javaHome;
    private static final String registryClass = "mr.registry.Registry";
    private static final String taskTrackerClass = "mr.worker.TaskTracker";
    private static final String classPathCommand = "-cp";
    private static final String SHUTDOWN_KEY = "shutdown";
    private static Process registryProcess;
    private static CfgParser cfg;
    private static String hostName;
    private static int port;
    private static PrintWriter[] outWriters;

    /**
     * Starts the registry and task trackers using parameters specified in the
     * configuration file
     * 
     * @param args - ignored by the program
     * 
     * @throws IOException when error connecting to registry
     */
    public static void main(String[] args) throws IOException {

        javaHome =
                System.getProperty("java.home") + File.separator + "bin"
                        + File.separator + "java";
        cfg = CfgParser.getInstance(null);
        allocateNumberOfTaskTrackers();
        startRegistry();
        startTaskTrackers();
        shutdownRegistryOnExit();
    }

    /**
     * Allocates the number of task slots based on the data provided in the configuration
     * file. If number of slots is not mentioned, then uses the available and jvm memory
     * settings to compute one. If computation is not successful, uses 2 as the default
     * value.
     */
    private static void allocateNumberOfTaskTrackers() {
        numberOfWorkers = cfg.getNumberOfTaskSlots();
        if (numberOfWorkers == 0) {
            Runtime runtime = Runtime.getRuntime();
            try {
                int availableProcessors = runtime.availableProcessors();
                long maxUsableMemory = cfg.getMaxUsableMemory();
                long jvmHeapSize = cfg.getJvmHeapSizeInBytes();
                long ratio = (maxUsableMemory / jvmHeapSize);

                numberOfWorkers =
                        (int) Math.max(2, Math.min(availableProcessors, ratio));

            } catch (Exception e) {
                // In case of error while calculating, assign default value
                numberOfWorkers = 2;
            }
        }
        System.out.println("Allocated number of task trackers"
                + numberOfWorkers);
    }

    /**
     * Start the registry for map reduce processes.
     * 
     * @throws IOException when error connecting to registry
     */
    private static void startRegistry() throws IOException {

        hostName = cfg.getRegistryHostName();
        port = cfg.getRegistryListenerPort();
        InetAddress inetAddress = InetAddress.getLocalHost();
        String thisHostName = inetAddress.getHostName();
        String thisHostAddress = inetAddress.getHostAddress();

        if (hostName == null) {
            throw new RuntimeException(
                    "Error: please add the registry host details in config file");
        }

        // Attempt to start registry only if this host is the same as the
        // registry host mentioned in the configuration file
        if (hostName.equals(thisHostName) || hostName.equals(thisHostAddress)) {

            // Start registry only if is not running
            if (!isRegistryRunning()) {

                ProcessBuilder pb =
                        new ProcessBuilder(javaHome, classPathCommand,
                                cfg.getClasspath(), registryClass);
                registryProcess = pb.start();

                pb.redirectErrorStream(true);

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    System.out.println("Shoudnt come here");
                }
            }
        }

    }

    /**
     * Check is the registry is currently running.
     * 
     * @return true if registry is currently running, otherwise false
     * @throws IOException when there is an error connecting to the registry
     */
    private static boolean isRegistryRunning() throws IOException {
        boolean isRegistryRunning = false;
        Socket socket = new Socket();
        SocketAddress socketAddress = new InetSocketAddress(hostName, port);

        try {

            socket.connect(socketAddress, 2000);
            OutputStream outputStream = socket.getOutputStream();
            MRUtility.writeMessageToStream(outputStream, PING);
            outputStream.close();
            isRegistryRunning = true;

        } catch (SocketTimeoutException e) {
            System.out.println("Timeout when connecting to registry");
        } catch (ConnectException e) {
            System.out.println("Registry not running");
        } finally {
            if (!socket.isClosed()) {
                socket.close();
            }
        }

        return isRegistryRunning;
    }

    /**
     * Starts the task trackers using the parameters provided in the configuration file.
     * Launches one listener thread for each task tracker, which prints the message
     * received from the process's input stream to the console
     * 
     * @throws IOException when waitForShutDown() throws an IOException
     */
    private static void startTaskTrackers() throws IOException {

        Thread[] starters = new Thread[numberOfWorkers];
        outWriters = new PrintWriter[numberOfWorkers];

        ProcessBuilder processBuilder =
                new ProcessBuilder(javaHome, "-Xmx2g", classPathCommand,
                        cfg.getClasspath(), taskTrackerClass);
        processBuilder.redirectErrorStream(true);

        for (int i = 0; i < numberOfWorkers; i++) {
            Process process = processBuilder.start();
            InputStreamListener in =
                    new InputStreamListener(process.getInputStream());
            outWriters[i] =
                    new PrintWriter(new OutputStreamWriter(
                            process.getOutputStream()));
            starters[i] = new Thread(in);
            starters[i].start();
        }

        waitForShutdown();
    }

    /**
     * Listens on the console input stream for a shutdown message. If a shutdown message
     * is received, initiates the shutdown process.
     * 
     * @throws IOException when there is an error reading from the input stream
     */
    private static void waitForShutdown() throws IOException {
        BufferedReader binp =
                new BufferedReader(new InputStreamReader(System.in));
        String line = null;

        while ((line = binp.readLine()) != null) {

            if (SHUTDOWN_KEY.equals(line)) {
                sendShutdownMessage(outWriters);
                break;
            } else {
                System.out.println("Invalid message");
            }
        }
    }

    /**
     * Sends the shutdown message to all task tracker processes running that were launched
     * by the NodeManager
     * 
     * @param outWriters an array of {@PrintWriter} that wraps the input
     *            stream
     */
    private static void sendShutdownMessage(PrintWriter[] outWriters) {
        for (PrintWriter printWriter : outWriters) {
            printWriter.println(SHUTDOWN_KEY);
            printWriter.flush();
            printWriter.close();
        }
    }

    /**
     * If the registry was started by this NodeManager, then the registry process is
     * killed (method is invoked on exit)
     */
    private static void shutdownRegistryOnExit() {
        if (registryProcess != null) {
            registryProcess.destroy();
        }
    }
}

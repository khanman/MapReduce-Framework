package mr.worker;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.PriorityQueue;

import mr.common.MRUtility;
import mr.common.Task;

/**
 * <p>
 * TaskListener listens for the Tasks and the associated data (such as file
 * splits for Map tasks). Once a Task is received, adds the task to the
 * TaskTracker's "task queue" and performs the appropriate action such as
 * saving the split file for map tasks.
 * 
 * <p>
 * Every TaskTracker launches one TaskTracker thread
 * 
 * 
 */
public class TaskListener implements Runnable {

    private PriorityQueue<Task> taskExecutionQueue;
    private Socket socket;
    private String tempDirectory;

    /**
     * 
     * @param socket {@link Socket} connection to the ApplicationMaster
     * @param taskExecutionQueue {@link PriorityQueue} task queue of the
     *            TaskTracker
     * @param tempDirectory - temporary directory for mapper to which the file
     *            split will be written
     */
    public TaskListener(Socket socket, PriorityQueue<Task> taskExecutionQueue,
            String tempDirectory) {
        this.taskExecutionQueue = taskExecutionQueue;
        this.socket = socket;
        this.tempDirectory = tempDirectory;
    }

    /**
     * Blocks until there is a task available in the InputStream, if a Task was
     * received, adds the task to the TaskTrackers Task queue and performs
     * appropriate action if required
     * 
     * <pre>
     * The types of tasks supported are,
     * 
     * 1)Task.TYPE.MAP
     * 2)Task.TYPE.REDUCE
     * 3)Task.TYPE.END
     * </pre>
     */
    @Override
    public void run() {
        boolean hasMoreTasks = true;
        while (hasMoreTasks) {
            try {
                InputStream inputStream = socket.getInputStream();
                if (inputStream.available() > 0) {
                    // Read task from the input stream
                    Task task = readTaskObjectFromInputStream(inputStream);

                    // Perform the appropriate action based on the task type
                    if (task.getType() == Task.TYPE.MAP) {
                        setupMapTask(task, inputStream);
                    } else if (task.getType() == Task.TYPE.END) {
                        hasMoreTasks = false;
                    }
                    // Add task to the TaskTracker's execution queue
                    synchronized (taskExecutionQueue) {
                        taskExecutionQueue.add(task);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * Reads the Task object from the given InputStream
     * 
     * @param inputStream {@link InputStream}
     * 
     * @return {@link Task} the task object retrieved from the given
     *         InputStream
     *         
     * @throws IOException when there is error reading the input stream
     */
    private Task readTaskObjectFromInputStream(InputStream inputStream)
            throws IOException {
        try {
            ObjectInputStream o = new ObjectInputStream(inputStream);
            Task task = (Task) o.readObject();
            return task;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * Sets up the Map task by saving the file split to a temporary directory
     * 
     * @param task {@link Task}
     * @param inputStream {@link InputStream}
     * 
     * @throws IOException when there is error reading the input stream
     */
    private void setupMapTask(Task task, InputStream inputStream)
            throws IOException {
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        File splitFile = MRUtility.receiveFile(tempDirectory, dataInputStream);
        task.setSplitFileForTask(splitFile);
    }
}

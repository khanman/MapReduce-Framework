package mr.worker;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.PriorityQueue;

import mr.common.Task;
import static mr.common.MRUtility.sleep;

/**
 * Updates the ApplicationMaster by sending the list Completed tasks from the
 * TaskTracker's CompletedTaskQueue (by Task priority)
 * 
 * 
 */
public class StatusUpdater implements Runnable {

    PriorityQueue<Task> completedTasksQueue;
    Socket socket;

    /**
     * Constructor
     * 
     * @param socket {@link Socket} connection to the ApplicationMaster
     * @param completedTasksQueue {@link PriorityQueue} of TaskTracker containing
     *            completed {@link Task}
     */
    StatusUpdater(Socket socket, PriorityQueue<Task> completedTasksQueue) {
        this.completedTasksQueue = completedTasksQueue;
        this.socket = socket;
    }

    /**
     * Monitors the CompletedTaskQueue for new tasks. While there are tasks in the queue,
     * removes highest priority Task from the the queue and writes the object to the
     * OutputStream
     */
    @Override
    public void run() {
        boolean hasMoreTasks = true;
        while (hasMoreTasks) {
            synchronized (completedTasksQueue) {
                while (!completedTasksQueue.isEmpty()) {
                    try {
                        Task task = completedTasksQueue.poll();
                        OutputStream outputStream = socket.getOutputStream();
                        writeObjectToStream(outputStream, task);
                        if (task.getType() == Task.TYPE.END) {
                            hasMoreTasks = false;
                            System.out.println("Status Updater end task");
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
            }
            sleep(500);
        }
        System.out.println("Status Updater has ended");
    }

    /**
     * Write the given object to the given output stream
     * 
     * @param outputStream {@link OutputStream}
     * @param object {@link Object}
     * @throws IOException when there is an error writing to the output stream
     */
    private static void writeObjectToStream(
            OutputStream outputStream,
            Object object) throws IOException {
        ObjectOutputStream objectOutputStream =
                new ObjectOutputStream(outputStream);
        objectOutputStream.writeObject(object);
        objectOutputStream.flush();
    }

}

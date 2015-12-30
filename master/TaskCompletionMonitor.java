package mr.master;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import mr.common.Task;
import mr.common.Task.TYPE;

/**
 * Monitors the completion of tasks for each worker. The successful completion
 * of all map/reduce tasks assigned to a particular worker is indicated by an
 * 'END' task. Once the 'END' task is received, the StatusMonitor thread comes
 * to a halt.
 * 
 * 
 */
public class TaskCompletionMonitor implements Runnable {

	private final Socket socket;
	private PriorityQueue<Task> taskQueue;
	public static final Log LOG = LogFactory
			.getLog(TaskCompletionMonitor.class);

	public TaskCompletionMonitor(final Socket socket,
			PriorityQueue<Task> taskQueue) {
		this.socket = socket;
		this.taskQueue = taskQueue;
	}

	@Override
	public void run() {
		InputStream inputStream = null;
		while (true) {
			try {
				inputStream = socket.getInputStream();
				// Blocks until a completed Task is sent by the worker. Once it
				// is received, the Task is removed from the task queue
				ObjectInputStream objectInputStream = new ObjectInputStream(
						inputStream);
				Task task = (Task) objectInputStream.readObject();
				synchronized (taskQueue) {
					taskQueue.remove(task);
					LOG.debug("Task " + task.getTaskId() + " completed");
				}
				if (task.getType() == TYPE.END) {
					break;
				}
			} catch (IOException e) {
				LOG.fatal("Error while monitoring task completion", e);
				throw new RuntimeException(e);
			} catch (ClassNotFoundException e) {
				LOG.fatal("Error while monitoring task completion", e);
				throw new RuntimeException(e);
			}
		}
		// close connection on exit
		if (inputStream != null) {
			try {
				inputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}

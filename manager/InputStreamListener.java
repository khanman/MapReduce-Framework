package mr.manager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Listener for the input stream of TaskTracker. Monitors the input stream for
 * String data and prints the data received to System.out
 * 
 * 
 */
public class InputStreamListener implements Runnable {

    private InputStream in;

    /**
     * Inputstream of a TaskTracker
     * 
     * @param in {@link InputStream}
     */
    public InputStreamListener(InputStream in) {
        this.in = in;
    }

    /**
     * Prints any message received in the input stream to System.out
     * 
     * @see InputStream
     */
    @Override
    public void run() {
        String line = null;
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        try {
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}

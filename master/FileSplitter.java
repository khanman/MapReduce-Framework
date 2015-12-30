package mr.master;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Handles computation of the File splits based on the given split size, while retaining
 * the integrity of split data for any file with line separated data.
 * 
 * @author Magesh Ramachandran
 * @author Mansoor Ahmed Khan
 * 
 */
public class FileSplitter {
    private RandomAccessFile randomAccessFile;
    private long fileLength;
    private long bytesRemaining;
    private int splitSize;
    private int numberOfMapTasks;
    public static final Log LOG = LogFactory.getLog(FileSplitter.class);

    /**
     * Constructor, to initialize the instance fields.
     * 
     * @param fileName - input file name
     * @param splitSize - input split size in bytes
     */
    public FileSplitter(String fileName, int splitSize) {
        try {

            randomAccessFile = new RandomAccessFile(fileName, "r");
            fileLength = randomAccessFile.length();
            bytesRemaining = fileLength;
            numberOfMapTasks = (int) Math.ceil(fileLength / splitSize);
            this.splitSize = splitSize;

        } catch (IOException e) {
            LOG.fatal("Error initializing FileSplitter");
            throw new RuntimeException(e);
        }
    }

    /**
     * To check if there are any more splits to be read.
     * 
     * @return true if there are more splits to be read, false othewise
     */
    public boolean hasMoreSplits() {
        return (bytesRemaining > 0);
    }

    /**
     * Get the estimated number of file splits.
     * 
     * @return approximate estimate of the number of file splits(same as the number map
     *         tasks)
     */
    public int getEstimatedNumberOfFileSplits() {
        return numberOfMapTasks;
    }

    /**
     * Reads '~splitSize' bytes of data sequentially till the end of file. The data is
     * read such that the start and end values of the byte chunk corresponds to a new line
     * and an end of line respectively
     * 
     * @return the next 'file split'
     */
    public byte[] getNextSplit() {
        try {

            long nextChunkSize = Math.min(splitSize, bytesRemaining);
            byte[] split = new byte[(int) nextChunkSize];
            int bytesRead = randomAccessFile.read(split);
            long filePointer = randomAccessFile.getFilePointer();

            // resize split array so that the data ends with an end of line
            // character
            int newSize = getBytesToEndOfLine(split);
            split = Arrays.copyOf(split, newSize);

            int effectiveBytesRead = (bytesRead - newSize);

            // move the file pointer to the start of the discarded data
            randomAccessFile.seek(filePointer - effectiveBytesRead);
            bytesRemaining = bytesRemaining - newSize;

            System.out.println("bytesRemaining" + bytesRemaining);
            return split;

        } catch (IOException e) {
            LOG.fatal("Error while reading the next split");
            throw new RuntimeException(e);
        }
    }

    /**
     * returns the position of the end of line character relative to the last index of the
     * given byte array
     * 
     * @param bytes - byte array
     * @return position of end of line character from the last index
     */
    private int getBytesToEndOfLine(byte[] bytes) {
        for (int i = bytes.length - 1; i > 0; i--) {
            if (bytes[i] == '\n') {
                return i + 1;
            }
        }
        return bytes.length;
    }
}

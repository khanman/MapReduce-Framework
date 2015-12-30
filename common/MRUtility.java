package mr.common;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;

/**
 * Static utility methods for common functionality used by the MR framework.
 * 
 * 
 * 
 */
public class MRUtility {

    /**
     * Same as Thread.sleep, but any InterruptedException is ignored
     * 
     * @param timeInMillis - time to sleep in milliseconds
     */
    public static void sleep(long timeInMillis) {
        try {
            Thread.sleep(timeInMillis);
        } catch (InterruptedException e) {
        }
    }

    /**
     * <pre>
     * Write the file corresponding to the given path to the give output stream.
     * 
     * Performs the following actions in sequence  
     * 1)Writes file name to stream
     * 2)Write the file length in bytes
     * 3)Write the file data
     * </pre>
     * 
     * @param filePath: file path of the file to written to the given stream
     * @param outputStream: {@link OutputStream}
     * 
     * @throws IOException when there is an error writing to the output stream
     */
    public static void sendFile(String filePath, OutputStream outputStream)
            throws IOException {

        File file = new File(filePath);
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");

        String fileName = file.getName();
        long fileLength = randomAccessFile.length();
        byte[] fileData = new byte[(int) fileLength];
        randomAccessFile.readFully(fileData);
        randomAccessFile.close();

        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        dataOutputStream.writeUTF(fileName);
        dataOutputStream.flush();

        dataOutputStream.writeLong(fileLength);
        dataOutputStream.flush();

        dataOutputStream.write(fileData, 0, (int) fileLength);
        dataOutputStream.flush();
    }

    /**
     * <pre>
     * Reads a file from the given input stream and saves it in the folder
     * specified Performs the following actions in sequence.
     * 
     * 1)reads file name from stream
     * 2)reads the file length in bytes 
     * 3)reads the file data into a byte array
     * 4)write the file to the given folder
     * </pre>
     * 
     * @param fileDirectory - directory to which the file is to be written
     * @param dataInputStream - {@link DataInputStream}
     * 
     * @return {@File} reference of the file save
     * 
     * @throws IOException when there is an error reading from the input stream
     */
    public static File receiveFile(
            String fileDirectory,
            DataInputStream dataInputStream) throws IOException {

        String fileName = dataInputStream.readUTF();
        long fileLength = dataInputStream.readLong();

        File file = new File(fileDirectory, fileName);
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        BufferedOutputStream bufferedOutputStream =
                new BufferedOutputStream(fileOutputStream);
        byte[] bytes = new byte[(int) fileLength];
        dataInputStream.readFully(bytes);
        bufferedOutputStream.write(bytes);

        bufferedOutputStream.flush();
        bufferedOutputStream.close();
        return file;
    }

    /**
     * Write the given object to the given output stream.
     * 
     * @param outputStream {@link OutputStream}
     * @param object {@link Object} to be written to the given output stream
     * 
     * @throws IOException when there is an error writing to the given output stream
     */
    public static void writeObjectToStream(
            OutputStream outputStream,
            Object object) throws IOException {
        ObjectOutputStream objectOutputStream =
                new ObjectOutputStream(outputStream);
        objectOutputStream.writeObject(object);
        objectOutputStream.flush();
    }

    /**
     * Get the next string message from the given input stream
     * 
     * @param inputStream {@link InputStream} to be read
     * 
     * @return {@link String} message from the input stream
     * 
     * @throws IOException when there is an error reading from the given stream
     */
    public static String getMessageFromStream(InputStream inputStream)
            throws IOException {
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        return dataInputStream.readUTF();
    }

    /**
     * Write the given text message to the given output stream
     * 
     * @param outputStream {@link OutputStream} to which the given message must be written     *            
     * @param message {@link String} message to be written to the given stream
     * 
     * @throws IOException when there is an error writing to the given stream
     */
    public static void writeMessageToStream(
            OutputStream outputStream,
            String message) throws IOException {
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        dataOutputStream.writeUTF(message);
        dataOutputStream.flush();
    }

}

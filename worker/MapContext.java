package mr.worker;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import mr.common.Configuration;
import mr.common.KeyValuePair;
import mr.io.BufferedLineReader;
import mr.io.LongWritable;
import mr.io.MapReduceObject;
import mr.io.Text;

/**
 * <p>
 * Reads the compressed input split files and writes map output in a compressed format.
 * Generates Key Value pair which is passed to the Mapper's map method. Key generated is
 * the file offset position + (file split number * split size), which is unique across all
 * Tasks run on different nodes.
 * 
 * <p>
 * Keeps the map output in a temporary collection till it reaches a threshold(twice the
 * split size by default). Once the threshold is reached , in-memory sort is performed on
 * collection before spilling to disk. The data is written to disk in a compressed format
 * 
 * <p>
 * The output files are partitioned correctly according to the number of reducers by using
 * mod logic on the hash code for each key. The files are labeled accordingly so that they
 * are sent to the correct reducer.
 * 
 * 
 * @param <KEYIN>
 * @param <VALUEIN>
 * @param <KEYOUT>
 * @param <VALUEOUT>
 * 
 */

public class MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    private static final String tempFile = "_temp.m",
            completeFile = "complete";

    private KEYIN currentKey;
    private VALUEIN currentValue;
    private String outputAbsFilePath;

    private String tempPath;
    private String finalPath;

    private BufferedLineReader reader;

    private List<KeyValuePair<KEYOUT, VALUEOUT>> keyValuePairs =
            new ArrayList<KeyValuePair<KEYOUT, VALUEOUT>>(200000);

    private long splitSize;
    private long spillSize;
    private int currentLine;
    private int splitNumber;
    private int numberOfReducers;
    private int spillCount = 0;
    private int bytesInBuffer = 0;

    /**
     * Constructor. Initializes the fields using data from the given configuration,
     * 
     * @param configuration contains info about the split size, number of reducers
     *            {@link Configuration}
     * @param inputSplit input split file for this map task
     * @param dataDir directory to store the map task output
     */
    public MapContext(Configuration configuration, File inputSplit,
            String dataDir) {

        try {

            this.numberOfReducers = configuration.getNumberOfReducers();
            this.outputAbsFilePath = new File(".").getCanonicalPath();
            this.splitSize = configuration.getSplitSize();
            this.spillSize = splitSize * 2;

            splitNumber = getSplitNumberFromInputFile(inputSplit.getName());

            // Prepare the path for map task output
            String pathStr =
                    outputAbsFilePath + File.separator + dataDir
                            + File.separator;
            this.tempPath = pathStr + tempFile + splitNumber;
            this.finalPath = pathStr + completeFile + splitNumber;

            // Reader to inflate and read the the compressed file
            reader =
                    new BufferedLineReader(new InputStreamReader(
                            new InflaterInputStream(new FileInputStream(
                                    inputSplit))));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Processes the file name to get the split number
     * 
     * @param fileName name of the input split file
     * @return split number
     */
    private static int getSplitNumberFromInputFile(String fileName) {
        int splitNumber =
                Integer.parseInt(fileName.substring(
                        fileName.lastIndexOf('_') + 1, fileName.length()));
        return splitNumber;
    }

    /**
     * Get the current key
     * 
     * @return KEYIN
     */
    public KEYIN getCurrentKey() {
        return currentKey;
    }

    /**
     * This method returns the current line read from the input split file
     * 
     * @return VALUEIN
     */
    public VALUEIN getCurrentValue() {
        return currentValue;
    }

    /**
     * Determines the next key value pair from the input split file. Key generated is
     * file_pointer + (split number * split size). Value is a {@link Text} containing the
     * current line read from file.
     * 
     * 
     * Once all the lines are read from the input, the file is closed
     * 
     * @return true if there is at least one more value to be read for the currentKey,
     *         false otherwise
     * 
     */
    @SuppressWarnings("unchecked")
    public boolean nextKeyValue() {

        try {
            this.currentKey =
                    (KEYIN) new LongWritable(
                            (reader.getFilePosition() + (this.splitNumber * this.splitSize)));

            String line = "";
            line = reader.readLine();

            if (line != null && !line.equals("")) {
                this.currentValue = (VALUEIN) new Text(line);
                currentLine++;
                return true;
            }

            // The below code will be invoked only when there are no more keys
            // to be read from the input split file
            this.currentValue = null;
            sortKeyValPairsByKey();
            spillToFile();
            reader.close();

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return false;
    }

    /**
     * Writes the processed map data into in-memory buffer until it reaches a threshold
     * (twice the split size by default). Once the threshold is reached, the data is
     * sorted and spilled to files with the and the buffer is freed. Keeps track of the
     * number of bytes read so far in order to test against the threshold
     * 
     * @param key - key written by the map method
     * @param value - value written by the map method
     */

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void write(KEYOUT key, VALUEOUT value) {

        int keySize = ((MapReduceObject) key).getSizeInBytes();
        int valueSize = ((MapReduceObject) value).getSizeInBytes();

        bytesInBuffer = bytesInBuffer + keySize + valueSize;

        if (bytesInBuffer > spillSize) {
            sortKeyValPairsByKey();
            spillToFile();
            // Reset buffer after spilling
            keyValuePairs =
                    new ArrayList<KeyValuePair<KEYOUT, VALUEOUT>>(200000);
            keyValuePairs.add(new KeyValuePair(key, value));
            // Reset bytes read so far
            bytesInBuffer = 0;
        } else {
            keyValuePairs.add(new KeyValuePair(key, value));
        }
    }

    /**
     * Sorts the in-memory buffer using Collections.sort
     * 
     * @see java.util.Collections
     */
    public void sortKeyValPairsByKey() {
        Collections.sort(keyValuePairs);
    }

    /**
     * Spills data from the in-memory buffer to disk. The data is compressed before
     * spilling
     */
    public void spillToFile() {
        File[] mapperOutputFiles = new File[this.numberOfReducers];
        ObjectOutputStream[] objectOutputStream =
                new ObjectOutputStream[this.numberOfReducers];

        try {

            initializeCompressedObjectOutputStream(mapperOutputFiles,
                    objectOutputStream);

            writeMapBufferToStream(objectOutputStream);

            renameFilesOnceComplete(mapperOutputFiles, objectOutputStream);

            spillCount++;

        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Renames the temporary mapper output files immediately after they are written to
     * disk. The renamed file is of the format completeX_Y_Z, where X is the split id, Y
     * is the spill number and Z is the reducer id. This is done to ensure that only the
     * completed files are sent to the reducer.
     * 
     * @param mapperOutputFiles: array of {@link File} Output files of the mapper, one for
     *            each reducer
     * @param objectOutputStream: array of {@link ObjectOutputStream} that are to be
     *            closed on completion
     * 
     * @throws IOException when there is an error while flushing/closing the output stream
     */
    private void renameFilesOnceComplete(
            File[] mapperOutputFiles,
            ObjectOutputStream[] objectOutputStream) throws IOException {
        for (int reducer = 0; reducer < this.numberOfReducers; reducer++) {
            objectOutputStream[reducer].flush();
            objectOutputStream[reducer].close();
            File renamedFile =
                    new File(finalPath + "_" + spillCount + "_" + reducer);
            mapperOutputFiles[reducer].renameTo(renamedFile);
        }
    }

    /**
     * 
     * Write all the key and values from in-memory map buffer to the
     * {@link ObjectOutputStream}, which in turn writes to the map output files
     * 
     * @param objectOutputStream {@link ObjectOutputStream}
     * @throws IOException when there is an error writing the objects to the output stream
     */
    private
            void
            writeMapBufferToStream(ObjectOutputStream[] objectOutputStream)
                    throws IOException {

        int count = 0;
        for (KeyValuePair<KEYOUT, VALUEOUT> keyValPair : keyValuePairs) {
            int reducerId = computeReducerIdForKey(keyValPair);
            objectOutputStream[reducerId].writeObject(keyValPair.getKey());
            objectOutputStream[reducerId].writeObject(keyValPair.getVal());
            // Invoke reset periodically so that the objects read from the file
            // are eligible for garbage collection. This value was chosen for
            // performance reasons
            if (count % 20480 == 0) {
                objectOutputStream[reducerId].reset();
            }
            count++;
        }
    }

    /**
     * Initializes the compressed object output stream to which the mapper output will be
     * written. Uses {@link DeflaterOutputStream} with Deflater.BEST_SPEED settings
     * 
     * @param mapperOutputFiles: array of {@link File} Output files of the mapper, one for
     *            each reducer
     * @param objectOutputStream: array of {@link ObjectOutputStream}, one for each
     *            reducer
     *            
     * @throws IOException {@link IOException}
     * @throws FileNotFoundException when the file cannot be read by the FileOutputStream
     */
    private void initializeCompressedObjectOutputStream(
            File[] mapperOutputFiles,
            ObjectOutputStream[] objectOutputStreams)
            throws IOException,
            FileNotFoundException {

        // Initialize OutputStream to serialize and compress the mapper output Objects to
        // files
        for (int reducerId = 0; reducerId < this.numberOfReducers; reducerId++) {
            File file = new File(tempPath + "_" + spillCount + "_" + reducerId);
            mapperOutputFiles[reducerId] = file;

            Deflater def = new Deflater(Deflater.BEST_SPEED);

            objectOutputStreams[reducerId] =
                    new ObjectOutputStream((new BufferedOutputStream(
                            new DeflaterOutputStream(
                                    new FileOutputStream(file), def))));

        }
    }

    /**
     * 
     * Returns the reducer id for the key in the given keyValPair
     * 
     * @param keyValPair {@link KeyValuePair}
     * @return the reducer Id computed for the key
     * 
     *         Tried using hash function (keyValPair.getKey().hashCode() %
     *         this.numberOfReducers), but that sometimes caused the reducer Id to be set
     *         as -1. Reference:
     *         http://ercoppa.github.io/HadoopInternals/AnatomyMapReduceJob.html
     */
    public
            int
            computeReducerIdForKey(KeyValuePair<KEYOUT, VALUEOUT> keyValPair) {
        return (Integer.MAX_VALUE & keyValPair.getKey().hashCode())
                % this.numberOfReducers;
    }

    /**
     * utility method to cleanup the temp files created once the intermediate files are
     * transferred to the reducer
     */
    public void cleanUp() {
        File folder = new File(outputAbsFilePath);
        File[] files = folder.listFiles();

        for (File file : files) {
            file.delete();
        }
    }
}
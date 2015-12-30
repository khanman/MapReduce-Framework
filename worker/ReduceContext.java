package mr.worker;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.zip.InflaterInputStream;

import mr.common.Configuration;

/**
 * Reads the compressed mapper output files and writes the reduced output to a file. The
 * pre-sorted mapper output files are then merged on the fly in sorted order while being
 * read from the disk.
 * 
 * 
 * @param <KEYIN>
 * @param <VALUEIN>
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */

public class ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    private static final String OUTPUTDIR = "output";
    private static final String OUTPUTFILENAME = "part_";

    private ArrayList<String> intermediateFileNames = new ArrayList<String>();;

    private String intermediateFilesPath;
    private String reducerId;

    // To read the mapper output files
    private ObjectInputStream[] objectInputStream;

    private PrintWriter reducerWriter;

    // Maintains the indices of files to be read while fetching the next key/value
    private Set<Integer> filesToProceed = new HashSet<Integer>();

    private VALUEIN value;
    private KEYIN currentKey;
    private KEYIN previousKey;

    // Iterable to iterate through all values for a key
    private Iterable<VALUEIN> values = new ValueIterable();

    // Used for merge sorting the mapper output files as they are being read
    private List<FileIdValuePair<VALUEIN>> currentValues =
            new ArrayList<FileIdValuePair<VALUEIN>>();
    private SortedMap<KEYIN, List<FileIdValuePair<VALUEIN>>> keyValCache =
            new TreeMap<KEYIN, List<FileIdValuePair<VALUEIN>>>();

    private boolean isFirst = true;
    private boolean hasNextKey = true;

    /**
     * Initializes the mapper output files path to read Gets the list of mapper out files
     * to shuffle and sort Sets the reducer output path
     * 
     * @param config {@link Configuration}
     * @param tempDirectory path in which the mapper files are stored
     * @param taskId reducer id
     */
    public ReduceContext(Configuration config, String tempDirectory,
            String taskId) {
        this.intermediateFilesPath = tempDirectory;
        getListOfFilesToProcess();
        this.reducerId = taskId;
        initializeInputStreamsForFiles();
        setUpForReducerOutput();
    }

    /**
     * Iterable to iterate through all the values for the current key
     * 
     * @author Deepak Jagadeesh
     * @see java.util.Iterable
     * @see java.util.Iterator
     * 
     */
    protected class ValueIterable implements Iterable<VALUEIN> {
        private ValueIterator iterator = new ValueIterator();

        /**
         * returns {@link Iterator} for iterating through the values
         */
        @Override
        public Iterator<VALUEIN> iterator() {
            return iterator;
        }
    }

    /**
     * Gets list of mapper output files to process for the reduce task
     */
    private void getListOfFilesToProcess() {

        File[] files = new File(intermediateFilesPath).listFiles();

        for (File f : files) {
            String fileName = f.getName();
            this.intermediateFileNames.add(fileName);
        }

    }

    /**
     * Initializes the stream for reading compressed Mapper output files
     */
    private void initializeInputStreamsForFiles() {
        try {
            System.out.print("intermediate" + intermediateFileNames);
            objectInputStream =
                    new ObjectInputStream[this.intermediateFileNames.size()];
            int count = 0;
            // Initializing the input to read from compressed & serialized object file
            for (String fileName : intermediateFileNames) {
                objectInputStream[count] =
                        new ObjectInputStream(new BufferedInputStream(
                                new InflaterInputStream(new FileInputStream(
                                        intermediateFilesPath + File.separator
                                                + fileName))));

                filesToProceed.add(count);
                count++;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Helper method to create a new directory
     * 
     * @param dataDir a directory to be created
     */
    private void createNewDirIfNotFound(String dataDir) {
        File dir = new File(dataDir);
        if (!dir.isDirectory()) {
            dir.mkdirs();
        }
    }

    /**
     * Helper method to initialize File writer for output of the reduce task
     * 
     */
    private void setUpForReducerOutput() {
        try {
            createNewDirIfNotFound(OUTPUTDIR);
            String reducerOutputPath =
                    new File(".").getCanonicalPath() + File.separator
                            + OUTPUTDIR + File.separator + OUTPUTFILENAME
                            + this.reducerId;

            this.reducerWriter = new PrintWriter(reducerOutputPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Check if there is anymore key to be processed from mapper output files.
     * 
     * @return true if there is a key to process, false otherwise
     */
    public boolean hasNextKey() {
        if (isFirst) {
            nextKeyValue();
            return true;
        } else {
            if (currentKey == null) {
                cleanup();
                return false;
            } else {
                previousKey = currentKey;
                if (!hasNextKey) {
                    cleanup();
                }
                return hasNextKey;
            }
        }
    }

    /**
     * Closes all the streams for mapper output files and the reduce task output file
     * 
     */
    private void cleanup() {
        for (ObjectInputStream in : objectInputStream) {
            try {
                in.close();
            } catch (IOException e) {
                System.out.println("ERROR: Exception while closing the file");
            }
            reducerWriter.close();
        }
    }

    /**
     * Object to hold a file id along with a value
     * 
     * @author Deepak Jagadeesh
     * 
     * @param <VALUEIN> object of type VALUEIN
     */
    static class FileIdValuePair<VALUEIN> {
        private VALUEIN key;
        private Integer value;

        FileIdValuePair(VALUEIN key, Integer value) {
            this.key = key;
            this.value = value;
        }

        /**
         * @return the value
         */
        public VALUEIN getValue() {
            return key;
        }

        /**
         * @return the file id
         */
        public Integer getFileId() {
            return value;
        }

    }

    /**
     * To iterate through all the values for the current key
     * 
     * @author Deepak Jagadeesh
     * @see java.util.Iterator
     */

    public class ValueIterator implements Iterator<VALUEIN> {

        /**
         * @return true if there is anymore value for the current key
         * 
         */
        @Override
        public boolean hasNext() {
            if (isFirst || previousKey == null) {
                previousKey = currentKey;
                isFirst = false;
                return true;
            }
            try {
                nextKeyValue();
            } catch (NoSuchElementException e) {
                hasNextKey = false;
                return false;
            }
            return previousKey.equals(currentKey);
        }

        /**
         * get next value
         */
        @Override
        public VALUEIN next() {
            return value;
        }

        @Override
        public void remove() {

        }
    }

    /**
     * Reads one line from all the reducer input files at a time and stores it in a
     * SortedMap used as a cache from which the next values are read. Once all the values
     * for a particular key have been read, reads the next line from only those reducer
     * input files from which those values read were obtained. This process is repeated
     * until all the input files have been read completely.
     * 
     * throws {@link NoSuchElementException} when all the reducer input files have been
     * read
     * 
     * @see java.util.TreeMap
     * @see java.util.Set
     * @see java.util.Collections
     */
    @SuppressWarnings("unchecked")
    private void nextKeyValue() {
        try {
            if (currentValues.size() > 0) {
                value = currentValues.remove(0).getValue();
            } else {
                for (Integer fileId : filesToProceed) {
                    KEYIN key = null;
                    VALUEIN value = null;
                    // Get the next key and value
                    try {
                        key = (KEYIN) objectInputStream[fileId].readObject();
                        value =
                                (VALUEIN) objectInputStream[fileId]
                                        .readObject();
                    } catch (EOFException eof) {
                        // Comes here when one of the files have been read completely.
                        // This can be safely ignored and the next file from
                        // filesToProceed will be processed
                        continue;
                    }

                    // Add the current key and the list of file ids containing the key
                    // along with the corresponding values to the cache
                    List<FileIdValuePair<VALUEIN>> currentValuesForKey =
                            keyValCache.get(key);
                    if (currentValuesForKey == null) {
                        currentValuesForKey =
                                new ArrayList<FileIdValuePair<VALUEIN>>();
                        currentValuesForKey.add(new FileIdValuePair<VALUEIN>(
                                value, fileId));
                        keyValCache.put(key, currentValuesForKey);
                    } else {
                        currentValuesForKey.add(new FileIdValuePair<VALUEIN>(
                                value, fileId));
                    }
                }
                // Get and remove the first key from the cache
                KEYIN key = keyValCache.firstKey();
                previousKey = currentKey;
                currentKey = key;
                currentValues = keyValCache.remove(key);

                // Add only the file id's corresponding to the file that contained the key
                // removed
                filesToProceed.clear();
                for (FileIdValuePair<VALUEIN> va : currentValues) {
                    filesToProceed.add(va.getFileId());
                }
                if (previousKey == null || currentKey.equals(previousKey)) {
                    value = currentValues.remove(0).getValue();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * @return values for the current key {@link Iterable}
     */
    public Iterable<VALUEIN> getValues() {
        return values;
    }

    /**
     * write the output of reduce task to the file
     * 
     * @param key
     * @param value
     */
    public void write(KEYOUT key, VALUEOUT value) {
        this.reducerWriter.println(key + "--" + value);
        this.reducerWriter.flush();
    }

    /**
     * @return current key in process
     */
    public KEYIN getCurrentKey() {
        return this.currentKey;
    }

}

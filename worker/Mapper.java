package mr.worker;

import java.io.File;

import mr.common.Configuration;

/**
 * Maps key/value read from the input file to a set of intermediate key/value pairs.
 * 
 * Reference: This class is based on Apache Hadoop's Mapper class.  
 * 
 * 
 * @param <KEYIN>
 * @param <VALUEIN>
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    /**
     * For mapping the generic types of the Mapper class to the Generic types of
     * MapContext
     * 
     * @see MapContext
     * 
     * 
     */
    public class Context extends MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
        Context(Configuration configuration, File inputSplit, String dataDir) {
            super(configuration, inputSplit, dataDir);
        }
    }

    /**
     * Runs until all the values are processed from the input file. Invokes the map
     * function for every key-value pair read from the input file until all the keys have
     * been read
     * 
     * @param context {@link Context}
     */
    public void run(Context context) {
        while (context.nextKeyValue()) {
            map(context.getCurrentKey(), context.getCurrentValue(), context);
        }
    }

    /**
     * Called once for each key value pair. If the method is not overridden, writes the
     * same key value pair read from input
     * 
     * @param key
     * @param value
     * @param ctx {@link Context}
     */
    @SuppressWarnings("unchecked")
    public void map(KEYIN key, VALUEIN value, Context ctx) {
        ctx.write((KEYOUT) key, (VALUEOUT) value);
    }

}

package mr.worker;

import mr.common.Configuration;

/**
 * Reduces eack pair of mapper output key and the list of its values to a final output.
 * 
 * Reference: This class is based on Apache Hadoop's Reducer class.
 * 
 * @author Deepak Jagadeesh
 * 
 * @param <KEYIN>
 * @param <VALUEIN>
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */

public class Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    /**
     * For mapping the generic types of the Reducer class to the Generic types of
     * ReduceContext
     * 
     * @see ReduceContext
     * 
     * @author Deepak Jagadeesh
     * 
     */
    public class Context extends
            ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
        Context(Configuration config, String tempDirectory, String taskId) {
            super(config, tempDirectory, taskId);
        }
    }

    /**
     * Calls the reduce method once for every key from the mapper output (in sorted order
     * of the key). For every key, an Iterable object is provided which has the logic to
     * iterate over all the map output values for the current key
     * 
     * @param ctx {link @Reduce.Context}
     * @see ReduceContext
     */
    public void run(Context ctx) {
        while (ctx.hasNextKey()) {
            reduce(ctx.getCurrentKey(), ctx.getValues(), ctx);
        }
    }

    /**
     * Called once for every key from the map output. the values are an {@link Iterable}
     * Object of all values for the given key
     * 
     * @param key from the Mapper's output
     * @param values {@link Iterable} over all the values for the current key
     * @param ctx
     */
    @SuppressWarnings("unchecked")
    public void reduce(KEYIN key, Iterable<VALUEIN> values, Context ctx) {

        for (VALUEIN value : values) {
            ctx.write((KEYOUT) key, (VALUEOUT) value);
        }
    }
}

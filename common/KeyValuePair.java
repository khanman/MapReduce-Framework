package mr.common;

/**
 * 
 * Object to store a pair of KEY-VALUE objects which of the type specified. Any Collection
 * of {@link KeyValuePair} instances sort in the ascending order specified by the KEY
 * object's compareTo method.
 * 
 * 
 * 
 */

public class KeyValuePair<KEY, VALUE> implements
        Comparable<KeyValuePair<KEY, VALUE>> {

    public KEY key;
    public VALUE val;

    /**
     * Constructor, creates a new instance of {@link KeyValuePair} using the KEY and the
     * VALUE object provided
     * 
     * @param key
     * @param val
     */
    public KeyValuePair(KEY key, VALUE val) {
        this.key = key;
        this.val = val;
    }

    /**
     * Get the key of this instance
     * 
     * @return the key of this instance
     */
    public KEY getKey() {
        return key;
    }

    /**
     * Set the key of this instance
     * 
     * @param key
     */
    public void setKey(KEY key) {
        this.key = key;
    }

    /**
     * Get the value of this instance
     * 
     * @return value of this instance
     */
    public VALUE getVal() {
        return val;
    }

    /**
     * Set the value for this instance
     * 
     * @param val
     */
    public void setVal(VALUE val) {
        this.val = val;
    }

    /**
     * Sorts in ascending of the KEY
     */
    @SuppressWarnings("unchecked")
    public int compareTo(KeyValuePair<KEY, VALUE> other) {
        if (other == null) {
            return -1;
        }
        if (key == null || other.key == null) {
            return 1;
        }
        if (key instanceof Comparable) {
            return ((Comparable<KEY>) key).compareTo(other.key);
        } else {
            return key.toString().compareTo(other.key.toString());
        }
    }
}

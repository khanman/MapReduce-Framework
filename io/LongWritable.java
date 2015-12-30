package mr.io;

/**
 * Serializable container for long values. 
 * 
 * 
 * 
 */
@SuppressWarnings("serial")
public class LongWritable implements MapReduceObject, Comparable<LongWritable> {

    private long value;

    public LongWritable(long value) {
        this.value = value;
    }
    
    /**
     * 
     * @return the long value wrapped by this object
     */
    public long getValue() {
        return this.value;
    }

    public String toString() {
        return String.valueOf(this.value);
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LongWritable other = (LongWritable) obj;
        if (value != other.value)
            return false;
        return true;
    }
    
    /**
     * Sorts in ascending order
     */
    @Override
    public int compareTo(LongWritable o) {
        return this.value < o.value ? -1 : this.value == o.value ? 0 : 1;
    }
    
    /**
     * Size occupied by each instance of this object (approximate)
     */
    @Override
    public int getSizeInBytes() {
        return 4;
    }
}

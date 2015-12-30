package mr.io;

/**
 * Serializable container for int values. 
 * 
 * 
 * 
 */
@SuppressWarnings("serial")
public class IntWritable implements MapReduceObject, Comparable<IntWritable> {

    private int value;

    public IntWritable(int value) {
        this.value = value;
    }
    
    /**
     * Get the int value wrapped by this object
     * 
     * @return the int value wrapped by this object
     */
    public int getValue() {
        return this.value;
    }

    public String toString() {
        return String.valueOf(this.value);
    }

    @Override
    public int hashCode() {
        return this.value;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        IntWritable other = (IntWritable) obj;
        if (value != other.value)
            return false;
        return true;
    }
    
    /**
     * Sorts in ascending order
     */
    @Override
    public int compareTo(IntWritable o) {
        return this.value < o.value ? -1 : this.value == o.value ? 0 : 1;
    }
    
    /**
     * Size occupied by each instance of this object (approximate)
     */
    @Override
    public int getSizeInBytes() {
        return 2;
    }
}

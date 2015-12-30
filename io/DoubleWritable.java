package mr.io;

/**
 * Serializable container for double values.
 *  
 * 
 * 
 */
@SuppressWarnings("serial")
public class DoubleWritable implements MapReduceObject,
        Comparable<DoubleWritable> {

    private double value;

    public DoubleWritable(double value) {
        this.value = value;
    }
    
    /**
     * Get the double value wrapped by this object
     *  
     * @return the double value wrapped by this object
     */
    public double getValue() {
        return this.value;
    }
    
    public String toString() {
        return String.valueOf(this.value);
    }

    @Override
    public int hashCode() {
        long temp;
        temp = Double.doubleToLongBits(value);
        return (int) (temp ^ (temp >>> 32));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DoubleWritable other = (DoubleWritable) obj;
        if (Double.doubleToLongBits(value) != Double
                .doubleToLongBits(other.value))
            return false;
        return true;
    }
    
    /**
     * Sorts in ascending order
     */
    @Override
    public int compareTo(DoubleWritable o) {
        return (this.value < o.value ? -1 : (this.value == o.value ? 0 : 1));
    }
    
    /**
     * Size occupied by each instance of this object (approximate)
     */
    @Override
    public int getSizeInBytes() {
        return 4;
    }

}

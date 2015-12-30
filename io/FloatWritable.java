package mr.io;

/**
 * Serializable container for float values.
 * 
 * 
 * 
 */
@SuppressWarnings("serial")
public class FloatWritable implements MapReduceObject,
        Comparable<FloatWritable> {

    private float value;

    public FloatWritable(float value) {
        this.value = value;
    }

    /**
     * Get the float value wrapped by this object
     * 
     * @return the float value wrapped by this object
     */
    public float getValue() {
        return this.value;
    }

    public String toString() {
        return String.valueOf(this.value);
    }

    @Override
    public int hashCode() {

        return Float.floatToIntBits(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        FloatWritable other = (FloatWritable) obj;
        if (Float.floatToIntBits(value) != Float.floatToIntBits(other.value))
            return false;
        return true;
    }

    /**
     * sorts in ascending order
     */
    @Override
    public int compareTo(FloatWritable o) {
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

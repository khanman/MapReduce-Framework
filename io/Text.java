package mr.io;

/**
 * Serializable container for String values.
 *  
 * 
 * 
 */
@SuppressWarnings("serial")
public class Text implements MapReduceObject, Comparable<Text> {

    private String value;

    public Text(String value) {
        this.value = value;
    }
    
    /**
     * Get the String value wrapped by this object
     *  
     * @return the String value wrapped by this object
     */
    public String getValue(){
        return this.value;
    }
    
    public String toString() {
        return value;
    }

    @Override
    public int hashCode() {
        return ((value == null) ? 0 : value.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Text other = (Text) obj;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        return true;
    }
    
    /**
     * sorts in lexicographical order
     */
    @Override
    public int compareTo(Text o) {
        return this.value.compareTo(o.value);
    }
    
    /**
     * Size occupied by each instance of this object (approximate)
     */
    @Override
    public int getSizeInBytes() {
        if (this.value == null) {
            return 0;
        } else {
            return this.value.length() * 2;
        }
    }

}

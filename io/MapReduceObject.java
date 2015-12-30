package mr.io;

import java.io.Serializable;

/**
 * Interface with method definition to return the size of implementing object. * 
 * All keys and values used by MR framework implement this interface
 * 
 * @author Magesh Ramachandran
 * 
 */
public interface MapReduceObject extends Serializable {
    int getSizeInBytes();
}

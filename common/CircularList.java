package mr.common;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * CircularList enables retrieval of its elements in a circular manner. Once
 * the end of the list has been reached, the next element returned will be the
 * first element in the list and so on. A CircularList with at least one element
 * will iterate indefinitely.
 * 
 * @author Magesh Ramachandran
 * @author Mansoor Ahmed Khan
 * 
 * @param <V>
 */
public class CircularList<V> implements Iterator<V>, Iterable<V> {

    private List<V> items = new ArrayList<V>();
    int currentIndex = -1;

    /**
     * Add items to the list
     * 
     * @param item - item to be added
     */
    public void add(V item) {
        items.add(item);
    }

    /**
     * Remove given item from the list
     * 
     * @param item - item to be removed
     */
    public void remove(V item) {
        items.remove(item);
    }

    /**
     * returns the iterator for this object
     */
    @Override
    public Iterator<V> iterator() {
        return this;
    }

    /**
     * Always returns true for a CircularList of size > 0. returns false
     * otherwise
     */
    @Override
    public boolean hasNext() {
        if (items.size() > 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Once the end of the list is reached, returns its first element and so on
     */
    @Override
    public V next() {
        currentIndex++;
        if (currentIndex == items.size()) {
            currentIndex = 0;
        }
        return items.get(currentIndex);
    }

    /**
     * Not Supported
     */
	@Override
	public void remove() {
		// remove not supported		
	}

}

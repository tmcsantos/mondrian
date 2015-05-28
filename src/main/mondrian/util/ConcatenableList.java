/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2015 Pentaho and others
// All Rights Reserved.
*/
package mondrian.util;

import java.util.*;

/**
 * List backed by a collection of sub-lists.
 *
 * @author Luis F. Canals
 * @since december, 2007
 */
public class ConcatenableList<T> extends AbstractList<T> {
    private static int nextHashCode = 1000;

    // The backing collection of sublists
    private final List<List<T>> lists;

    // The backing hashset for constant time performance
    private final Set<T> hash;
    private int[] indexesTo;

    // List containing all elements from backing lists, populated only after
    // consolidate()
    private List<T> plainList;
    private final int hashCode = nextHashCode++;

    /**
     * Creates an empty ConcatenableList.
     */
    public ConcatenableList() {
        this.lists = new ArrayList<List<T>>();
        this.plainList = null;
        this.hash = new HashSet<T>();
    }

    public <T2> T2[] toArray(T2[] a) {
        consolidate();
        //noinspection unchecked,SuspiciousToArrayCall
        return (T2[]) plainList.toArray((Object []) a);
    }

    public Object[] toArray() {
        consolidate();
        return plainList.toArray();
    }

    /**
     * Performs a load of all elements into memory, removing sequential
     * access advantages.
     */
    public void consolidate() {
        if (this.plainList == null) {
            this.plainList = new ArrayList<T>();
            for (final List<T> list : lists) {
                this.plainList.addAll(list);
            }
        }
    }

    public boolean addAll(final Collection<? extends T> collection) {
        hash.addAll(collection);
        if (this.plainList == null) {
            final List<T> list = (List<T>) collection;
            addToIndexes(list.size());
            return this.lists.add(list);
        } else {
            return this.plainList.addAll(collection);
        }
    }

    private void addToIndexes(int size) {
        if (indexesTo == null) {
            indexesTo = new int[1];
            indexesTo[0] = size - 1;
        } else {
            final int i = indexesTo.length;
            indexesTo = Arrays.copyOf(indexesTo, i + 1);
            int previousSize = indexesTo[i - 1] + size;
            indexesTo[i] = previousSize;
        }
    }

    private int lookupIndex(final int index) {
        int i = Arrays.binarySearch(indexesTo, index);
        if (i < 0) {
            return -i - 1;
        }
        // check for indexed miss if there is empty lists in lists
        while (i != 0 && indexesTo[i] == indexesTo[i - 1]) {
            --i;
        }
        return i;
    }

    public T get(final int index) {
        if (this.plainList == null) {
            final int indexes = lookupIndex(index);
            int newIndex = index;
            if (indexes > 0) {
                newIndex -= (indexesTo[indexes - 1] + 1);
            }
            List<T> result = lists.get(indexes);
            return result.get(newIndex);
        } else {
            return this.plainList.get(index);
        }
    }

    public boolean add(final T t) {
        hash.add(t);
        if (this.plainList == null) {
            addToIndexes(1);
            return this.lists.add(Collections.singletonList(t));
        } else {
            return this.plainList.add(t);
        }
    }

    public void add(final int index, final T t) {
        if (this.plainList == null) {
            throw new UnsupportedOperationException();
        } else {
            hash.add(t);
            this.plainList.add(index, t);
        }
    }

    public T set(final int index, final T t) {
        if (this.plainList == null) {
            throw new UnsupportedOperationException();
        } else {
            hash.add(t);
            return this.plainList.set(index, t);
        }
    }

    public int size() {
        return hash.size();
    }

    public Iterator<T> iterator() {
        if (this.plainList == null) {
            return new Iterator<T>() {
                private final Iterator<List<T>> listsIt = lists.iterator();
                private Iterator<T> currentListIt;

                public boolean hasNext() {
                    if (currentListIt == null) {
                        if (listsIt.hasNext()) {
                            currentListIt = listsIt.next().iterator();
                        } else {
                            return false;
                        }
                    }

                    // If the current sub-list iterator has no next, grab the
                    // next sub-list's iterator, and continue until either a
                    // sub-list iterator with a next is found (at which point,
                    // the while loop terminates) or no more sub-lists exist (in
                    // which case, return false).
                    while (!currentListIt.hasNext()) {
                        if (listsIt.hasNext()) {
                            currentListIt = listsIt.next().iterator();
                        } else {
                            return false;
                        }
                    }
                    return currentListIt.hasNext();
                }

                public T next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    } else {
                        return currentListIt.next();
                    }
                }

                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        } else {
            return this.plainList.iterator();
        }
    }

    public boolean isEmpty() {
        return hash.isEmpty();
    }

    public boolean contains(Object o) {
        return hash.contains(o);
    }

    public void clear() {
        this.plainList = null;
        this.lists.clear();
        this.hash.clear();
        this.indexesTo = null;
    }

    public int hashCode() {
        return this.hashCode;
    }
}

// End ConcatenableList.java

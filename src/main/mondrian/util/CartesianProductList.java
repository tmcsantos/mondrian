/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2015 Pentaho Corporation..  All rights reserved.
*/

package mondrian.util;

import mondrian.olap.Util;

import java.util.*;

/**
 * List that generates the cartesian product of its component lists.
 *
 * @author jhyde
 */
public class CartesianProductList<T>
    extends AbstractList<List<T>>
    implements RandomAccess
{
    private final List<List<T>> lists;
    private final int size;
    private final int[] offsets;

    public CartesianProductList(List<List<T>> lists) {
        super();
        this.lists = lists;
        this.offsets = new int[lists.size()];
        // calculate size
        int n = 1, ordinal=0;
        for (List<T> list : lists) {
            int size = list.size();
            n *= size;
            offsets[ordinal++] = size;
        }
        this.size = n;
    }

    @Override
    public List<T> get(int index) {
        final List<T> result = new ArrayList<T>();
        int size, idx, arity = lists.size();
        for (int i = arity; --i >= 0;) {
            size = offsets[i];
            idx = index % size;
            index /= size;
            final List<T> list = lists.get(i);
            result.add(0, list.get(idx));
        }
        return result;
    }

    @Override
    public int size() {
        return size;
    }

    public void getIntoArray(int index, Object[] a) {
        int n = 0, size, idx, arity = lists.size();
        for (int i = arity; --i >= 0;) {
            size = offsets[i];
            idx = index % size;
            index /= size;
            Object t = lists.get(i).get(idx);
            if (t instanceof List) {
                List tList = (List) t;
                int ar = tList.size();
                for (int j = ar; --j >= 0;) {
                    a[n++] = tList.get(j);
                }
            } else {
                a[n++] = t;
            }
        }
        reverse(a, n);
    }

    private void reverse(Object[] a, int size) {
        for (int i = 0, j = size - 1; i < j; ++i, --j) {
            Object t = a[i];
            a[i] = a[j];
            a[j] = t;
        }
    }

    @Override
    public Iterator<List<T>> iterator() {
        return new CartesianProductIterator();
    }

    /**
     * Iterator over a cartesian product list. It computes the list of elements
     * incrementally, so is a more efficient at generating the whole cartesian
     * product than calling {@link CartesianProductList#get} each time.
     */
    private class CartesianProductIterator implements Iterator<List<T>> {
        private final int[] offsets;
        private final T[] elements;
        private boolean hasNext;

        public CartesianProductIterator() {
            this.offsets = new int[lists.size()];
            //noinspection unchecked
            this.elements = (T[]) new Object[lists.size()];
            hasNext  = true;
            for (int i = 0; i < lists.size(); i++) {
                final List<T> list = lists.get(i);
                if (list.isEmpty()) {
                    hasNext = false;
                    return;
                }
                elements[i] = list.get(0);
            }
        }

        public boolean hasNext() {
            return hasNext;
        }

        public List<T> next() {
            @SuppressWarnings({"unchecked"})
            List<T> result = Util.flatListCopy(elements);
            moveToNext();
            return result;
        }

        private void moveToNext() {
            int ordinal = offsets.length;
            while (ordinal > 0) {
                --ordinal;
                ++offsets[ordinal];
                final List<T> list = lists.get(ordinal);
                if (offsets[ordinal] < list.size()) {
                    elements[ordinal] = list.get(offsets[ordinal]);
                    return;
                }
                offsets[ordinal] = 0;
                elements[ordinal] = list.get(0);
            }
            hasNext = false;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}

// End CartesianProductList.java

package com.code.crabs.common.util;

import java.util.Arrays;
import java.util.Collection;

/**
 * 只读型的List
 *
 * @param <E> 存储在集合中的元素类型
 * @author zhuchunlai
 * @version $Id: ReadonlyList.java, v1.0 2014/07/30 17:07 $
 */
public abstract class ReadonlyList<E> implements Iterable<E> {

    public static <E> ReadonlyList<E> newInstance(final E... elements) {
        if (elements == null) {
            throw new IllegalArgumentException("Argument [elements] is null.");
        }
        return new ReadonlyArrayList<E>(elements.clone());
    }

    @SuppressWarnings("unchecked")
    public static <E> ReadonlyList<E> newInstance(final Collection<E> elements) {
        if (elements == null) {
            throw new IllegalArgumentException("Argument [elements] is null.");
        }
        return new ReadonlyArrayList<E>(elements.toArray((E[]) (new Object[elements.size()])));
    }

    protected ReadonlyList() {
        // to do nothing.
    }

    public abstract boolean isEmpty();

    public abstract int size();

    public abstract E get(int index);

    public abstract Object[] toArray();

    public abstract E[] toArray(E[] array);

    private static final class ReadonlyArrayList<E> extends ReadonlyList<E> {

        ReadonlyArrayList(final E[] elements) {
            this.elements = elements;
        }

        private final E[] elements;

        @Override
        public final boolean isEmpty() {
            return this.size() == 0;
        }

        @Override
        public final int size() {
            return this.elements.length;
        }

        @Override
        public final E get(final int index) {
            return this.elements[index];
        }

        @Override
        public final Object[] toArray() {
            return this.elements.clone();
        }

        @SuppressWarnings("unchecked")
        @Override
        public final E[] toArray(final E[] array) {
            if (array == null) {
                throw new IllegalArgumentException("Argument [array] is null.");
            }
            final E[] elements = this.elements;
            final int elementCount = elements.length;
            if (array.length < elementCount) {
                return (E[]) Arrays.copyOf(elements, elementCount, array.getClass());
            } else {
                System.arraycopy(elements, 0, array, 0, elementCount);
                for (int i = elementCount; i < array.length; i++) {
                    array[i] = null;
                }
                return array;
            }
        }

        @Override
        public final java.util.Iterator<E> iterator() {
            return new Iterator();
        }

        @Override
        public final boolean equals(final Object object) {
            if (object != null && object instanceof ReadonlyArrayList) {
                if (object == this) {
                    return true;
                }
                final Object[] thisElements = this.elements;
                final Object[] thatElements = ((ReadonlyArrayList<?>) object).elements;
                if (thisElements.length == thatElements.length) {
                    for (int i = 0, elementCount = thisElements.length; i < elementCount; i++) {
                        if (thisElements[i] == null) {
                            if (thatElements[i] != null) {
                                return false;
                            }
                        } else if (!thisElements[i].equals(thatElements[i])) {
                            return false;
                        }
                    }
                    return true;
                }
            }
            return false;
        }

        private final class Iterator implements java.util.Iterator<E> {

            Iterator() {
                this.position = 0;
            }

            private int position;

            @Override
            public final boolean hasNext() {
                return this.position < ReadonlyArrayList.this.elements.length;
            }

            @Override
            public final E next() {
                return ReadonlyArrayList.this.elements[this.position++];
            }

            @Override
            public final void remove() {
                throw new UnsupportedOperationException("List is readonly.");
            }

        }

    }

}

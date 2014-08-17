package com.mcintyret.rdbmstm.collect;

import java.util.NoSuchElementException;

/*
Heavily influenced by Guava
 */
public abstract class AbstractIterator<T> implements PeekableIterator<T> {

    private T next;

    @Override
    public boolean hasNext() {
        if (next == null) {
            next = findNext();
        }
        return next != null;
    }

    protected abstract T findNext();

    @Override
    public T next() {
        if (hasNext()) {
            T ret = next;
            next = null;
            return ret;
        }
        throw new NoSuchElementException();
    }

    @Override
    public T peek() {
        if (hasNext()) {
            return next;
        }
        throw new NoSuchElementException();
    }
}

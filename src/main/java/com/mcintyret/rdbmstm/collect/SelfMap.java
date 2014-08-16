package com.mcintyret.rdbmstm.collect;

import static com.mcintyret.rdbmstm.collect.CollectUtils.toOrderedSet;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;


// TODO: worth having?
public class SelfMap<T> extends AbstractMap<T, T> {

    private final Set<T> self;

    public SelfMap(Iterable<T> self) {
        this.self = toOrderedSet(self);
    }

    @Override
    public boolean containsKey(Object key) {
        return self.contains(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return self.contains(value);
    }

    @Override
    public int size() {
        return self.size();
    }

    @Override
    public boolean isEmpty() {
        return self.isEmpty();
    }

    @Override
    public T get(Object key) {
        return self.contains(key) ? (T) key : null;
    }

    @Override
    public Set<T> keySet() {
        return self;
    }

    @Override
    public Collection<T> values() {
        return self;
    }

    @Override
    public Set<Entry<T, T>> entrySet() {
        return new AbstractSet<Entry<T, T>>() {
            @Override
            public Iterator<Entry<T, T>> iterator() {
                return new Iterator<Entry<T, T>>() {
                    private final Iterator<T> it = self.iterator();
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public Entry<T, T> next() {
                        T next = it.next();
                        return new Entry<T, T>() {
                            @Override
                            public T getKey() {
                                return next;
                            }

                            @Override
                            public T getValue() {
                                return next;
                            }

                            @Override
                            public T setValue(T value) {
                                throw new UnsupportedOperationException();
                            }
                        };
                    }
                };
            }

            @Override
            public int size() {
                return self.size();
            }
        };
    }
}

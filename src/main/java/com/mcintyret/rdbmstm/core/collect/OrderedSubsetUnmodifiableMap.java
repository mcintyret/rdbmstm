package com.mcintyret.rdbmstm.core.collect;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class OrderedSubsetUnmodifiableMap<K, V> implements Map<K, V> {

    private final Map<K, V> map;

    private final Set<K> keys;

    public OrderedSubsetUnmodifiableMap(Map<K, V> map, Iterable<K> keys) {
        this.map = map;
        Set<K> tempKeys = new LinkedHashSet<>();
        for (K key : keys) {
            tempKeys.add(key);
        }
        this.keys = Collections.unmodifiableSet(tempKeys);
    }


    @Override
    public int size() {
        return keys.size();
    }

    @Override
    public boolean isEmpty() {
        return keys.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return keys.contains(key);
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V get(Object key) {
        return keys.contains(key) ? map.get(key) : null;
    }

    @Override
    public V put(K key, V value) {
        throw new UnsupportedOperationException("Cannot modify OrderedSubsetUnmodifiableMap");
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException("Cannot modify OrderedSubsetUnmodifiableMap");
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException("Cannot modify OrderedSubsetUnmodifiableMap");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Cannot modify OrderedSubsetUnmodifiableMap");
    }

    @Override
    public Set<K> keySet() {
        return keys;
    }

    @Override
    public Collection<V> values() {
        return new AbstractCollection<V>() {
            @Override
            public Iterator<V> iterator() {
                final Iterator<Entry<K, V>> entryIt = entrySet().iterator();
                return new Iterator<V>() {
                    @Override
                    public boolean hasNext() {
                        return entryIt.hasNext();
                    }

                    @Override
                    public V next() {
                        return entryIt.next().getValue();
                    }
                };
            }

            @Override
            public int size() {
                return OrderedSubsetUnmodifiableMap.this.size();
            }
        };
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new AbstractSet<Entry<K, V>>() {
            @Override
            public Iterator<Entry<K, V>> iterator() {
                final Iterator<K> keyIt = keys.iterator();

                return new Iterator<Entry<K, V>>() {

                    Entry<K, V> next = null;

                    @Override
                    public boolean hasNext() {
                        if (next == null) {
                            if (keyIt.hasNext()) {
                                K key = keyIt.next();
                                next = new AbstractMap.SimpleImmutableEntry<>(key, map.get(key));
                                return true;
                            }
                            return false;
                        }
                        return true;
                    }

                    @Override
                    public Entry<K, V> next() {
                        if (hasNext()) {
                            Entry<K, V> ret = next;
                            next = null;
                            return ret;
                        }
                        throw new NoSuchElementException();
                    }
                };
            }

            @Override
            public int size() {
                return OrderedSubsetUnmodifiableMap.this.size();
            }
        };
    }

    @Override
    public int hashCode() {
        return entrySet().hashCode();
    }
}

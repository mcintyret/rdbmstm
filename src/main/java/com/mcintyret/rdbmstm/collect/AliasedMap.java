package com.mcintyret.rdbmstm.collect;

import static java.util.Collections.unmodifiableMap;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class AliasedMap<A, K, V> extends AbstractMap<A, V> {

    private final Map<A, K> aliasMap;

    private final Map<K, V> backingMap;

    public AliasedMap(Map<A, K> aliasMap, Map<K, V> backingMap) {
        this.aliasMap = unmodifiableMap(aliasMap);
        this.backingMap = unmodifiableMap(backingMap);
    }

    @Override
    public int size() {
        return aliasMap.size();
    }

    @Override
    public boolean isEmpty() {
        return aliasMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return aliasMap.containsKey(key);
    }

    @Override
    public V get(Object key) {
        K alias = aliasMap.get(key);
        return alias == null ? null : backingMap.get(alias);
    }

    @Override
    public V put(A key, V value) {
        throw new UnsupportedOperationException("AliasedMap is read-only");
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException("AliasedMap is read-only");
    }

    @Override
    public void putAll(Map<? extends A, ? extends V> m) {
        throw new UnsupportedOperationException("AliasedMap is read-only");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("AliasedMap is read-only");
    }

    @Override
    public Set<A> keySet() {
        return aliasMap.keySet();
    }

    @Override
    public Set<Entry<A, V>> entrySet() {
        return new AbstractSet<Entry<A, V>>() {
            @Override
            public Iterator<Entry<A, V>> iterator() {
                return new Iterator<Entry<A, V>>() {

                    private final Iterator<Entry<A, K>> aliasIt = aliasMap.entrySet().iterator();

                    @Override
                    public boolean hasNext() {
                        return aliasIt.hasNext();
                    }

                    @Override
                    public Entry<A, V> next() {
                        Entry<A, K> alias = aliasIt.next();
                        return new AbstractMap.SimpleImmutableEntry<>(alias.getKey(), backingMap.get(alias.getValue()));
                    }
                };
            }

            @Override
            public int size() {
                return aliasMap.size();
            }
        };
    }
}

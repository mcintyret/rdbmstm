package com.mcintyret.rdbmstm.collect;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class HashCachingMap<K, V> implements Map<K, V> {

    private final Map<K, V> map;

    private int hash;

    private boolean dirty = true;

    public HashCachingMap(Map<K, V> map) {
        this.map = map;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return map.get(key);
    }

    @Override
    public V put(K key, V value) {
        V existing = map.put(key, value);
        dirty = !Objects.equals(existing, value);
        return existing;
    }

    @Override
    public V remove(Object key) {
        boolean containsKey = map.containsKey(key);
        if (containsKey) {
            dirty = true;
            return map.remove(key);
        }
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        map.putAll(m);
        dirty = true;
    }

    @Override
    public void clear() {
        map.clear();
        dirty = true;
    }

    @Override
    public Set<K> keySet() {
        return Collections.unmodifiableSet(map.keySet());
    }

    @Override
    public Collection<V> values() {
        return Collections.unmodifiableCollection(map.values());
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return Collections.unmodifiableSet(map.entrySet());
    }

    @Override
    public int hashCode() {
        if (dirty) {
            hash = map.hashCode();
            dirty = false;
        }
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Map && map.equals(o);
    }
}

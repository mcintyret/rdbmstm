package com.mcintyret.rdbmstm.collect;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public final class CollectUtils {

    public static <T> Set<T> toOrderedSet(Iterable<T> c) {
        if (c instanceof Set) {
            return (Set<T>) c;
        } else {
            Set<T> set;
            if (c instanceof Collection) {
                set = new LinkedHashSet<>((Collection<T>) c);
            } else {
                set = new LinkedHashSet<>();
                for (T t : c) {
                    set.add(t);
                }
            }
            return set;
        }
    }

    public static <K, V> Map<K, V> toMap(Collection<K> keys, Collection<V> vals) {
        if (keys.size() != vals.size()) {
            throw new IllegalArgumentException("Different number of keys and values");
        }

        Map<K, V> map = new LinkedHashMap<>();

        Iterator<K> keyIt = keys.iterator();
        Iterator<V> valueIt = vals.iterator();

        while (keyIt.hasNext()) {
            K key;
            if (map.put((key = keyIt.next()), valueIt.next()) != null) {
                throw new IllegalArgumentException("Duplicate key: " + key);
            }
        }

        return map;
    }
}

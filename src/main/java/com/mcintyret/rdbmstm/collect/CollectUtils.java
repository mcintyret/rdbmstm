package com.mcintyret.rdbmstm.collect;

import java.util.Collection;
import java.util.LinkedHashSet;
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


}

package com.mcintyret.rdbmstm.collect;

import java.util.Iterator;
import java.util.Objects;

public final class Iterators {

    public static String toString(Iterator<?> iterator) {
        StringBuilder sb = new StringBuilder("[");
        while (iterator.hasNext()) {
            sb.append(String.valueOf(iterator.next()));
            if (iterator.hasNext()) {
                sb.append(", ");
            }
        }
        return sb.append("]").toString();
    }

    public static boolean equals(Iterator<?> a, Iterator<?> b) {
        while (a.hasNext()) {
            if (!b.hasNext()) {
                return false;
            }
            if (!Objects.equals(a.next(), b.next())) {
                return false;
            }
        }

        return !b.hasNext();
    }

    public static int hashCode(Iterator<?> a) {
        int hash = 31;
        while (a.hasNext()) {
            Object next = a.next();
            hash = 31 * hash + (next != null ? next.hashCode() : 0);
        }
        return hash;
    }

}

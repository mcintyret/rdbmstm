package com.mcintyret.rdbmstm.core.select;

import com.mcintyret.rdbmstm.core.Relation;

@FunctionalInterface
public interface Selector {

    Relation select(Relation in);

    default Selector chain(Selector next) {
        return in -> next.select(select(in));
    }

}

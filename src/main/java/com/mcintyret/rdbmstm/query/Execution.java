package com.mcintyret.rdbmstm.query;

import java.util.function.Supplier;

import com.mcintyret.rdbmstm.core.Relation;

public interface Execution {

    public static Execution forModification(Supplier<Modification> supplier) {
        return new Execution() {

            @Override
            public Modification executeModification() {
                return supplier.get();
            }

            @Override
            public Relation executeQuery() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static Execution forQuery(Supplier<Relation> supplier) {
        return new Execution() {
            @Override
            public Modification executeModification() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Relation executeQuery() {
                return supplier.get();
            }

            @Override
            public boolean isQuery() {
                return true;
            }
        };
    }

    Modification executeModification();

    Relation executeQuery();

    default boolean isQuery() {
        return false;
    }

}

package com.mcintyret.rdbmstm.query;

import com.mcintyret.rdbmstm.SqlException;
import com.mcintyret.rdbmstm.core.Database;

public interface Query {

    void execute(Database database) throws SqlException;

}

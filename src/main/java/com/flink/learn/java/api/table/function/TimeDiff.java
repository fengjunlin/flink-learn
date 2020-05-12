package com.flink.learn.java.api.table.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

public class TimeDiff extends ScalarFunction {

    public @DataTypeHint("BIGINT") long eval(Timestamp first, Timestamp second) {
        return java.time.Duration.between(first.toInstant(), second.toInstant()).toMillis();
    }
}

package com.flink.learn.java.api.table;

import com.flink.learn.java.api.table.function.TableFunc;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class TableFunctionExample {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<Tuple4<Integer, Long, String, Timestamp>> list = new ArrayList<>();
        list.add(Tuple4.of(1, 1L, "Jack#22", Timestamp.valueOf("2020-03-06 00:00:00")));
        list.add(Tuple4.of(2, 2L, "John#19", Timestamp.valueOf("2020-03-06 00:00:01")));
        list.add(Tuple4.of(3, 3L, "nosharp", Timestamp.valueOf("2020-03-06 00:00:03")));

        DataStream<Tuple4<Integer, Long, String, Timestamp>> stream = env
                .fromCollection(list)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer, Long, String, Timestamp>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple4<Integer, Long, String, Timestamp> element) {
                        return element.f3.getTime();
                    }
                });

        Table table = tEnv.fromDataStream(stream, "id, long, str, ts.rowtime");

        tEnv.createTemporaryView("input_table", table);

        tEnv.registerFunction("Func", new TableFunc());

        // input_table与LATERAL TABLE(Func(str))进行JOIN
        Table tableFunc = tEnv.sqlQuery("SELECT id, s FROM input_table, LATERAL TABLE(Func(str)) AS T(s)");
        DataStream<Row> tableFuncResult = tEnv.toAppendStream(tableFunc, Row.class);

        // input_table与LATERAL TABLE(Func(str))进行LEFT JOIN
        Table joinTableFunc = tEnv.sqlQuery("SELECT id, s FROM input_table LEFT JOIN LATERAL TABLE(Func(str)) AS T(s) ON TRUE");
        DataStream<Row> joinTableFuncResult = tEnv.toAppendStream(joinTableFunc, Row.class);
        joinTableFuncResult.print();

        env.execute("table api");
    }
}

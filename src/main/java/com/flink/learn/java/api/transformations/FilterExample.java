package com.flink.learn.java.api.transformations;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> dataStream = senv.fromElements(1, 2, -3, 0, 5, -9, 8);

        // 使用 -> 构造Lambda表达式
        DataStream<Integer> lambda = dataStream.filter ( input -> input > 0 );

        // 继承RichFilterFunction
        DataStream<Integer> richFunctionDataStream = dataStream.filter(new MyFilterFunction(2));
        richFunctionDataStream.print();

        senv.execute("basic filter transformation");

    }

    public static class MyFilterFunction extends RichFilterFunction<Integer> {

        // limit参数可以从外部传入
        private Integer limit;

        public MyFilterFunction(Integer limit) {
            this.limit = limit;
        }

        @Override
        public boolean filter(Integer input) {
            return input > this.limit;
        }

    }
}

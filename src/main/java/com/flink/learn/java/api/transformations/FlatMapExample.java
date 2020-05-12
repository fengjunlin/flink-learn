package com.flink.learn.java.api.transformations;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = senv.fromElements("Hello World", "Hello this is Flink");

        // split函数的输入为 "Hello World" 输出为 "Hello" 和 "World" 组成的列表 ["Hello", "World"]
        // flatMap将列表中每个元素提取出来
        // 最后输出为 ["Hello", "World", "Hello", "this", "is", "Flink"]
        DataStream<String> words = dataStream.flatMap (
                (String input, Collector<String> collector) -> {
                    for (String word : input.split(" ")) {
                        collector.collect(word);
                    }
                }).returns(Types.STRING);

        // 只对字符串数量大于15的句子进行处理
        // 使用匿名函数
        DataStream<String> longSentenceWords = dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String input, Collector<String> collector) throws Exception {
                if (input.length() > 15) {
                    for (String word: input.split(" "))
                        collector.collect(word);
                }
            }
        });

        // 实现FlatMapFunction类
        DataStream<String> functionStream = dataStream.flatMap(new WordSplitFlatMap(10));

        // 实现RichFlatMapFunction类
        DataStream<String> richFunctionStream = dataStream.flatMap(new WordSplitRichFlatMap(10));
        richFunctionStream.print();

        JobExecutionResult jobExecutionResult = senv.execute("basic flatMap transformation");

        // 执行结束后 获取累加器的结果
        Integer lines = jobExecutionResult.getAccumulatorResult("num-of-lines");
        System.out.println("num of lines: " + lines);
    }

    // 使用FlatMapFunction实现过滤逻辑，只对字符串长度大于 limit 的内容进行词频统计
    public static class WordSplitFlatMap implements FlatMapFunction<String, String> {

        private Integer limit;

        public WordSplitFlatMap(Integer limit) {
            this.limit = limit;
        }

        @Override
        public void flatMap(String input, Collector<String> collector) throws Exception {
            if (input.length() > limit) {
                for (String word: input.split(" "))
                    collector.collect(word);
            }
        }
    }

    // 实现RichFlatMapFunction类
    // 添加了累加器 Accumulator
    public static class WordSplitRichFlatMap extends RichFlatMapFunction<String, String> {

        private int limit;

        // 创建一个累加器
        private IntCounter numOfLines = new IntCounter(0);

        public WordSplitRichFlatMap(Integer limit) {
            this.limit = limit;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 在RuntimeContext中注册累加器
            getRuntimeContext().addAccumulator("num-of-lines", this.numOfLines);
        }

        @Override
        public void flatMap(String input, Collector<String> collector) throws Exception {

            // 运行过程中调用累加器
            this.numOfLines.add(1);

            if(input.length() > limit) {
                for (String word: input.split(" "))
                    collector.collect(word);
            }
        }
    }
}

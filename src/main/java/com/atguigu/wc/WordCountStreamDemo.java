package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @date 2024-01-22
 * DataStream实现WordCount 读文件（有界流）
 */
public class WordCountStreamDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取数据
        DataStreamSource<String> lineDS = env.readTextFile("input/words.txt");

        // 3. 处理数据  切分 转换 分组 聚合
        // 详细解读一下FlatMapFunction, 将一个类型的元素转换成另一个类型的元素，所以这个泛型是String 和 Tuple2
        SingleOutputStreamOperator<Tuple2<String, Integer>> WordAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> wordAndOne = Tuple2.of(word, 1);
                    collector.collect(wordAndOne);
                }
            }
        });
        // 3.2 分组
        KeyedStream<Tuple2<String, Integer>, String> WordAndOneG = WordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });
        // 3.3 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = WordAndOneG.sum(1);

        // 4. 输出数据
        sumDS.print();
        // 5. 执行： 类似 sparkStreaming最后的ssc.start()
        env.execute();
    }
}

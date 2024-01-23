package com.atguigu.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 无界流单词计数程序
 *
 * @date 2024-01-22
 */
public class WCSUnboundedDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);

        // 3. 处理
        socketDS.flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(Tuple2.of(word,1));
            }
        }).keyBy()

        // 4. 输出

        // 5. 执行
        env.execute();
    }
}

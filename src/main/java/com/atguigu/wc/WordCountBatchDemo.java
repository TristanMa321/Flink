package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import static java.lang.System.out;


/**
 * @date 2024-01-22
 */
public class WordCountBatchDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取数据：从文件中读取
        DataSource<String> lineDS = env.readTextFile("input/words.txt");
        // 切分 转换（word,1）
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 按照 空格 切分单词
                String[] words = s.split(" ");
                // 将单词转换为（word,1）
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                    // 调用collector向下游发送数据
                    out.collect(wordTuple2);
                }
            }
        });

        // 按照word分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneG = wordAndOne.groupBy(0);

        // 各分组内聚合
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneG.sum(1);
        // 输出
        sum.print();
    }
}

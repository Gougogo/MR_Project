package com.goumi.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordcountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable v = new IntWritable();

    protected void reduce(Text key, Iterable<IntWritable> values, Mapper.Context context)
            throws IOException, InterruptedException, IOException {
        // 1 汇总
        int sum = 0;
        for(IntWritable value :values){
            sum += value.get();
        }
        v.set(sum);


        // 2 写出
        context.write(key, v);
    }
}
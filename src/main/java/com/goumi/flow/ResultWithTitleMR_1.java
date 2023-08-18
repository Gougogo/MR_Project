package com.goumi.flow;

import com.goumi.utils.DeleteDir;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * @version 1.0
 * @auther GouMi
 */
public class ResultWithTitleMR_1 {
    // 自定义 Mapper 类
    public static class ResultMapper extends Mapper<Object, Text, NullWritable, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 在 Mapper 阶段直接输出原始数据，不做任何处理
            context.write(NullWritable.get(), value);
        }
    }

    // 自定义 Reducer 类
    public static class ResultReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        // 定义标题
        private static final Text TITLE = new Text("Title: ID, Name, Age");

        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 在 Reduce 阶段，首先输出标题
            context.write(NullWritable.get(), TITLE);
            System.out.println(key.hashCode());

            // 然后输出原始数据
            for (Text value : values) {
                context.write(NullWritable.get(), value);
                //System.out.println(value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String ouputPath = "d:/output/testFlowBeanTitle";

        //删除老目录
        File directory = new File(ouputPath);
        DeleteDir.deleteDirectory(directory);

        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[] { "d:/output/testFlowBean/part-r-00000", ouputPath};

        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 指定本程序的jar包所在的本地路径
        job.setJarByClass(ResultWithTitleMR_1.class);

        // 3 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(ResultMapper.class);
        job.setReducerClass(ResultReducer.class);

        // 4 指定mapper输出数据的kv类型
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(FlowBean.class);

        // 5 指定最终输出的数据的kv类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // 6 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

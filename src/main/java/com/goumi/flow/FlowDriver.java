package com.goumi.flow;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.DosFileAttributes;

import com.goumi.utils.DeleteDir;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowDriver {

    public static void main(String[] args)
            throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {

        String outputPathStr = "d:/output/testFlowBean";

        //删除老目录
        File directory = new File(outputPathStr);
        DeleteDir.deleteDirectory(directory);

        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[] { "d:/input/testFlowBean", outputPathStr};

        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowDriver.class);

        // 3 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 4 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 5 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 6 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 6 指定job的输出文件所在目录
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 8 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        /*boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);*/

        int status = job.waitForCompletion(true) ? 0 : 1;

        // 在作业完成后，在结果文件的第一行添加标题
        if (status == 0) {
            FileSystem fileSystem = FileSystem.get(configuration);
            fileSystem.setPermission(outputPath, new FsPermission("777"));
            addTitileToFile_2(configuration, outputPath);
        }

        System.exit(status);
    }

    // 添加标题到结果文件的第一行
    private static void addTitleToFile(Configuration conf, Path outputPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path outputFile = new Path(outputPath, "part-r-00000"); // 假设只有一个输出文件
        String title = "Title: ID, Name, Age";

        // 读取原始文件内容
        String content = fs.open(outputFile).readLine();
        System.out.println(content);

        // 将标题与原始内容拼接并写回文件
        Path outputFile2 = new Path(outputPath, "part-r-000001");
        FSDataOutputStream fsDataOutputStream = fs.create(outputFile2);
        fsDataOutputStream.writeBytes(title + "\n" + content);
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }

    private static void addTitileToFile_2(Configuration conf, Path outputPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path outputFile = new Path(outputPath, "part-r-00000"); // 假设只有一个输出文件
        String title = "Title: ID, Name, Age";

        FSDataInputStream open = fs.open(outputFile);
        byte[] bytes = new byte[1024];
        int readLen = 0;

        String sourceFileContent = title + "\n";

        while ((readLen = open.read(bytes)) != -1){
            sourceFileContent = sourceFileContent+new String(bytes, 0, readLen);
        }

        Path outputFile2 = new Path(outputPath, "part-r-00000");
        FSDataOutputStream fsDataOutputStream = fs.create(outputFile2);
        fsDataOutputStream.writeBytes(sourceFileContent);
        fsDataOutputStream.flush();
        fsDataOutputStream.close();

        //System.out.println(sourceFileContent);
    }

    // 添加标题到结果文件的第一行
    private static void addTitleToFile_1(Configuration conf, Path outputPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path outputFile = new Path("D:\\output\\testFlowBean\\part-r-00000"); // 假设只有一个输出文件
        String title = "Title: ID, Name, Age";

        // 读取原始文件内容
        String content = fs.open(outputFile).readLine();

        java.nio.file.Path filePath = Paths.get("D:\\output\\testFlowBean\\part-r-00000");
        try {
            DosFileAttributes attributes = Files.readAttributes(filePath, DosFileAttributes.class);
            System.out.println("File Permissions: " + attributes.isReadOnly());
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 将标题与原始内容拼接并写回文件
        boolean delete = fs.delete(outputFile, true);
        if (delete){
            System.out.println("删除成功");
        }else{
            System.out.println("删除失败");
        }
        /*FSDataOutputStream fsDataOutputStream = fs.create(outputFile);
        fsDataOutputStream.writeBytes(title + "\n" + content);
        fsDataOutputStream.flush();
        fsDataOutputStream.close();*/
        //fs.close();
    }
}
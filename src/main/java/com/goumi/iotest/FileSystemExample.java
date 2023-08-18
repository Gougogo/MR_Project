package com.goumi.iotest;

/**
 * @version 1.0
 * @auther GouMi
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class FileSystemExample {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(conf);
            // 使用文件系统对象进行文件操作
            Path filePath = new Path("D:\\output\\testFlowBean\\part-r-00000");

            boolean delete = fs.delete(filePath, true);
            if (delete){
                System.out.println("删除成功");
            }else{
                System.out.println("删除失败");
            }

            //fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

package com.goumi.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {
    @Test
    public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        // configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
        // FileSystem fs = FileSystem.get(configuration);

        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.146.133:9820"), configuration, "hadoop");

        // 2 创建目录
        fs.copyFromLocalFile(new Path("D:\\input\\1.txt"), new Path("/1108/daxian/banzhang"));

        // 3 关闭资源
        fs.close();
    }
}
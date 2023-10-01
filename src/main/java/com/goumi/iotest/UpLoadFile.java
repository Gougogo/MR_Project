package com.goumi.iotest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @version 1.0
 * @auther GouMi
 */
public class UpLoadFile {
    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop2-01:9820"), configuration);

        // 2 上传文件
        fs.copyFromLocalFile(new Path("e:/output/emp.txt"), new Path("/testdata/banzhang.txt"));

        // 3 关闭资源
        fs.close();

        System.out.println("over");
    }
}

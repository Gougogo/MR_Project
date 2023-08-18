package com.goumi.utils;

import java.io.File;

/**
 * @version 1.0
 * @auther GouMi
 */
public class DeleteDir {
    public static void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file); // 递归删除子目录
                    } else {
                        file.delete(); // 删除文件
                    }
                }
            }
            directory.delete(); // 删除空目录
            System.out.println("Directory deleted: " + directory.getAbsolutePath());
        } else {
            System.out.println("Directory does not exist: " + directory.getAbsolutePath());
        }
    }
}

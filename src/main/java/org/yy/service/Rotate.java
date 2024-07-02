package org.yy.service;

import java.io.FileWriter;
import java.io.IOException;

public class Rotate extends Thread {
    NormalStore normalStore;
    public Rotate (NormalStore normalStore) {
        this.normalStore = normalStore;
    }
    @Override
    public void run() {
        // 清空数据库文件
        normalStore.ClearDataBaseFile("YY-table");

        // 压缩日志文件
        normalStore.CompressLogFile();

        // 重写数据库文件
        try (FileWriter writer = new FileWriter("YY-table")) {
            for (String key : normalStore.getIndex().keySet()) {
                writer.write(key + "," + normalStore.Get(key) + "\r\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

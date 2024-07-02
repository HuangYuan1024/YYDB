/*
 *@Type SocketServerUsage.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 14:08
 * @version
 */
package org.yy.example;

import org.yy.controller.SocketServerController;
import org.yy.service.NormalStore;
import org.yy.service.Store;

import java.io.File;

public class SocketServerUsage {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 12345;
        String dataDir = "data" + File.separator;
        Store store = new NormalStore(dataDir);
        SocketServerController controller = new SocketServerController(host, port, store);
        controller.StartServer();

    }

}
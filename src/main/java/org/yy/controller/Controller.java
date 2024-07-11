/*
 *@Type Controller.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 12:17
 * @version
 */
package org.yy.controller;

import java.io.IOException;

public interface Controller {
    void Set(String key, String value);

    String Get(String key);

    void Remove(String key);

    void StartServer();
}

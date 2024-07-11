/*
 *@Type Store.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:05
 * @version
 */
package org.yy.service;

import java.io.Closeable;
import java.io.IOException;

public interface Store extends Closeable {
    void Set(String key, String value);

    String Get(String key);

    void Remove(String key);

}

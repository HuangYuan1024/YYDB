/*
 *@Type CommandPos.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:35
 * @version
 */
package org.yy.model.command;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class CommandPos {
    private int fileIndex;
    private int pos;
    private int len;

    public CommandPos(int pos, int len, int fileIndex) {
        this.pos = pos;
        this.len = len;
        this.fileIndex = fileIndex;
    }

    @Override
    public String toString() {
        return "CommandPos{" +
                "pos=" + pos +
                ", len=" + len +
                '}';
    }
}

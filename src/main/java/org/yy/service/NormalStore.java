/*
 *@Type NormalStore.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:07
 * @version
 */
package org.yy.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import org.yy.client.Client;
import org.yy.model.command.Command;
import org.yy.model.command.CommandPos;
import org.yy.model.command.RmCommand;
import org.yy.model.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yy.utils.CommandUtil;
import org.yy.utils.LoggerUtil;
import org.yy.utils.RandomAccessFileUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class NormalStore implements Store {

    public static final String TABLE = ".log";
    public static final String RW_MODE = "rw";
    public static final String NAME = "data";
    public static final String LEVEL = "level";
    public static final String REDOLOG = "redoLog";
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";

    /**
     * 内存表，类似缓存
     */
    private TreeMap<String, Command> memTable;

    /**
     * hash索引，存的是数据长度和偏移量
     * */
    @Getter
    private HashMap<String, CommandPos> index;

    /**
     * 数据目录
     */
    private final String dataDir;

    /**
     * 读写锁，支持多线程，并发安全写入
     */
    private final ReadWriteLock indexLock;

    /**
     * 暂存数据的日志句柄
     */
    private RandomAccessFile writerReader;

    /**
     * 持久化阈值
     */
    private final int storeThreshold = 5;

    public NormalStore(String dataDir) throws IOException {
        this.dataDir = dataDir;
        this.indexLock = new ReentrantReadWriteLock();
        this.memTable = new TreeMap<String, Command>();
        this.index = new HashMap<>();

        File file = new File(dataDir);
        if (!file.exists()) {
            LoggerUtil.info(LOGGER,logFormat, "NormalStore","dataDir isn't exist,creating...");
            file.mkdirs();
        }
        reloadIndex();
        RedoLog();
    }

    public String getFilePath(int level) {
        return this.dataDir + File.separator + LEVEL + level + File.separator + NAME + TABLE;
    }

    public String getRedologFilePath() {
        return dataDir + File.separator + REDOLOG + TABLE;
    }

    public String getNewFilePath() throws IOException {
        int LEVEL = getNumberOfLevels();
        File file = new File(getFilePath(0));

        return file.getAbsolutePath() + getNumberOfFiles(0);
    }

    public int getMaxFileLenth() throws IOException {
        FileInputStream fisp = new FileInputStream("p.properties");
        Properties prop = new Properties();
        prop.load(fisp);
        fisp.close();
        return Integer.parseInt(prop.getProperty("MAXFILELENTH"));
    }

    public int getNumberOfLevels() throws IOException {
        FileInputStream fisp = new FileInputStream("p.properties");
        Properties prop = new Properties();
        prop.load(fisp);
        fisp.close();
        return Integer.parseInt(prop.getProperty("LEVEL"));
    }

    public void setNumberOfLevels(int LEVEL) throws IOException {
        FileInputStream fisp = new FileInputStream("p.properties");
        Properties prop = new Properties();
        prop.load(fisp);
        fisp.close();

        prop.put("LEVEL",Integer.toString(LEVEL));
        FileOutputStream fosp = new FileOutputStream("p.properties");
        prop.store(fosp, null);
        fosp.close();
    }

    public int getNumberOfFiles(int level) {
        int accout = 0;
        try {
            Path directoryPath = Paths.get("data" + File.separator + "level" + level);
            final AtomicLong counter = new AtomicLong(0);
            Files.walkFileTree(directoryPath, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    counter.incrementAndGet();
                    return FileVisitResult.CONTINUE;
                }
            });
            accout = (int) counter.get();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return accout;
    }

    public void RedoLog() {
        try {
            RandomAccessFile file = new RandomAccessFile(getRedologFilePath(), RW_MODE);
            long len = file.length();
            long start = 0;
            file.seek(start);
            while (start < len) {
                int cmdLen = file.readInt();
                byte[] bytes = new byte[cmdLen];
                file.read(bytes);
                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                Command command = CommandUtil.jsonToCommand(value);
                start += 4;
                if (command != null) {
                    memTable.put(command.getKey(), command);
                    // 如果redo日志中记载该键值是被删除的，就将其从内存表里删去
                    if (command.getClass().equals(RmCommand.class)) {
                        memTable.remove(command.getKey());
                    }
                }
                start += cmdLen;
            }
            file.seek(file.length());
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 清空redoLog
        ClearDataBaseFile(getRedologFilePath());
    }

    public void reloadIndex() throws IOException {
        int LEVEL = getNumberOfLevels();
        // 由旧目录往新目录遍历
        for (int i = LEVEL; i >= 0; i--) {
            // 由旧文件往新文件遍历
            for (int j = 1; j <= getNumberOfFiles(i); j++) {
                String filePath = getFilePath(i) + j;
                reloadFile(filePath, i, j);
            }
        }
    }

    public void reloadFile(String filePath, int levelIndex, int fileIndex) {
        try {
            RandomAccessFile file = new RandomAccessFile(filePath, RW_MODE);
            long len = file.length();
            long start = 0;
            file.seek(start);
            while (start < len) {
                int cmdLen = file.readInt();
                byte[] bytes = new byte[cmdLen];
                file.read(bytes);
                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                Command command = CommandUtil.jsonToCommand(value);
                start += 4;
                if (command != null) {
                    CommandPos cmdPos = new CommandPos((int) start, cmdLen, levelIndex, fileIndex);
                    index.put(command.getKey(), cmdPos);
                    // 如果日志中记载该键值是被删除的，就将其从索引里删去
                    if (command.getClass().equals(RmCommand.class)) {
                        index.remove(command.getKey(), cmdPos);
                    }
                }
                start += cmdLen;
            }
            file.seek(file.length());
        } catch (Exception e) {
            e.printStackTrace();
        }
        LoggerUtil.debug(LOGGER, logFormat, "reload index: "+index.toString());
    }

    @Override
    public void Set(String key, String value) {
        try {
            SetCommand command = new SetCommand(key, value);
            byte[] commandBytes = command.toByte();
            // 加锁
            indexLock.writeLock().lock();
            // redoLog
            RandomAccessFileUtil.writeInt(dataDir + File.separator + NAME + TABLE, commandBytes.length);
            int posReDoLog = RandomAccessFileUtil.write(dataDir + File.separator + NAME + TABLE, commandBytes);
            // 先写内存表，内存表达到一定阀值再写进磁盘
            // 保存到memTable
            memTable.put(key, command);
            if (memTable.size() > storeThreshold) {
                for (String i :
                     memTable.keySet()) {
                    Command cmd = memTable.get(i);
                    String keyMem = cmd.getKey();
                    String valueMem = cmd.getValue();

                    byte[] cmdBytes;
                    if (valueMem != null) {
                        SetCommand setCommand = new SetCommand(keyMem, valueMem);
                        cmdBytes = setCommand.toByte();
                    } else {
                        RmCommand rmCommand = new RmCommand(keyMem);
                        cmdBytes = rmCommand.toByte();
                    }

                    RandomAccessFileUtil.writeInt(dataDir + File.separator + NAME + TABLE, cmdBytes.length);
                    int pos = RandomAccessFileUtil.write(dataDir + File.separator + NAME + TABLE, cmdBytes);
                    if (valueMem != null) {
                        // 添加索引
                        CommandPos cmdPos = new CommandPos(pos, cmdBytes.length, getNumberOfLevels(), getNumberOfFiles(getNumberOfLevels()));
                        index.put(key, cmdPos);
                    } else {
                        // 删除索引
                        index.remove(key);
                    }
                }
            }
            // rotate
            RotateDataBaseFile();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public String Get(String key) {
        try {
            indexLock.readLock().lock();

            // rotate
            RotateDataBaseFile();

            // 从索引中获取信息
            CommandPos cmdPos = index.get(key);
            if (cmdPos == null) {
                return null;
            }

            String fileIndex = String.valueOf(cmdPos.getFileIndex());
            int levelIndex = Integer.parseInt(String.valueOf(cmdPos.getLevelIndex()));
            byte[] commandBytes;
            if (fileIndex.equals("0")) {
                commandBytes = RandomAccessFileUtil.readByIndex(getRedologFilePath(), cmdPos.getPos(), cmdPos.getLen());
            } else {
                commandBytes = RandomAccessFileUtil.readByIndex(getFilePath(levelIndex) + fileIndex, cmdPos.getPos(), cmdPos.getLen());
            }

            JSONObject value = null;
            if (commandBytes != null) {
                value = JSONObject.parseObject(new String(commandBytes));
            }
            Command cmd = null;
            if (value != null) {
                cmd = CommandUtil.jsonToCommand(value);
            }
            if (cmd instanceof SetCommand) {
                return cmd.getValue();
            }
            if (cmd instanceof RmCommand) {
                return null;
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.readLock().unlock();
        }
        return null;
    }

    @Override
    public void Remove(String key) {
        try {
            RmCommand command = new RmCommand(key);
            byte[] commandBytes = command.toByte();
            // 加锁
            indexLock.writeLock().lock();
            // redoLog
            RandomAccessFileUtil.writeInt(getRedologFilePath(), commandBytes.length);
            RandomAccessFileUtil.write(getRedologFilePath(), commandBytes);
            // 先写内存表，内存表达到一定阀值再写进磁盘
            // 保存到memTable
            memTable.put(key, command);
            if (memTable.size() > storeThreshold) {
                RotateDataBaseFile();
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public void close() {}

    public void CreateNewDirectory(int level) {
        // 指定要检查的目录路径
        String directoryPath = this.dataDir + File.separator + LEVEL + level;

        // 使用File类创建一个表示该目录的File对象
        File directory = new File(directoryPath);

        // 使用exists()方法判断目录是否存在
        if (!directory.exists()) {
            // 不存在就创建目录
            File dir = new File(directoryPath);

            // 尝试创建目录
            boolean success = dir.mkdir();

            if (success) {
                System.out.println("目录创建成功");
            } else {
                System.out.println("目录创建失败");
            }
        }
    }

    public void ClearDataBaseFile(String filePath) {
        try (FileWriter writer = new FileWriter(filePath)) {
            // 不写入任何内容，直接关闭writer
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void RotateDataBaseFile() throws IOException {
        for (String i :
                memTable.keySet()) {
            File file = new File(getNewFilePath());
            if (file.length() > getMaxFileLenth()) {
                // 如果level0目录中的文件即将超过3个就开始合并SSTable
                if (getNumberOfFiles(0) < 3) {
                    // 创建新文件（空的新文件）
                    ClearDataBaseFile(getFilePath(0) + getNumberOfFiles(0) + 1);
                } else {
                    // 判断是否需要合并SSTable
                    DetermineMerge();
                    // 创建新文件（空的新文件）
                    ClearDataBaseFile(getFilePath(0) + getNumberOfFiles(0) + 1);
                }
            }

            // 内存表存储的命令
            Command cmd = memTable.get(i);
            String keyMem = cmd.getKey();
            String valueMem = cmd.getValue();

            byte[] cmdBytes;
            if (valueMem != null) {
                SetCommand setCommand = new SetCommand(keyMem, valueMem);
                cmdBytes = setCommand.toByte();
            } else {
                RmCommand rmCommand = new RmCommand(keyMem);
                cmdBytes = rmCommand.toByte();
            }

            // 将MemTable写入SSTable
            RandomAccessFileUtil.writeInt(getNewFilePath(), cmdBytes.length);
            int pos = RandomAccessFileUtil.write(getNewFilePath(), cmdBytes);

            if (valueMem != null) {
                // 添加索引
                CommandPos cmdPos = new CommandPos(pos, cmdBytes.length, getNumberOfLevels(), getNumberOfFiles(getNumberOfLevels()));
                index.put(keyMem, cmdPos);
            } else {
                // 删除索引
                index.remove(keyMem);
            }
        }

        // 清空内存表
        memTable.clear();
    }

    public void DetermineMerge() throws IOException {
        for (int i = 0; i < getNumberOfLevels(); i++) {
            if (getNumberOfFiles(i) >= 3) {
                String filePath1 = getFilePath(i) + 1;
                String filePath2 = getFilePath(i) + 2;
                String filePath3 = getFilePath(i) + 3;

                // 判断是否要创建新目录（level）
                CreateNewDirectory(i + 1);

                // 合并SSTable
                MergeSSTable(filePath1, filePath2, filePath3, i + 1);
            }
        }
    }

    // 合并
    public void MergeSSTable(String filePath1, String filePath2, String filePath3, int level) {
        ArrayList<byte[]> data = new ArrayList<>();
        HashSet<String> keys = new HashSet<>();

        // 文件数据去重
        RemoveDuplicateData(filePath1, data, keys);
        RemoveDuplicateData(filePath2, data, keys);
        RemoveDuplicateData(filePath3, data, keys);

        String newFilePath = getFilePath(level) + getNumberOfFiles(level) + 1;

        // 创建新文件（空的新文件）
        ClearDataBaseFile(newFilePath);

        // 往新文件里写数据
        for (byte[] line :
             data) {
            RandomAccessFileUtil.write(newFilePath, line);
        }
    }

    // 去重
    public void RemoveDuplicateData(String filePath, ArrayList<byte[]> data, HashSet<String> keys) {
        try {
            RandomAccessFile file = new RandomAccessFile(filePath, RW_MODE);
            long len = file.length();
            long start = 0;
            file.seek(start);
            while (start < len) {
                int cmdLen = file.readInt();
                byte[] bytes = new byte[cmdLen];
                file.read(bytes);
                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                Command command = CommandUtil.jsonToCommand(value);
                start += 4;
                if (command != null) {
                    // 如果日志中记载该键值是被删除的，就跳过
                    if (command.getClass().equals(RmCommand.class)) {
                        continue;
                    } else {
                        String key = command.getKey();
                        // 如果键不重复，就添加数据
                        if (!keys.contains(key)) {
                            data.add(bytes);
                        }
                    }
                }
                start += cmdLen;
            }
            file.seek(file.length());
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 删除文件
        DeleteFile(filePath);
    }

    public void DeleteFile(String filePath) {
        // 创建一个File对象，指向你想要删除的文件
        File file = new File(filePath);

        // 检查文件是否存在
        if (file.exists()) {
            // 尝试删除文件
            boolean isDeleted = file.delete();

            // 检查文件是否被删除
            if (isDeleted) {
                System.out.println("文件已被删除！");
            } else {
                System.out.println("文件删除失败！");
            }
        } else {
            System.out.println("文件不存在！");
        }
    }

}

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
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.GZIPOutputStream;

public class NormalStore implements Store {

    public static final String TABLE = ".log";
    public static final String RW_MODE = "rw";
    public static final String NAME = "data";
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

    /**
     * 计数
     */
    private int storeOperateNumber;

    public NormalStore(String dataDir) {
        this.dataDir = dataDir;
        this.indexLock = new ReentrantReadWriteLock();
        this.memTable = new TreeMap<String, Command>();
        this.index = new HashMap<>();
        storeOperateNumber = 0;

        File file = new File(dataDir);
        if (!file.exists()) {
            LoggerUtil.info(LOGGER,logFormat, "NormalStore","dataDir isn't exist,creating...");
            file.mkdirs();
        }
        this.reloadIndex();
        this.RotateDataBaseFile();
    }

    public String genFilePath() {
        return this.dataDir + File.separator + NAME + TABLE;
    }

    public String genNewFilePath() {
        return this.dataDir + File.separator + NAME + "0" + TABLE;
    }

    public String genScratchFilePath() {
        return this.dataDir + File.separator + "Scratch.log";
    }

    public void reloadIndex() {
        try {
            RandomAccessFile file = new RandomAccessFile(this.genFilePath(), RW_MODE);
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
                    CommandPos cmdPos = new CommandPos((int) start, cmdLen);
                    index.put(command.getKey(), cmdPos);
                    // 如果日志中记载该键值是被删除的，就将其从内存里删去
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

    public void reloadScratchIndex() {
        try {
            RandomAccessFile file = new RandomAccessFile(this.genScratchFilePath(), RW_MODE);
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
                    CommandPos cmdPos = new CommandPos((int) start, cmdLen);
                    index.put(command.getKey(), cmdPos);
                    // 如果日志中记载该键值是被删除的，就将其从内存里删去
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
            // 先写内存表，内存表达到一定阀值再写进磁盘
            if (storeOperateNumber >= storeThreshold) {
                storeOperateNumber = 0;

                RotateDataBaseFile();
            }
            // 写table（wal）文件
            RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);
            int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);
            // 保存到memTable
            // 添加索引
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
            index.put(key, cmdPos);
            storeOperateNumber++;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
//        try {
//            FilCompress(new File(this.genFilePath()));
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }

    @Override
    public String Get(String key) {
        try {
            indexLock.readLock().lock();
            // 从索引中获取信息
            CommandPos cmdPos = index.get(key);
            if (cmdPos == null) {
                return null;
            }
            byte[] commandBytes = RandomAccessFileUtil.readByIndex(this.genFilePath(), cmdPos.getPos(), cmdPos.getLen());

            JSONObject value = null;
            if (commandBytes != null) {
                value = JSONObject.parseObject(new String(commandBytes));
            }
            Command cmd = null;
            if (value != null) {
                cmd = CommandUtil.jsonToCommand(value);
            }
            if (cmd instanceof SetCommand) {
                return ((SetCommand) cmd).getValue();
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

    public String ReGet(String key) {
        try {
            indexLock.readLock().lock();
            // 从索引中获取信息
            CommandPos cmdPos = index.get(key);
            if (cmdPos == null) {
                return null;
            }
            byte[] commandBytes = RandomAccessFileUtil.readByIndex(genScratchFilePath(), cmdPos.getPos(), cmdPos.getLen());

            JSONObject value = null;
            if (commandBytes != null) {
                value = JSONObject.parseObject(new String(commandBytes));
            }
            Command cmd = null;
            if (value != null) {
                cmd = CommandUtil.jsonToCommand(value);
            }
            if (cmd instanceof SetCommand) {
                return ((SetCommand) cmd).getValue();
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

            // 先写内存表，内存表达到一定阀值再写进磁盘
            if (storeOperateNumber >= storeThreshold) {
                storeOperateNumber = 0;

//                RotateDataBaseFile();

                RotateDataBaseFile();
            }
            // 写table（wal）文件
            RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);
            RandomAccessFileUtil.write(this.genFilePath(), commandBytes);
            // 保存到memTable
            index.remove(key);
            storeOperateNumber++;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
//        try {
//            FilCompress(new File(this.genFilePath()));
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }

    @Override
    public void ReDoLog() {
        reloadIndex();
        RotateDataBaseFile();
    }

    @Override
    public void close() {}

    public void ClearDataBaseFile(String filePath) {
        try (FileWriter writer = new FileWriter(filePath)) {
            // 不写入任何内容，直接关闭writer
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void ScratchLogSet(String key, String value) {
        try {
            SetCommand command = new SetCommand(key, value);
            byte[] commandBytes = command.toByte();
            // 加锁
            indexLock.writeLock().lock();
            // 写table（wal）文件
            RandomAccessFileUtil.writeInt(genScratchFilePath(), commandBytes.length);
            RandomAccessFileUtil.write(genScratchFilePath(), commandBytes);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    public void RenameFile(String oldFileName, String newFileName) {
        // 假设我们有一个名为"oldName.txt"的文件
        File oldFile = new File(oldFileName);

        // 我们想要将其重命名为"newName.txt"
        File newFile = new File(newFileName);

        // 尝试重命名文件
        boolean success = oldFile.renameTo(newFile);

        // 检查操作是否成功
        if (success) {
            System.out.println("文件重命名成功！");
        } else {
            System.out.println("文件重命名失败，请确保没有其他程序正在使用该文件。");
        }
    }

    public void ReSet(String key, String value) {
        try {
            SetCommand command = new SetCommand(key, value);
            byte[] commandBytes = command.toByte();
            // 加锁
            indexLock.writeLock().lock();
            // 写table（wal）文件
            RandomAccessFileUtil.writeInt(genFilePath(), commandBytes.length);
            RandomAccessFileUtil.write(genFilePath(), commandBytes);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    public void CompressLogFile() {
        // 索引压缩后写到临时文件
        for (String key : index.keySet()) {
            ScratchLogSet(key, Get(key));
        }

        // 从临时文件加载索引
        reloadScratchIndex();

        ClearDataBaseFile(genFilePath());

        // 重写日志文件
        for (String key : index.keySet()) {
            ReSet(key, ReGet(key));
        }

        ClearDataBaseFile(genScratchFilePath());

        reloadIndex();

//        ArrayList<String> arrayList = new ArrayList<>();
//        HashSet<String> hashSet = new HashSet<>();
//
//        try (Scanner scanner = new Scanner(new File(this.genFilePath()))) {
//            // 按行读取
//            while (scanner.hasNextLine()) {
//                String line = scanner.nextLine();
//                if (!hashSet.contains(line)) {
//                    arrayList.add(line);
//                    hashSet.add(line);
//                } else if (arrayList.size() > 1){
//                    if (!arrayList.get(arrayList.size() - 1).equals(line)) {
//                        arrayList.add(line);
//                    }
//                }
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//
//        ClearDataBaseFile(this.genFilePath());
//
//        try (FileWriter writer = new FileWriter(this.genFilePath())) {
//            for (String line :
//                arrayList) {
//                writer.write(line + "\r\n");
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        index.clear();
//
//        reloadIndex();
    }

    public void RotateDataBaseFile() {
//        Rotate rotate = new Rotate(normalStore);
//        rotate.start();


        // 压缩日志文件
        CompressLogFile();

//        // 重写数据库文件
//        try (FileWriter writer = new FileWriter("Scratch.log")) {
//            for (String key : index.keySet()) {
//                writer.write(key + "," + Get(key) + "\r\n");
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public void FilCompress(File file) throws IOException {
        FileInputStream fisp = new FileInputStream("p.properties");
        Properties prop = new Properties();
        prop.load(fisp);
        fisp.close();
        int MAXFILELENTH = Integer.parseInt(prop.getProperty("MAXFILELENTH"));
        int TIMES = Integer.parseInt(prop.getProperty("TIMES"));
        if(file.length() > MAXFILELENTH){
            TIMES++;
            prop.put("TIMES",Integer.toString(TIMES));
            FileOutputStream fosp = new FileOutputStream("p.properties");
            prop.store(fosp, null);
            fosp.close();
            try(FileInputStream fis = new FileInputStream(file);
            FileOutputStream fos = new FileOutputStream(file.getAbsolutePath() + TIMES + ".gz");
            GZIPOutputStream gzos = new GZIPOutputStream(fos)){
                indexLock.writeLock().lock();
                byte[] bytes = new byte[MAXFILELENTH];
                int len;
                while ((len = fis.read(bytes)) != -1) {
                    gzos.write(bytes, 0, len);
                }
                ClearDataBaseFile(file.getAbsolutePath());
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                indexLock.writeLock().unlock();
            }
        }
    }
}

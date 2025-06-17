package plugin.input;

import anno.Input;
import cn.hutool.core.io.FileUtil;
import core.Channel;
import core.ThreadPoolManger;
import core.flowdata.Row;
import core.flowdata.RowSetTable;
import core.intf.IInput;
import lombok.var;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

@Input(type = "csv") // 与Factory扫描的注解类型一致
public class CsvInput implements IInput {
    private Map<String, Object> config; // 保存初始化配置
    private char delimiter = ',';       // 默认分隔符
    private char quoteChar = '"';       // 默认引号字符
    private boolean hasHeader = true;   // 默认有表头
    private String filePath;
    private long fileBlockSize = 4 * 1024;   // 单位Byte, 默认4KB
    
    @Override
    public void init(Map<String, Object> cfg) {
        // 初始化配置并校验
        this.config = cfg;
        this.filePath = (String) cfg.get("filePath");
        String delimiterStr = (String) cfg.getOrDefault("delimiter", ",");
        String quoteStr = (String) cfg.getOrDefault("quoteChar", "\"");
        this.hasHeader = (Boolean) cfg.getOrDefault("hasHeader", true);
        
        // 参数校验
        if (filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException("缺少文件路径！");
        }
        if (delimiterStr.length() != 1) {
            throw new IllegalArgumentException("分隔符必须是一个字符");
        }
        if (quoteStr.length() != 1) {
            throw new IllegalArgumentException("引号字符必须是一个字符");
        }
        this.delimiter = delimiterStr.charAt(0);
        this.quoteChar = quoteStr.charAt(0);
    }
    
    @Override
    public void start(Channel output) throws Exception {
        File file = new File(filePath);
        long fileLength = file.length();
        if (fileLength < fileBlockSize) {
            var lines = FileUtil.readLines(file, StandardCharsets.UTF_8);
            int dataStart = hasHeader ? 1 : 0;
            if (lines.isEmpty()) {
                throw new IllegalArgumentException("CSV文件为空");
            }
            for (int i = dataStart; i < lines.size(); i++) {
                String line = lines.get(i)
                                   .trim();
                if (line.isEmpty()) continue;
                
                Row row = parseCsvLine(line);
                output.publish(row);
                System.out.println("[CsvInput] 数据行已发送: " + row);
            }
            output.close();
            return;
        }
        ExecutorService threadPool = ThreadPoolManger.getThreadPool();
        RandomAccessFile reader = new RandomAccessFile(file, "r");
        List<Future> futureList = new ArrayList<>();
        // 设置列名
        long headerEnd = this.getNextCRLocationNoEOF(reader);
        byte[] headerBytes = new byte[(int) headerEnd];
        new FileInputStream(file).read(headerBytes);
        String headerStr = new String(headerBytes);
        Row row = parseCsvLine(headerStr);
        output.setHeader(row.RowChangeTable());
        // 向上取整获取分块数量
        long blockSize = (fileLength + fileBlockSize + 1) / fileBlockSize;
        // 处理除了最后一个块
        for (long i = 0; i < blockSize - 1; i++) {
            long offer = i * this.fileBlockSize;
            Future<?> submit = threadPool.submit(() -> {
                try {
                    RandomAccessFile threadReader = new RandomAccessFile(file, "r");
                    threadReader.seek(offer);
                    long begin = offer;
                    long end = offer + fileBlockSize;
                    begin = this.getPrevCRLocation(threadReader);
                    threadReader.seek(end);
                    end = this.getPrevCRLocation(threadReader);
                    System.out.printf("开始字节:%d, 结束字节:%d\n", begin, end);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            futureList.add(submit);
        }
        Future<?> last = threadPool.submit(() -> {
            try {
                RandomAccessFile threadReader = new RandomAccessFile(file, "r");
                long offer = (blockSize - 1) * fileBlockSize;
                threadReader.seek(offer);
                long begin = offer;
                long end = Math.min(offer + fileBlockSize, threadReader.length());
                begin = this.getPrevCRLocation(threadReader);
                threadReader.seek(end);
                end = this.getPrevCRLocation(threadReader);
                System.out.printf("end: 开始字节:%d, 结束字节:%d\n", begin, end);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        futureList.add(last);
        futureList.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        output.close();
        reader.close();
    }
    
    // 返回的是当前reader的文件指针的下一个\n的文件字节偏移量
    // 如果文件指针已经指向\n,那么就会返回当前这个\n的位置
    /// 该方法一定不能用于后面没有回车的情况只能用于中间块, 也就是不能判断是否读到文件尾,如果需要判断是否读完文件,请使用有判断的查找方法
    public long getNextCRLocationNoEOF(RandomAccessFile reader) throws IOException {
        byte[] bytes = new byte[16];
        while (true) {
            reader.read(bytes);
            reader.seek(reader.getFilePointer() - 16);
            for (int offer = 0; offer < bytes.length; offer++) {
                if ((char) bytes[offer] == '\n') {
                    long location = reader.getFilePointer() + offer;
                    reader.seek((reader.getFilePointer() + offer + 1));
                    return location;
                }
            }
            reader.seek(reader.getFilePointer() + 16);
        }
    }
    
    public long getNextCRLocationHaveEOF(RandomAccessFile reader) throws Exception {
        byte[] bytes = new byte[16];
        while (true) {
            int readL = reader.read(bytes);
            if (readL < 16) {
                return reader.length();
            }
            reader.seek(reader.getFilePointer() - 16);
            for (int offer = 0; offer < bytes.length; offer++) {
                if ((char) bytes[offer] == '\n') {
                    long location = reader.getFilePointer() + offer;
                    // 指向回车的下一个字节
                    reader.seek((reader.getFilePointer() + offer + 1));
                    return location;
                }
            }
            reader.seek(reader.getFilePointer() + 16);
        }
    }
    
    public long getPrevCRLocation(RandomAccessFile reader) throws Exception {
        long location = reader.getFilePointer();
        byte[] bytes = new byte[16];
        if (reader.getFilePointer() == 0) {
            return 0;
        }
        while (true) {
            reader.seek(reader.getFilePointer() - 16);
            reader.read(bytes);
            reader.seek(reader.getFilePointer() - 16);
            for (int offer = 0; offer < bytes.length; offer++) {
                if ((char) bytes[offer] == '\n') {
                    // 还原文件指针
//                    location = reader.getFilePointer();
                    reader.seek(location);
                    return reader.getFilePointer() + offer;
                }
            }
            reader.seek(reader.getFilePointer() + 32);
        }
    }
    
    // 解析单行CSV数据
    private Row parseCsvLine(String line) {
        Row row = new Row();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == quoteChar) {
                if (i + 1 < line.length() && line.charAt(i + 1) == quoteChar) {
                    current.append(quoteChar);
                    i++; // 跳过下一个引号
                } else {
                    inQuotes = ! inQuotes;
                }
            } else if (c == delimiter && ! inQuotes) {
                row.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        row.add(current.toString()); // 添加最后一个字段
        
        if (inQuotes) {
            throw new IllegalArgumentException("CSV行包含未闭合的引号: " + line);
        }
//        System.out.println(row);
        return row;
    }
}

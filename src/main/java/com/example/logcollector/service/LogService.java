package com.example.logcollector.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.annotation.PreDestroy;

import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.example.logcollector.model.LogEntry;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * 日志服务类，负责处理实时日志和批量日志的收集与处理
 * 使用 Disruptor 框架实现高性能的日志处理
 */
@Service
public class LogService {
    // 核心组件
    private final LogWriter logWriter;                    // 日志写入器
    private final RingBuffer<LogEntry> ringBuffer;        // Disruptor环形缓冲区
    private final ScheduledExecutorService scheduler;     // 定时任务执行器
    private final List<LogEntry> batchBuffer;            // 批量处理缓冲区
    private final Object batchLock = new Object();       // 批处理同步锁
    
    // 常量配置
    private static final int RING_BUFFER_SIZE = 1024 * 64;  // 环形缓冲区大小，必须是2的幂
    private static final DateTimeFormatter DATE_TIME_FORMATTER = 
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");  // 日期格式化器

    public LogService(LogWriter logWriter) {
        this.logWriter = logWriter;
        this.batchBuffer = new ArrayList<>();
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.ringBuffer = createRingBuffer();

        scheduler.scheduleAtFixedRate(this::processBatchBuffer,
                10, 10, TimeUnit.SECONDS);
    }

    /**
     * 创建并配置用于日志处理的环形缓冲区
     * @return 配置好的RingBuffer实例
     */
    private RingBuffer<LogEntry> createRingBuffer() {
        // 创建自定义线程工厂，为日志处理线程指定名称
        ThreadFactory threadFactory = r -> {
            Thread thread = new Thread(r);
            thread.setName("LogProcessor");
            return thread;
        };

        // 初始化Disruptor，配置环形缓冲区
        Disruptor<LogEntry> disruptor = new Disruptor<>(
                LogEntry::new,                // 事件工厂，用于创建LogEntry实例
                RING_BUFFER_SIZE,            // 环形缓冲区大小
                threadFactory,               // 自定义线程工厂
                ProducerType.MULTI,          // 多生产者模式
                new BlockingWaitStrategy()   // 使用阻塞等待策略
        );

        // 配置事件处理器，定义如何处理日志条目
        disruptor.handleEventsWith((event, sequence, endOfBatch) ->
                logWriter.writeLog(event));

        // 启动Disruptor
        disruptor.start();
        // 返回配置好的环形缓冲区
        return disruptor.getRingBuffer();
    }

    /**
     * 处理实时日志条目
     * 计算处理时间和延迟时间，并将日志放入 RingBuffer
     */
    public void processRealtimeLog(LogEntry logEntry) {
        // 获取 RingBuffer 中的下一个可用序号
        long sequence = ringBuffer.next();
        try {
            // 获取该序号对应的事件对象（在 RingBuffer 中的槽位）
            LogEntry event = ringBuffer.get(sequence);
            // 使用 Spring 的 BeanUtils 工具类，将输入的 logEntry 的所有属性复制到事件对象中
            BeanUtils.copyProperties(logEntry, event);
        } finally {
            // 发布事件，通知消费者可以消费这个序号的事件了
            // 放在 finally 块中确保即使发生异常也能正确发布，避免 RingBuffer 死锁
            ringBuffer.publish(sequence);
        }
    }

    /**
     * 处理批量日志文件（ZIP格式）
     * 包含重试机制，最多重试3次
     */
    public void processBatchLogs(MultipartFile zipFile) {
        int retryCount = 0;
        while (retryCount < 3) {
            try {
                try (ZipInputStream zis = new ZipInputStream(zipFile.getInputStream())) {
                    ZipEntry entry;
                    while ((entry = zis.getNextEntry()) != null) {
                        processZipEntry(zis);
                    }
                }
                return; // Success, exit the retry loop
            } catch (IOException e) {
                retryCount++;
                if (retryCount >= 3) {
                    System.err.println("Failed to process batch logs after 3 attempts: " + e.getMessage());
                    throw new RuntimeException("Failed to process batch logs", e);
                }
                try {
                    Thread.sleep(1000 * retryCount);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during retry wait", ie);
                }
            }
        }
    }

    private void processZipEntry(ZipInputStream zis) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(zis));
        String line;
        while ((line = reader.readLine()) != null) {
            LogEntry logEntry = parseLogEntry(line);
            if (logEntry != null) {
                processRealtimeLog(logEntry);
            }
        }
    }

    /**
     * 解析单行日志文本
     * 格式：ID|IP|时间|名称|随机数
     */
    private LogEntry parseLogEntry(String line) {
        try {
            String[] parts = line.split("\\|");
            if (parts.length >= 5) {
                LogEntry entry = new LogEntry();
                entry.setId(parts[0]);
                entry.setIp(parts[1]);
                entry.setEventTime(LocalDateTime.parse(parts[2], DATE_TIME_FORMATTER));
                entry.setName(parts[3]);
                entry.setRandomNumber(Integer.parseInt(parts[4]));
                return entry;
            }
        } catch (Exception e) {
            System.err.println("Failed to parse log entry: " + line);
            e.printStackTrace();
        }
        return null;
    }

    private void addToBatchBuffer(LogEntry logEntry) {
        synchronized (batchLock) {
            batchBuffer.add(logEntry);
            if (batchBuffer.size() >= 5) {
                processBatchBuffer();
            }
        }
    }

    private void processBatchBuffer() {
        synchronized (batchLock) {
            if (!batchBuffer.isEmpty()) {
                logWriter.writeBatch(new ArrayList<>(batchBuffer));
                batchBuffer.clear();
            }
        }
    }
    @PreDestroy
    public void cleanup() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
    }
}

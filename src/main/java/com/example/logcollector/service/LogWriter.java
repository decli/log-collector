package com.example.logcollector.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.example.logcollector.model.LogEntry;

/**
 * 日志写入器组件
 * 负责将日志条目异步写入文件系统
 * 支持按小时自动分割日志文件
 */
@Component
public class LogWriter {
    // 文件系统相关常量
    private static final String LOG_DIR = "logs";                 // 日志目录
    private static final String LOG_FILE_PREFIX = "client_";      // 日志文件前缀
    private static final String LOG_FILE_SUFFIX = ".log";         // 日志文件后缀

    private final ExecutorService writeExecutor;                  // 写入线程池
    private Path currentLogFile;                                 // 当前日志文件路径
    private int currentHour = -1;                               // 当前小时，用于文件分割

    public LogWriter() {
        this.writeExecutor = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors()
        );
        createLogDirectory();
    }

    /**
     * 异步写入单条日志
     * 使用 CompletableFuture 实现异步操作
     */
    public void writeLog(LogEntry logEntry) {
        CompletableFuture.runAsync(() -> {
            try {
                Path logFile = getOrCreateLogFile();
                String logLine = formatLogEntry(logEntry);
                Files.write(logFile,
                        (logLine + System.lineSeparator()).getBytes(),
                        StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, writeExecutor);
    }

    /**
     * 异步批量写入日志
     * 将多条日志一次性写入文件
     */
    public void writeBatch(List<LogEntry> entries) {
        CompletableFuture.runAsync(() -> {
            try {
                Path logFile = getOrCreateLogFile();List<String> lines = entries.stream()
                        .map(this::formatLogEntry)
                        .collect(Collectors.toList());
                Files.write(logFile, lines,
                        StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, writeExecutor);
    }

    /**
     * 获取或创建日志文件
     * 按小时自动分割日志文件
     * 文件命名格式：client_YYYYMMDD_HH.log
     */
    private synchronized Path getOrCreateLogFile() throws IOException {
        LocalDateTime now = LocalDateTime.now();
        int hour = now.getHour();

        if (currentHour != hour || currentLogFile == null) {
            String fileName = String.format("%s%s_%02d%s",
                    LOG_FILE_PREFIX,
                    now.format(DateTimeFormatter.BASIC_ISO_DATE),
                    hour,
                    LOG_FILE_SUFFIX);
            currentLogFile = Paths.get(LOG_DIR, fileName);
            currentHour = hour;

            if (!Files.exists(currentLogFile)) {
                Files.createFile(currentLogFile);
            }
        }

        return currentLogFile;
    }

    /**
     * 格式化日志条目
     * 输出格式：ID|IP|时间|名称|随机数|处理时间|延迟时间
     */
    private String formatLogEntry(LogEntry entry) {
        return String.format("%s|%s|%s|%s|%d|%d|%d",
                entry.getId(),
                entry.getIp(),
                entry.getEventTime(),
                entry.getName(),
                entry.getRandomNumber(),
                entry.getProcessTime(),
                entry.getDelayTime());
    }

    private void createLogDirectory() {
        try {
            Files.createDirectories(Paths.get(LOG_DIR));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create log directory", e);
        }
    }
}

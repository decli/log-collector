package com.example.logcollector.controller;

import com.example.logcollector.model.LogEntry;
import com.example.logcollector.service.LogService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/api/logs")
public class LogController {
    private final LogService logService;

    public LogController(LogService logService) {
        this.logService = logService;
    }

    @PostMapping("/realtime")
    public ResponseEntity<String> handleRealtimeLog(@RequestBody LogEntry logEntry) {
        try {
            logService.processRealtimeLog(logEntry);
            return ResponseEntity.ok("Success");
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Failed to process log: " + e.getMessage());
        }
    }

    @PostMapping("/batch")
    public ResponseEntity<String> handleBatchLogs(@RequestParam("file") MultipartFile zipFile) {
        try {
            logService.processBatchLogs(zipFile);
            return ResponseEntity.ok("Success");
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Failed to process batch logs: " + e.getMessage());
        }
    }
}
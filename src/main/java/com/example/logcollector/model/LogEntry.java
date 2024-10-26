package com.example.logcollector.model;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonFormat;

public class LogEntry {
    private String id;
    private String ip;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime eventTime;

    private String name;
    private Integer randomNumber;
    private Long processTime;
    private Long delayTime;

    // Default constructor
    public LogEntry() {
    }

    // Getters and Setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public LocalDateTime getEventTime() {
        return eventTime;
    }

    public void setEventTime(LocalDateTime eventTime) {
        this.eventTime = eventTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getRandomNumber() {
        return randomNumber;
    }

    public void setRandomNumber(Integer randomNumber) {
        this.randomNumber = randomNumber;
    }

    public Long getProcessTime() {
        return processTime;
    }

    public void setProcessTime(Long processTime) {
        this.processTime = processTime;
    }
    public Long getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(Long delayTime) {
        this.delayTime = delayTime;
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "id='" + id + '\'' +
                ", ip='" + ip + '\'' +
                ", eventTime=" + eventTime +
                ", name='" + name + '\'' +
                ", randomNumber=" + randomNumber +
                ", processTime=" + processTime +
                ", delayTime=" + delayTime +
                '}';
    }
}

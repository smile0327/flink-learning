package com.kevin.study.flink;

import java.sql.Timestamp;

/**
 * @Auther: kevin
 * @Description: 登录日志
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 11:42 2020/5/7
 * @ProjectName: Flink-SXT
 */

public class LoginEvent {

    private int id;
    private String userName;
    private String eventType;
    private Long eventTime;

    public LoginEvent() {
    }

    public LoginEvent(int id, String userName, String eventType, Long eventTime) {
        this.id = id;
        this.userName = userName;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "id=" + id +
                ", userName='" + userName + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + new Timestamp(eventTime * 1000) +
                '}';
    }
}

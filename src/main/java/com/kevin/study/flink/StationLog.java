package com.kevin.study.flink;

/**
 * @Auther: kevin
 * @Description:
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 13:45 2020/4/21
 * @ProjectName: Flink-SXT
 */
public class StationLog {

    private String sid;
    private String callOut;
    private String callIn;
    private String callType;
    private Long callTime;
    private Long duration;

    public StationLog() {
    }

    public StationLog(String sid, String callOut, String callIn, String callType, Long callTime, Long duration) {
        this.sid = sid;
        this.callOut = callOut;
        this.callIn = callIn;
        this.callType = callType;
        this.callTime = callTime;
        this.duration = duration;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getCallOut() {
        return callOut;
    }

    public void setCallOut(String callOut) {
        this.callOut = callOut;
    }

    public String getCallIn() {
        return callIn;
    }

    public void setCallIn(String callIn) {
        this.callIn = callIn;
    }

    public String getCallType() {
        return callType;
    }

    public void setCallType(String callType) {
        this.callType = callType;
    }

    public Long getCallTime() {
        return callTime;
    }

    public void setCallTime(Long callTime) {
        this.callTime = callTime;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    @Override
    public String toString() {
        return "StationLog{" +
                "sid='" + sid + '\'' +
                ", callOut='" + callOut + '\'' +
                ", callIn='" + callIn + '\'' +
                ", callType='" + callType + '\'' +
                ", callTime=" + callTime +
                ", duration=" + duration +
                '}';
    }
}

package com.kevin.study.flink;

/**
 * @Auther: kevin
 * @Description:
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 15:22 2020/5/8
 * @ProjectName: Flink-SXT
 */
public class Event {

    private Long id;
    private String name;
    private String operateType;
    private Long time;

    public Event() {
    }

    public Event(Long id, String name, String operateType, Long time) {
        this.id = id;
        this.name = name;
        this.operateType = operateType;
        this.time = time;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOperateType() {
        return operateType;
    }

    public void setOperateType(String operateType) {
        this.operateType = operateType;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "Event{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", operateType='" + operateType + '\'' +
                ", time=" + time +
                '}';
    }
}

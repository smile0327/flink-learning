package com.kevin.study.flink.udf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;



import java.util.Arrays;


/**
 * @Auther: kevin
 * @Description:  自定义split函数
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 11:18 2020/4/23
 * @ProjectName: Flink-SXT
 */
public class MySplitFunc extends TableFunction<Row> {

    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING , Types.INT);
    }

    /**
     * 执行拆分
     *
     * @param line
     */
    public void eval(String line) {
        Arrays.stream(line.split(" ")).forEach(w -> {
            Row row = new Row(2);
            row.setField(0 , w);
            row.setField(1 , 1);
            collect(row);
        });
    }

}


package cdc_test.func;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @author zxy
 * @create 2022-03-28 0:24
 * 自定义反序列化
 */
public class CustomerDeserializationSchema implements DebeziumDeserializationSchema<String> {

    /** 需求：返回的json数据格式
     * {
     * "db":"",
     * "tableName":"",
     * "before":{"id":"1001","name":""...},
     * "after":{"id":"1001","name":""...},
     * "op":""
     * }
     */

    /*mysql SourceRecord数据源to_string
    * SourceRecord{
    *   sourcePartition={server=mysql_binlog_source},
    *   sourceOffset={transaction_id=null, ts_sec=1648398062, file=myslq-bin.000001, pos=1736, row=1, server_id=1, event=2}
    * }
    *
    * ConnectRecord{
    *   topic='mysql_binlog_source.flink_test.cdc_test',
    *   kafkaPartition=null,
    *   key=null,
    *   keySchema=null,
    *   value=Struct{
    *       before=Struct{id=1001,name=张馨予,sex=男},
    *       after=Struct{id=1001,name=张馨予,sex=女},
    *       source=Struct{version=1.5.2.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1648398062000,db=flink_test,table=cdc_test,server_id=1,file=myslq-bin.000001,pos=1886,row=0},
    *       op=u,ts_ms=1648398062282
    *   },
    *   valueSchema=Schema{mysql_binlog_source.flink_test.cdc_test.Envelope:STRUCT},
    *   timestamp=null,
    *   headers=ConnectHeaders(headers=)
    * }
     * */

    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        //1.创建JSON对象用于封装结果数据
        JSONObject result = new JSONObject();

        //2.获取数据源记录 sourceRecord 具体信息
        //1）获取cdc数据源:库名、表名
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        result.put("db", split[1]);
        result.put("table", split[2]);

        //2）获取数据中的 before 信息
        Struct value = (Struct) sourceRecord.value();//value是一个struct数据类型
        Struct before = value.getStruct("before");
        JSONObject beforeJSON = new JSONObject();
        if( before != null){
            //获取列信息
            Schema schema = before.schema();
            List<Field> fields = schema.fields();
            for (Field field:fields) {
                beforeJSON.put(field.name(), before.get(field));
            }
        }
        result.put("before", beforeJSON);

        //3）获取数据中的 after 信息
        Struct after = value.getStruct("after");
        JSONObject afterJSON = new JSONObject();
        if (after != null){
            Schema schema = after.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                afterJSON.put(field.name(), after.get(field));
            }

        }
        result.put("after", afterJSON);

        //4）获取操作类型：op 信息
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        result.put("op", operation);

        //3.输出数据
        collector.collect(result.toString());
    }


    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}

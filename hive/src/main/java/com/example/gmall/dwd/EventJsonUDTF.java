package com.example.gmall.dwd;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zxy
 * @create 2020-05-29 12:01
 */
public class EventJsonUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();

        //输入校验：LogUDF(line, "et")
        //[{"ett":"1583762395754","en":"display","kv":{"goodsid":"0","action":"1","extend1":"1","place":"1","category":"75"}},……
        if(inputFields.size() != 1){
            throw new UDFArgumentException("参数个数只能为1");
        }
        if (!"string".equals(inputFields.get(0).getFieldObjectInspector().getTypeName())) {
            throw new UDFArgumentException("参数类型只能为string");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("event_name");
        fieldNames.add("event_json");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {

        String eventArray = objects[0].toString();

        JSONArray jsonArray = new JSONArray(eventArray);

        for (int i = 0; i < jsonArray.length(); i++) {

            String[] result = new String[2];

            result[0] = jsonArray.getJSONObject(i).getString("en");
            result[1] = jsonArray.getString(i);


            forward(result);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}

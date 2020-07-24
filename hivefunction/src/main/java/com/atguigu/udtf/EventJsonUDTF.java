package com.atguigu.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

public class EventJsonUDTF extends GenericUDTF {

    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        //定义UDTF返回值类型和名称
        List<String> fieldName = new ArrayList<>();
        List<ObjectInspector> fieldType = new ArrayList<>();

        fieldName.add("event_name");
        fieldName.add("event_json");

        fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName,fieldType);
        //此处getColumnarStructObjectInspector需要返回两个list，所以要创建定义两个数组，并通过add()方法添
        //加需要处理的en,和整个json串的两部分数据(其中一个是字符串，另一个是json数据)
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        //将输入的值转为字符串
        String input = objects[0].toString();
        //合法性校验，通过StringUtils.isBlank()方法判断输入的是否为空
        if (StringUtils.isBlank(input)){
            return;
        }else {
            //如果不为空，则先创建JSONArray数据，并且遍历每一个数组，取出json当中的en,和每一个json数据
            JSONArray ja = new JSONArray(input);
            if (ja == null){
                return;
            }
            //循环遍历array当中的每一个元素，封装成返回的事件名称和时间内容
            for (int i = 0; i < ja.length(); i++) {
//                {
//                    "ett": "1595268497852",
//                        "en": "display",
//                        "kv": {
//                    "goodsid": "276",
//                            "action": "1",
//                            "extend1": "1",
//                            "place": "2",
//                            "category": "43"
//                }
//                }
                //目的是取出en,和en当中的所有值包括en(也就是数组当中的每一个json串)
                String[] result = new String[2];

                try {
                    result[0] = ja.getJSONObject(i).getString("en");
                    result[1] = ja.getString(i);
                }catch (JSONException e){
                    continue;
                }
                //写出
                forward(result);

            }
        }
    }

    @Override
    public void close() throws HiveException {

    }
}

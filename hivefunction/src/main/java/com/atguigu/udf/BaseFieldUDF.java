package com.atguigu.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONObject;
//自定义udf方法实现日志解析步骤：
//1.自定义UDF方法(BaseFieldUDF) 集成 UDF，然后重写evaluate()方法--输出两个参数，第一个是这一行数据，第二个是查询时传递的单数;
//2.获取一行用split方法切割（log[0]是时间，log[1]是json串）
//3.判断合法性
//4.获取json对象
//5.如果输入的参数是时间(st->log[0])，则放回时间
//6.如果输入的参数是et，则通过json.getString()方法获取到数据并返回
//7.如果输入的参数是cm(json串)则new一个json.getJSONObject("cm")方法用于接收到的json串，并判断是否有输入参数(第二个参数)的key，如果有则返回
public class BaseFieldUDF extends UDF {
    public String evaluate(String line,String key){
        String[] log = line.split("\\|");
        System.out.println(log.length);

        //3.判断合法性
        if (log.length != 2 || StringUtils.isBlank(log[1].trim())){
            return "";
        }

        String result = "";
//      获取到json对象
        JSONObject json = new JSONObject(log[1].trim());

        if ("st".equals(key)){
            result = log[0].trim();

        }else if ("et".equals(key)){
            if(json.has("et")){
                result = json.getString("et");
            }
        }else {
            JSONObject cm = json.getJSONObject("cm");
            if (cm.has(key)){
                result = cm.getString(key);
            }
        }
        return result;
    }

    public static void main(String[] args) {
        String line = "1595326086286,{\"cm\":{\"ln\":\"-100.9\",\"sv\":\"V2.7.0\",\"os\":\"8.2.4\",\"g\":\"37DD5PA2@gmail.com\",\"mid\":\"998\",\"nw\":\"WIFI\",\"l\":\"en\",\"vc\":\"4\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"998\",\"t\":\"1595229430004\",\"la\":\"14.2\",\"md\":\"sumsung-0\",\"vn\":\"1.0.8\",\"ba\":\"Sumsung\",\"sr\":\"S\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1595268497852\",\"en\":\"display\",\"kv\":{\"goodsid\":\"276\",\"action\":\"1\",\"extend1\":\"1\",\"place\":\"2\",\"category\":\"43\"}},{\"ett\":\"1595245404503\",\"en\":\"ad\",\"kv\":{\"entry\":\"3\",\"show_style\":\"1\",\"action\":\"1\",\"detail\":\"\",\"source\":\"4\",\"behavior\":\"1\",\"content\":\"2\",\"newstype\":\"8\"}},{\"ett\":\"1595293354032\",\"en\":\"active_foreground\",\"kv\":{\"access\":\"\",\"push_id\":\"2\"}},{\"ett\":\"1595294174996\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"2\"}},{\"ett\":\"1595269156430\",\"en\":\"error\",\"kv\":{\"errorDetail\":\"java.lang.NullPointerException\\\\n    at cn.lift.appIn.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\\\\n at cn.lift.dfdf.web.AbstractBaseController.validInbound\",\"errorBrief\":\"at cn.lift.dfdf.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\"}},{\"ett\":\"1595310012102\",\"en\":\"comment\",\"kv\":{\"p_comment_id\":1,\"addtime\":\"1595274636761\",\"praise_count\":199,\"other_id\":0,\"comment_id\":2,\"reply_count\":195,\"userid\":6,\"content\":\"吴夏喇舆靖胶幂摊丹夯通友额\"}},{\"ett\":\"1595236839864\",\"en\":\"praise\",\"kv\":{\"target_id\":7,\"id\":9,\"type\":4,\"add_time\":\"1595234196409\",\"userid\":1}}]}";
        String result = new BaseFieldUDF().evaluate(line, "st");
        System.out.println(result);

    }
}

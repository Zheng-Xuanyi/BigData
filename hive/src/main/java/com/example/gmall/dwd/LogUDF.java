package com.example.gmall.dwd;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author zxy
 * @create 2020-05-29 9:09
 */
public class LogUDF extends UDF {

    public String evaluate(String line, String key) throws JSONException{

        String[] log = line.split("\\|");

        if(log.length != 2 || StringUtils.isBlank(log[1])){
            return "";
        }

        JSONObject jsonObject = new JSONObject(log[1].trim());

        String result ="";

        if("st".equals(key)){
            result = log[0].trim();
        } else if("et".equals(key)){
            if(jsonObject.has(key))
                result = jsonObject.getString("et");
        } else {
            JSONObject cm = jsonObject.getJSONObject("cm");
            if(cm.has(key)){
                result = cm.getString(key);
            }
        }
        return result;
    }

    public static void main(String[] args) {
        String line = "1583769878795|{\"cm\":{\"ln\":\"-81.8\",\"sv\":\"V2.0.4\",\"os\":\"8.1.4\",\"g\":\"6WTJOK0U@gmail.com\",\"mid\":\"0\",\"nw\":\"WIFI\",\"l\":\"es\",\"vc\":\"13\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"0\",\"t\":\"1583759085431\",\"la\":\"-56.8\",\"md\":\"HTC-11\",\"vn\":\"1.2.0\",\"ba\":\"HTC\",\"sr\":\"V\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1583762395754\",\"en\":\"display\",\"kv\":{\"goodsid\":\"0\",\"action\":\"1\",\"extend1\":\"1\",\"place\":\"1\",\"category\":\"75\"}},{\"ett\":\"1583724532282\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"9\",\"action\":\"3\",\"extend1\":\"\",\"type\":\"3\",\"type1\":\"433\",\"loading_way\":\"2\"}},{\"ett\":\"1583769508760\",\"en\":\"ad\",\"kv\":{\"activityId\":\"1\",\"displayMills\":\"108168\",\"entry\":\"3\",\"action\":\"2\",\"contentType\":\"0\"}},{\"ett\":\"1583679928078\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1583691432030\",\"action\":\"3\",\"type\":\"3\",\"content\":\"\"}},{\"ett\":\"1583707657793\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"3\"}},{\"ett\":\"1583719679570\",\"en\":\"comment\",\"kv\":{\"p_comment_id\":2,\"addtime\":\"1583730948438\",\"praise_count\":971,\"other_id\":0,\"comment_id\":5,\"reply_count\":112,\"userid\":7,\"content\":\"荆拱涩板饶铀蠢亲\"}},{\"ett\":\"1583670432177\",\"en\":\"praise\",\"kv\":{\"target_id\":2,\"id\":1,\"type\":1,\"add_time\":\"1583744935803\",\"userid\":0}}]}";

        LogUDF logUDF = new LogUDF();

        System.out.println(logUDF.evaluate(line, "st"));
        System.out.println(logUDF.evaluate(line, "et"));
        System.out.println("---------------");
        System.out.println(logUDF.evaluate(line, "mid"));
    }
}


/*
1583769878795 |
{
        "cm": {
            "ln": "-81.8",
            "sv": "V2.0.4",
            "os": "8.1.4",
            "g": "6WTJOK0U@gmail.com",
            "mid": "0",
            "nw": "WIFI",
            "l": "es",
            "vc": "13",
            "hw": "750*1134",
            "ar": "MX",
            "uid": "0",
            "t": "1583759085431",
            "la": "-56.8",
            "md": "HTC-11",
            "vn": "1.2.0",
            "ba": "HTC",
            "sr": "V"
        },
        "ap": "app",
        "et": [{
                "ett": "1583762395754",
                "en": "display",
                "kv": {
                "goodsid": "0",
                "action": "1",
                "extend1": "1",
                "place": "1",
                "category": "75"
                }
            }, {
            "ett": "1583724532282",
            "en": "loading",
            "kv": {
            "extend2": "",
            "loading_time": "9",
            "action": "3",
            "extend1": "",
            "type": "3",
            "type1": "433",
            "loading_way": "2"
            }
            }, {
            "ett": "1583769508760",
            "en": "ad",
            "kv": {
            "activityId": "1",
            "displayMills": "108168",
            "entry": "3",
            "action": "2",
            "contentType": "0"
            }
            }, {
            "ett": "1583679928078",
            "en": "notification",
            "kv": {
            "ap_time": "1583691432030",
            "action": "3",
            "type": "3",
            "content": ""
            }
            }, {
            "ett": "1583707657793",
            "en": "active_background",
            "kv": {
            "active_source": "3"
            }
            }, {
            "ett": "1583719679570",
            "en": "comment",
            "kv": {
            "p_comment_id": 2,
            "addtime": "1583730948438",
            "praise_count": 971,
            "other_id": 0,
            "comment_id": 5,
            "reply_count": 112,
            "userid": 7,
            "content": "荆拱涩板饶铀蠢亲"
            }
            }, {
            "ett": "1583670432177",
            "en": "praise",
            "kv": {
            "target_id": 2,
            "id": 1,
            "type": 1,
            "add_time": "1583744935803",
            "userid": 0
            }
        }]
        }

 */
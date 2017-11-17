package com.github.danielwegener.logback.kafka.encoding;

import com.alibaba.fastjson.JSONObject;
import com.github.danielwegener.logback.kafka.util.NetworkUtil;

import java.util.Calendar;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author heyc
 * @date 2017/11/17 15:09
 */
public class EventFieldsWrapper {

    private static final AtomicLong lineNumber = new AtomicLong(0);

    private static int dayOfMonth = Calendar.getInstance().get(Calendar.DAY_OF_MONTH);

    private static String serverIp;

    private static String app;

    private static String source;

    static {
        EventFieldsWrapper.serverIp = NetworkUtil.getSiteIp();
    }

    public static String wrap(String message){
        try {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(Constants.BODY_MESSAGE, message);
            /** 记录行号 **/
            if (dayOfMonth != Calendar.getInstance().get(Calendar.DAY_OF_MONTH)){
                lineNumber.set(0);
                dayOfMonth = Calendar.getInstance().get(Calendar.DAY_OF_MONTH);
            }
            jsonObject.put("lineNumber",lineNumber.incrementAndGet());
            if (serverIp != null){
                jsonObject.put("serverIp",serverIp);
            }
            if (app != null){
                jsonObject.put("app",app);
            }
            if (source != null){
                jsonObject.put("source",source);
            }
            return jsonObject.toJSONString();
        }catch (Exception e){
        }
        return message;
    }



    public static class Constants{

        private static final String CHARSET = "charset";

        private static final String FIELDS = "fields";

        private static final String BODY_MESSAGE = "message";

        private static final String APP = "app";

        private static final String SERVER_HOST = "server.host";

        private static final String SERVER_IP = "server.ip";

        private static final String TIMESTAMP = "timestamp";

        private static final String SOURCE = "source";

    }

    public static String getApp() {
        return app;
    }

    public static void setApp(String app) {
        EventFieldsWrapper.app = app;
    }

    public static String getSource() {
        return source;
    }

    public static void setSource(String source) {
        EventFieldsWrapper.source = source;
    }
}

package com.laozhang.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//分发拦截器
public class DistributeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] lineByte = event.getBody();
        String lineStr = new String(lineByte, Charset.forName("UTF-8"));

        String dataType = getDataType(lineStr);
        Map<String, String> headers = event.getHeaders();
        if (dataType.equals("load")) {
            headers.put("topic","topic_load");
        }else {
            headers.put("topic","topic_prod");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> filterEvents = new ArrayList<>(list.size());

        for (Event event : list) {
            Event validEvent = intercept(event);
            filterEvents.add(validEvent);
        }
        return filterEvents;
    }

    @Override
    public void close() {

    }

    private String getDataType(String lineStr){
        Map<String, Object> dataMap = JSONObject.parseObject(lineStr, new TypeReference<HashMap<String, Object>>() {});
        return (String)dataMap.get("topic");
    }

    public static class Builder implements  Interceptor.Builder{
        @Override
        public Interceptor build() {
            return new DistributeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}

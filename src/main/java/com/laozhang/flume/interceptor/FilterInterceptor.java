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

//过滤拦截器
public class FilterInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        byte[] lineByte = event.getBody();
        String lineStr = new String(lineByte, Charset.forName("UTF-8"));

        if (lineStr.contains("state")) {
            if (validState(lineStr)){
                return event;
            }
            return null;
        }else {//其他数据原样输出
            return event;
        }
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

    private boolean validState(String lineStr){
        Map<String, Object> dataMap =  null;
        try {
            dataMap = JSONObject.parseObject(lineStr, new TypeReference<HashMap<String, Object>>() {});
        }catch (Exception ex){
            return false;
        }
        if(dataMap == null){
            return false;
        }

        int count = (Integer) dataMap.get("cnt");
        if(count == -1) {//生产设备短暂失效，不是有效数据，需过滤
            return false;
        }
        String mechineId = (String) dataMap.get("id");
        if(StringUtils.isBlank(mechineId)){//生产设备未设置成正常生产状态，需过滤
            return false;
        }
        return true;
    }

    public static class Builder implements  Interceptor.Builder{
        @Override
        public Interceptor build() {
            return new FilterInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}

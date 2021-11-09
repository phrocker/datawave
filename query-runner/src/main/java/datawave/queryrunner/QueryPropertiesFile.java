package datawave.queryrunner;

import datawave.query.config.ShardQueryConfiguration;
import org.apache.commons.collections.map.HashedMap;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;

public class QueryPropertiesFile extends Properties {

    static Map<String, Method> propertySetters = new HashedMap();

    static{
        try {
            propertySetters.put("maxIndexBatchSize",ShardQueryConfiguration.class.getDeclaredMethod("setMaxIndexBatchSize", int.class));
            propertySetters.put("maxIndexScanTimeMillis",ShardQueryConfiguration.class.getDeclaredMethod("setMaxIndexScanTimeMillis", long.class));
            propertySetters.put("queryThreads",ShardQueryConfiguration.class.getDeclaredMethod("setNumQueryThreads", Integer.class));
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    private ShardQueryConfiguration shardQueryConfiguration;

    public QueryPropertiesFile(){
        shardQueryConfiguration = new ShardQueryConfiguration();
    }

    @Override
    public synchronized void load(InputStream input) throws IOException {
        super.load(input);
        for(Object key : keySet()){
            // better exception management in this for-loop.
            setConfigurationProperty(key.toString(),getProperty(key.toString()));
        }
    }

    public synchronized Object setProperty(String key, Long value) throws IOException {

        setConfigurationProperty(key,value.toString());
        return put(key, value.toString());
    }

    public synchronized Object setProperty(String key, Integer value) throws IOException {
        setConfigurationProperty(key,value.toString());
        return put(key, value.toString());
    }

    private synchronized void setConfigurationProperty(String key, String value) throws IOException {
        Method mthd = propertySetters.get(key);
        try {
            if (null != mthd) { // we have a property to set.
                if (mthd.getParameters()[0].getType().toString().equals(int.class.toString())) {
                    mthd.invoke(shardQueryConfiguration, Integer.valueOf(value));
                } else if (mthd.getParameters()[0].getType().toString().equals(long.class.toString())) {
                    mthd.invoke(shardQueryConfiguration, Long.valueOf(value));
                }
            }
        }catch(InvocationTargetException | IllegalAccessException e){
            throw new IOException(e);
        }
    }

    public ShardQueryConfiguration getShardQueryConfiguration(){
        return shardQueryConfiguration;
    }
}

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
    
    static Map<String,Method> propertySetters = new HashedMap();
    
    static {
        for (final Method method : ShardQueryConfiguration.class.getMethods()) {
            if (method.getName().startsWith("set")) {
                String option = method.getName().substring(3);
                option = Character.toLowerCase(option.charAt(0)) + option.substring(1);
                propertySetters.put(option, method);
            }
        }
        
    }
    
    private ShardQueryConfiguration shardQueryConfiguration;
    
    public QueryPropertiesFile() {
        shardQueryConfiguration = new ShardQueryConfiguration();
    }
    
    @Override
    public synchronized void load(InputStream input) throws IOException {
        super.load(input);
        for (Object key : keySet()) {
            // better exception management in this for-loop.
            setConfigurationProperty(key.toString(), getProperty(key.toString()));
        }
    }
    
    @Override
    public synchronized Object setProperty(String key, String value) {
        try {
            setConfigurationProperty(key, value);
        } catch (IOException e) {
            return null;
        }
        return put(key, value.toString());
    }
    
    /**
     * Convenience method for setting the properties
     * 
     * @param key
     *            key
     * @param value
     *            long value
     * @return this object
     * @throws IOException
     */
    public synchronized Object setProperty(String key, Long value) throws IOException {
        setConfigurationProperty(key, value.toString());
        return put(key, value.toString());
    }
    
    /**
     * Convenience method for setting the properties
     * 
     * @param key
     *            key
     * @param value
     *            integer value
     * @return this object
     * @throws IOException
     */
    public synchronized Object setProperty(String key, Integer value) throws IOException {
        setConfigurationProperty(key, value.toString());
        return put(key, value.toString());
    }
    
    private synchronized void setConfigurationProperty(String key, String value) throws IOException {
        final Method mthd = propertySetters.get(key);
        try {
            if (null != mthd) { // we have a property to set.
                if (mthd.getParameters()[0].getType().toString().equals(int.class.toString())) {
                    mthd.invoke(shardQueryConfiguration, Integer.valueOf(value));
                } else if (mthd.getParameters()[0].getType().toString().equals(long.class.toString())) {
                    mthd.invoke(shardQueryConfiguration, Long.valueOf(value));
                } else if (mthd.getParameters()[0].getType().toString().equals(String.class.toString())) {
                    mthd.invoke(shardQueryConfiguration, value);
                }
            }
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new IOException(e);
        }
    }
    
    public ShardQueryConfiguration getShardQueryConfiguration() {
        return shardQueryConfiguration;
    }
}

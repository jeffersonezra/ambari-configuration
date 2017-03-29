package com.microsoft.configuration.management;
import java.util.Map;

/**
 * Created by jeezra on 3/11/2016.
 */
public class ConfigItem {
    public String href;
    public String tag;
    public String type;
    public int version;
    public Config Config;
    public Map<String,String> properties;
    public PropertiesAttributes properties_attributes;
}

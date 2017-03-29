package com.microsoft.configuration.management;
import java.util.Map;

/**
 * Created by jeezra on 3/10/2016.
 */
public class Cluster {
    public String cluster_name;
    public String version;
    public Map<String, ComponentConfig> desired_configs;
}

package com.microsoft.configuration.management;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

/**
 * Created by jeezra on 3/11/2016.
 */
public class PropertiesAttributes {
    @SerializedName("final")
    public Map<String, String> finalAttributes;
}

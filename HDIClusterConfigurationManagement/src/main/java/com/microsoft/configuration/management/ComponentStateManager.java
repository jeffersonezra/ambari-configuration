package com.microsoft.configuration.management;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;

/**
 * Created by jeezra on 3/11/2016.
 */
public class ComponentStateManager {
    public RequestInfo RequestInfo;
    @SerializedName("Requests/resource_filters")
    public ArrayList<ResourceFilter> ResourceFilters;
}

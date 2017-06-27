package com.microsoft.configuration.management;
import com.google.gson.JsonSyntaxException;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.internal.Streams;
import com.google.gson.internal.bind.ArrayTypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.joda.time.*;
import org.apache.commons.lang3.StringUtils;
import sun.misc.BASE64Encoder;

import javax.naming.AuthenticationException;
import javax.net.ssl.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Paths;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class Main {

    static JsonObject configuration = null;
    static boolean shouldRestartService = false;

    public static void main(String[] args) throws  Exception {

        Scanner scanner = new Scanner(System.in);
        int actionType = Integer.MIN_VALUE;
        int serviceRestartType = Integer.MIN_VALUE;
        String configFilePath = null;

        while (actionType == Integer.MIN_VALUE || actionType < 1 || actionType > 3) {
            System.out.println("Press 1 to download configurations, 2 to upload configurations, 3 to exit: ");
            if (scanner.hasNextInt()) {
                actionType = scanner.nextInt();
            } else {
                scanner.next();
            }
        }

        if (actionType == 3) {
            return;
        }

        if (actionType == 2) {
            while (serviceRestartType == Integer.MIN_VALUE || serviceRestartType < 1 || serviceRestartType > 3) {
                System.out.println("Press 1 to enable service restart after config update, 2 to disable service restart, 3 to exit: ");
                if (scanner.hasNextInt()) {
                    serviceRestartType = scanner.nextInt();
                } else {
                    scanner.next();
                }
            }

            if (serviceRestartType == 3) {
                return;
            }
        }

        shouldRestartService = serviceRestartType == 1;

        System.out.println("Location of configuration file (example - config.json): ");
        configFilePath = scanner.next();

        configuration = readConfigurationFromFile(configFilePath);

        URL ambariUrl = new URL(configuration.get("ambariUrl").getAsString());
        String ambariUsername = configuration.get("ambariUsername").getAsString();
        String ambariPassword = configuration.get("ambariPassword").getAsString();
        String clusterName = ambariUrl.getHost().split("\\.")[0];

        switch (actionType) {
            case 1:
                //sourceClusterName = configuration.get("sourceClusterName").getAsString();
                DownloadConfigurations(ambariUrl, ambariUsername, ambariPassword, clusterName);
                break;

            case 2:
                UploadConfiguration(ambariUrl, ambariUsername, ambariPassword, clusterName);
                break;
        }
    }

    public static void DownloadConfigurations(URL ambariHost, String ambariUsername, String ambariPassword, String clusterName) throws Exception
    {
        System.out.println("Downloading configurations from cluster: " + clusterName);

        Gson gson = new Gson();
        ComponentsConfigurationVersion currentConfigurationVersions = null;

        LinkedTreeMap<String, LinkedTreeMap> servicesToDownload = gson.fromJson(configuration.getAsJsonObject("services"), LinkedTreeMap.class);
        String location = configuration.get("location").getAsString();
        System.out.println("Configuration will be stored in: " + location);

        if (!Paths.get(location).toFile().exists()){
            System.out.println("Location " + location + " does not exist. Creating");
            Paths.get(location).toFile().mkdirs();
        }

        if (servicesToDownload == null) {
            System.out.println("No services to download");
            return;
        }

        System.out.println("Querying current configuration version");
        HttpResult result = GetCurrentConfigurationVersion(ambariHost, ambariUsername, ambariPassword, clusterName);
        if (result.ResponseCode == 200) {
            currentConfigurationVersions = gson.fromJson(result.Content, ComponentsConfigurationVersion.class);
        }

        for (Map.Entry<String,LinkedTreeMap> service : servicesToDownload.entrySet()) {
            LinkedTreeMap<String, LinkedTreeMap> serviceComponents = (LinkedTreeMap<String, LinkedTreeMap>) (service.getValue().get("configFiles"));
            if (serviceComponents != null) {
                for (Map.Entry<String, LinkedTreeMap> component : serviceComponents.entrySet()) {
                    if (currentConfigurationVersions.Clusters.desired_configs.entrySet() != null) {
                        for (Map.Entry<String, ComponentConfig> configVersion : currentConfigurationVersions.Clusters.desired_configs.entrySet()) {
                            if (configVersion.getKey().equalsIgnoreCase(component.getKey())) {
                                System.out.println("Querying current configuration for " + component.getKey());
                                result = GetCurrentConfiguration(ambariHost, ambariUsername, ambariPassword, clusterName, configVersion.getKey(), configVersion.getValue().tag);
                                if (result.ResponseCode != 200) {
                                    throw new RuntimeException("Failed to retrieve configuration for " + component.getKey());
                                }

                                CurrentConfig currentConfig = gson.fromJson(result.Content, CurrentConfig.class);

                                // cleanup

                                ArrayList<String> cleanup = (ArrayList<String>) (component.getValue().get("ignore"));

                                if (cleanup != null && currentConfig.items.get(0).properties.entrySet() != null) {
                                    System.out.println("Cleaning up sensitive data");
                                    for (String ignoredItem : cleanup) {
                                        for (Map.Entry<String, String> property : currentConfig.items.get(0).properties.entrySet()) {
                                            Pattern pattern = Pattern.compile(ignoredItem, Pattern.CASE_INSENSITIVE);
                                            Matcher matcher = pattern.matcher(property.getKey());
                                            if (matcher.matches()) {
                                                property.setValue(null);
                                            }
                                        }
                                    }
                                }

                                if (!Paths.get(location, service.getKey()).toFile().exists()) {
                                    System.out.println("Creating directory: " + Paths.get(location, service.getKey()).toString());
                                    Paths.get(location, service.getKey()).toFile().mkdir();
                                }

                                System.out.println("Writing " + currentConfig.items.get(0).type + " configurations to " + Paths.get(location, service.getKey(), currentConfig.items.get(0).type).toString());
                                String configDownloadFilename = Paths.get(location, service.getKey(), currentConfig.items.get(0).type).toString();
                                configDownloadFilename += ".json";
                                JsonWriter jsonWriter = gson.newJsonWriter(new BufferedWriter(new FileWriter(configDownloadFilename)));
                                jsonWriter.setIndent("  ");
                                gson.toJson(currentConfig, CurrentConfig.class, jsonWriter);
                                jsonWriter.close();
                            }
                        }
                    }
                }
            }
        }
    }

    public static void UploadConfiguration(URL targetAmbariHost, String targetAmbariUsername, String targetAmbariPassword, String targetClusterName) throws Exception {
        Gson gson = new Gson();
        ComponentsConfigurationVersion currentTargetConfigurationVersions = null;
        String location = null;

        if (configuration.get("location") == null) {
            System.out.println("Configuration files location required to upload configurations");
            return;
        }
        else {
            location = configuration.get("location").getAsString();
        }


        System.out.println("Configuration will be read from: " + location);

        System.out.println("Querying current configuration version");
        HttpResult result = GetCurrentConfigurationVersion(targetAmbariHost, targetAmbariUsername, targetAmbariPassword, targetClusterName);
        if (result.ResponseCode == 200) {
            currentTargetConfigurationVersions = gson.fromJson(result.Content, ComponentsConfigurationVersion.class);
        }

        if (currentTargetConfigurationVersions == null) {
            System.out.println("Failed to get the current configuration version");
        }

        JsonElement tag = configuration.get("tag");

        LinkedTreeMap<String, LinkedTreeMap> servicesToDownload = gson.fromJson(configuration.getAsJsonObject("services"), LinkedTreeMap.class);

        if (servicesToDownload != null) {
            boolean configurationUpdated = false;
            for (Map.Entry<String, LinkedTreeMap> downloadService : servicesToDownload.entrySet()) {
                LinkedTreeMap<String, LinkedTreeMap> downloadServiceComponents = (LinkedTreeMap<String, LinkedTreeMap>) (downloadService.getValue().get("configFiles"));
                if (downloadServiceComponents != null) {

                    if (!Paths.get(location, downloadService.getKey()).toFile().exists()) {
                        throw new FileNotFoundException("Directory " + Paths.get(location, downloadService.getKey()).toFile().toString() + " does not exist");
                    }

                    for (Map.Entry<String, LinkedTreeMap> downloadComponent : downloadServiceComponents.entrySet()) {

                        if (!Paths.get(location, downloadService.getKey(), downloadComponent.getKey() + ".json").toFile().exists()) {
                            throw new FileNotFoundException("File " + Paths.get(location, downloadService.getKey(), downloadComponent.getKey() + ".json").toFile().toString() + " does not exist");
                        }

                        String downloadedConfigFilename = Paths.get(location, downloadService.getKey(), downloadComponent.getKey() + ".json").toString();
                        JsonReader jsonReader = gson.newJsonReader(new BufferedReader(new FileReader(downloadedConfigFilename)));
                        CurrentConfig downloadedComponentConfig = gson.fromJson(jsonReader, CurrentConfig.class);
                        jsonReader.close();

                        if (downloadedComponentConfig.items == null || downloadedComponentConfig.items.isEmpty() || downloadedComponentConfig.items.get(0).properties == null) {
                            System.out.println("There is no configuration found for " + downloadComponent.getKey() + ". Skipping...");
                            break;
                        }

                        ArrayList<String> cleanup = (ArrayList<String>) (downloadComponent.getValue().get("ignore"));

                        if (cleanup != null) {
                            System.out.println("Cleaning up ignored data from downloaded config.");
                            for (String ignoredItem : cleanup) {
                                for (Map.Entry<String, String> property : downloadedComponentConfig.items.get(0).properties.entrySet()) {
                                    Pattern pattern = Pattern.compile(ignoredItem, Pattern.CASE_INSENSITIVE);
                                    Matcher matcher = pattern.matcher(property.getKey());
                                    if (matcher.matches()) {
                                        property.setValue(null);
                                    }
                                }
                            }
                        }

                        CurrentConfig targetComponentConfig = null;
                        System.out.println("Querying current configuration from cluster for " + downloadComponent.getKey());
                        for (Map.Entry<String, ComponentConfig> targetConfigVersion : currentTargetConfigurationVersions.Clusters.desired_configs.entrySet()) {
                            if (targetConfigVersion.getKey().equalsIgnoreCase(downloadComponent.getKey())) {
                                result = GetCurrentConfiguration(targetAmbariHost, targetAmbariUsername, targetAmbariPassword, targetClusterName, targetConfigVersion.getKey(), targetConfigVersion.getValue().tag);
                                if (result.ResponseCode != 200) {
                                    throw new RuntimeException("Failed to retrieve configuration for config " + targetConfigVersion.getKey());
                                }
                                targetComponentConfig = gson.fromJson(result.Content, CurrentConfig.class);
                                break;
                            }
                        }

                        PropertiesAttributes propertiesAttributes = downloadedComponentConfig.items.get(0).properties_attributes;
                        if (targetComponentConfig != null && targetComponentConfig.items != null && !targetComponentConfig.items.isEmpty()) {
                            if (targetComponentConfig.items.get(0).properties != null && !targetComponentConfig.items.get(0).properties.isEmpty()) {
                                System.out.println("Merging current configurations from cluster for " + downloadComponent.getKey());

                                for (Map.Entry<String, String> property : targetComponentConfig.items.get(0).properties.entrySet()) {
                                    // if this property doesn't exist in downloaded config, append it to downloaded config
                                    if (!downloadedComponentConfig.items.get(0).properties.containsKey(property.getKey())) {
                                        downloadedComponentConfig.items.get(0).properties.put(property.getKey(), property.getValue());
                                    }
                                    // we will merge the config from cluster if it is in the ignored list
                                    else if (cleanup != null && !cleanup.isEmpty()) {
                                        for (String ignoredItem : cleanup) {
                                            Pattern pattern = Pattern.compile(ignoredItem, Pattern.CASE_INSENSITIVE);
                                            Matcher matcher = pattern.matcher(property.getKey());
                                            if (matcher.matches()) {
                                                downloadedComponentConfig.items.get(0).properties.put(property.getKey(), property.getValue());
                                                break;
                                            }
                                        }
                                    }
                                }
                            }

                            if (propertiesAttributes == null || propertiesAttributes.finalAttributes == null || propertiesAttributes.finalAttributes.isEmpty()) {
                                propertiesAttributes = targetComponentConfig.items.get(0).properties_attributes;
                            }
                            else if (targetComponentConfig.items.get(0).properties_attributes != null && targetComponentConfig.items.get(0).properties_attributes.finalAttributes != null && !targetComponentConfig.items.get(0).properties_attributes.finalAttributes.isEmpty()) {
                                System.out.println("Merging properties attributes from cluster for " + downloadComponent.getKey());
                                for (Map.Entry<String, String> attribute : targetComponentConfig.items.get(0).properties_attributes.finalAttributes.entrySet()) {
                                    if (!propertiesAttributes.finalAttributes.containsKey(attribute.getKey())) {
                                        propertiesAttributes.finalAttributes.put(attribute.getKey(), attribute.getValue());
                                    }
                                }
                            }
                        }

                        System.out.println("Updating " + downloadComponent.getKey() + " configurations for cluster " + targetClusterName);

                        DesiredConfig desiredConfig = new DesiredConfig();
                        desiredConfig.type = downloadComponent.getKey();
                        desiredConfig.tag = (tag == null) ? (downloadComponent.getKey() + Long.toString(System.currentTimeMillis())) : tag.getAsString();
                        desiredConfig.properties = downloadedComponentConfig.items.get(0).properties;
                        desiredConfig.properties_attributes = propertiesAttributes;
                        desiredConfig.service_config_version_note = "Configs updated by HDInsight Cluster Configuration Management client tool.";
                        ClusterConfig config = new ClusterConfig();
                        config.desired_config = desiredConfig;
                        NewClusterConfig newClusterConfig = new NewClusterConfig();
                        newClusterConfig.Clusters = config;

                        result = SetNewConfiguration(targetAmbariHost, targetAmbariUsername, targetAmbariPassword, targetClusterName, newClusterConfig);
                        if (result.ResponseCode != 200) {
                            throw new RuntimeException("Failed to update configuration for " + downloadComponent.getKey());
                        }
                        configurationUpdated = true;
                    }
                }
            }

            if (configurationUpdated && shouldRestartService) {
                System.out.println("Sleeping 75 seconds for configuration updates to be detected by Ambari");
                Thread.sleep(75000);
                RestartUpdatedServices(targetAmbariHost, targetAmbariUsername, targetAmbariPassword, targetClusterName);
            }
        }
    }

    public  static void RestartUpdatedServices(URL targetAmbariHost, String targetAmbariUsername, String targetAmbariPassword, String targetClusterName) throws Exception
    {
        Gson gson = new Gson();
        System.out.println("Querying Ambari for services with stale configurations");
        HttpResult result = GetStaleConfigurationComponents(targetAmbariHost, targetAmbariUsername, targetAmbariPassword, targetClusterName);
        StaleConfigs staleConfigs = null;
        if (result.ResponseCode == 200) {
            staleConfigs = gson.fromJson(result.Content, StaleConfigs.class);
        }

        if (staleConfigs != null && staleConfigs.items != null && staleConfigs.items.size() > 0) {
            HashMap<String, ComponentStateManager> restartRequests = new HashMap<String, ComponentStateManager>();

            for (ServiceInfo serviceInfo : staleConfigs.items) {
                ComponentStateManager stateManager = null;
                if (restartRequests.containsKey(serviceInfo.ServiceComponentInfo.service_name)) {
                    stateManager = restartRequests.get(serviceInfo.ServiceComponentInfo.service_name);
                } else {
                    stateManager = new ComponentStateManager();
                    OperationLevel operationLevel = new OperationLevel();
                    operationLevel.level = "SERVICE";
                    operationLevel.cluster_name = targetClusterName;
                    operationLevel.service_name = serviceInfo.ServiceComponentInfo.service_name;

                    RequestInfo requestInfo = new RequestInfo();
                    requestInfo.command = "RESTART";
                    requestInfo.context = "Restart all components with Stale Configs for " + serviceInfo.ServiceComponentInfo.service_name;
                    requestInfo.operation_level = operationLevel;
                    stateManager.RequestInfo = requestInfo;
                    stateManager.ResourceFilters = new ArrayList<ResourceFilter>();
                }

                ResourceFilter resourceFilter = new ResourceFilter();
                resourceFilter.service_name = serviceInfo.ServiceComponentInfo.service_name;
                resourceFilter.component_name = serviceInfo.ServiceComponentInfo.component_name;
                ArrayList<String> hosts = new ArrayList<String>();
                for (HostComponent hostComponent : serviceInfo.host_components) {
                    hosts.add(hostComponent.HostRoles.host_name);
                }
                resourceFilter.hosts = StringUtils.join(hosts, ",");
                stateManager.ResourceFilters.add(resourceFilter);

                if (!restartRequests.containsKey(serviceInfo.ServiceComponentInfo.service_name)) {
                    restartRequests.put(serviceInfo.ServiceComponentInfo.service_name, stateManager);
                }
            }

            for (ComponentStateManager restartRequest : restartRequests.values()) {
                System.out.println("Restarting service " + restartRequest.RequestInfo.operation_level.service_name);
                result = RestartStaleServices(targetAmbariHost, targetAmbariUsername, targetAmbariPassword, targetClusterName, restartRequest);
                if (result.ResponseCode == 202) {
                    if (gson.fromJson(result.Content, JsonElement.class) != null && gson.fromJson(result.Content, JsonElement.class).getAsJsonObject() != null && gson.fromJson(result.Content, JsonElement.class).getAsJsonObject().getAsJsonObject("Requests") != null && gson.fromJson(result.Content, JsonElement.class).getAsJsonObject().getAsJsonObject("Requests").getAsJsonPrimitive("id") != null) {
                        int requestId = gson.fromJson(result.Content, JsonElement.class).getAsJsonObject().getAsJsonObject("Requests").getAsJsonPrimitive("id").getAsInt();
                        System.out.println("Waiting for restart to complete");
                        WaitForOperationToComplete(targetAmbariHost, targetAmbariUsername, targetAmbariPassword, targetClusterName, requestId, Duration.standardMinutes(3));
                    }
                }
            }
        }
        else {
            System.out.println("All services have configurations up to date. No restart required.");
        }
    }

    public static void WaitForOperationToComplete(URL ambariHost, String ambariUsername, String ambariPassword, String clusterName, int requestId, Duration timeout) throws Exception
    {
        Instant end = Instant.now().plus(timeout);
        HttpResult result;
        Gson gson = new Gson();
        while (end.compareTo(Instant.now()) >= 0) {
            result = GetOperationState(ambariHost, ambariUsername, ambariPassword, clusterName, requestId);
            if (result.ResponseCode == 200 && gson.fromJson(result.Content, JsonElement.class) != null && gson.fromJson(result.Content, JsonElement.class).getAsJsonObject() != null) {
                JsonObject content = gson.fromJson(result.Content, JsonElement.class).getAsJsonObject();
                if (content != null && content.getAsJsonObject("Requests") != null && content.getAsJsonObject("Requests").getAsJsonPrimitive("request_status") != null) {
                    String state = content.getAsJsonObject("Requests").getAsJsonPrimitive("request_status").getAsString();
                    if (state.equalsIgnoreCase("completed")) {
                        break;
                    }
                }
            }
            Thread.sleep(30000);
        }
    }

    public static HttpResult GetCurrentConfiguration(URL ambariHost, String ambariUsername, String ambariPassword, String clusterName, String configurationType, String configurationVersion) throws  Exception
    {
        String requestPath = "/api/v1/clusters/"+clusterName +"/configurations?type=" + configurationType + "&tag=" + configurationVersion;
        URL requestUrl = new URL(ambariHost, requestPath);
        HttpResult result = SendHttpGetRequest(requestUrl, ambariUsername, ambariPassword);
        return  result;
    }

    public  static  HttpResult GetCurrentConfigurationVersion(URL ambariHost, String ambariUsername, String ambariPassword, String clusterName) throws Exception
    {
        String requestPath = "/api/v1/clusters/"+clusterName +"?fields=Clusters/desired_configs";
        URL requestUrl = new URL(ambariHost, requestPath);
        HttpResult result = SendHttpGetRequest(requestUrl, ambariUsername, ambariPassword);
        return  result;
    }

    public static HttpResult SetNewConfiguration(URL ambariHost, String ambariUsername, String ambariPassword, String clusterName, NewClusterConfig newConfig) throws Exception
    {
        String requestPath = "/api/v1/clusters/" + clusterName;
        URL requestUrl = new URL(ambariHost, requestPath);
        Gson gson = new Gson();
        HttpResult result = SendHttpPutRequest(requestUrl, ambariUsername, ambariPassword, gson.toJson(newConfig));
        return  result;
    }

    public static HttpResult GetStaleConfigurationComponents(URL ambariHost, String ambariUsername, String ambariPassword, String clusterName) throws Exception
    {
        String requestPath = "/api/v1/clusters/" + clusterName + "/components?host_components/HostRoles/stale_configs=true";
        URL requestUrl = new URL(ambariHost, requestPath);
        Gson gson = new Gson();
        HttpResult result = SendHttpGetRequest(requestUrl, ambariUsername, ambariPassword);
        return  result;
    }

    public static HttpResult ManageService(URL ambariHost, String ambariUsername, String ambariPassword, String clusterName, ComponentStateManager stateManager, String serviceName) throws Exception
    {
        String requestPath = "/api/v1/clusters/"+ clusterName + "/services/" + serviceName;
        URL requestUrl = new URL(ambariHost, requestPath);
        Gson gson = new Gson();
        HttpResult result = SendHttpPutRequest(requestUrl, ambariUsername, ambariPassword, gson.toJson(stateManager));
        return  result;
    }

    public static HttpResult RestartStaleServices(URL ambariHost, String ambariUsername, String ambariPassword, String clusterName, ComponentStateManager stateManager) throws Exception
    {
        String requestPath = "/api/v1/clusters/"+ clusterName + "/requests";
        URL requestUrl = new URL(ambariHost, requestPath);
        Gson gson = new Gson();
        HttpResult result = SendHttpPostRequest(requestUrl, ambariUsername, ambariPassword, gson.toJson(stateManager));
        return  result;
    }

    public static HttpResult GetOperationState(URL ambariHost, String ambariUsername, String ambariPassword, String clusterName, int requestId) throws Exception
    {
        String requestPath = "/api/v1/clusters/"+ clusterName + "/requests/" + requestId;
        URL requestUrl = new URL(ambariHost, requestPath);
        Gson gson = new Gson();
        HttpResult result = SendHttpGetRequest(requestUrl, ambariUsername, ambariPassword);
        return  result;
    }

    public static HttpResult SendHttpGetRequest(URL requestUrl, String ambariUsername, String ambariPassword) throws  Exception
    {
        String login = ambariUsername + ":" + ambariPassword;
        String encodedLogin = new BASE64Encoder().encodeBuffer(login.getBytes());
        encodedLogin.trim();

        HttpURLConnection connection = (HttpURLConnection) requestUrl.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Authorization", "Basic " + encodedLogin.trim());

        connection.connect();
        int responseCode = connection.getResponseCode();
        if (responseCode == 403) {
            throw new AuthenticationException("Invalid credentials provided. Please ensure your username and password are correct.");
        }

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));

            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = reader.readLine()) != null) {
                response.append(inputLine);
            }
            return new HttpResult(responseCode, response.toString());
        }
        finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    public static HttpResult SendHttpPutRequest(URL requestUrl, String ambariUsername, String ambariPassword, String content) throws Exception
    {
        String login = ambariUsername + ":" + ambariPassword;
        String encodedLogin = new BASE64Encoder().encodeBuffer(login.getBytes());
        encodedLogin.trim();

        HttpURLConnection connection = (HttpURLConnection) requestUrl.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("PUT");
        connection.setRequestProperty("Authorization", "Basic " + encodedLogin.trim());
        connection.setRequestProperty("X-Requested-By", "ambari");
        connection.setRequestProperty("Content-Type","application/octet-stream");

        if (content != null) {
            DataOutputStream wr = new DataOutputStream(
                    connection.getOutputStream());
            wr.writeBytes(content);
            wr.flush();
            wr.close();
        }

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = reader.readLine()) != null) {
                response.append(inputLine);
            }

            int responseCode = connection.getResponseCode();
            return new HttpResult(responseCode, response.toString());
        }
        finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    public static HttpResult SendHttpPostRequest(URL requestUrl, String ambariUsername, String ambariPassword, String content) throws Exception
    {
        String login = ambariUsername + ":" + ambariPassword;
        String encodedLogin = new BASE64Encoder().encodeBuffer(login.getBytes());
        encodedLogin.trim();

        HttpURLConnection connection = (HttpURLConnection) requestUrl.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Authorization", "Basic " + encodedLogin.trim());
        connection.setRequestProperty("X-Requested-By", "ambari");
        connection.setRequestProperty("Content-Type","application/octet-stream");

        if (content != null) {
            DataOutputStream wr = new DataOutputStream(
                    connection.getOutputStream());
            wr.writeBytes(content);
            wr.flush();
            wr.close();
        }

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = reader.readLine()) != null) {
                response.append(inputLine);
            }

            int responseCode = connection.getResponseCode();
            return new HttpResult(responseCode, response.toString());
        }
        finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    private static void SkipServerCertificateValidation() throws Exception
    {
        TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }

                    public void checkClientTrusted(X509Certificate[] certs, String authType) {  }

                    public void checkServerTrusted(X509Certificate[] certs, String authType) {  }

                }
        };

        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

        HostnameVerifier allHostsValid = new HostnameVerifier() {
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        };

        HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
    }

    private static JsonObject readConfigurationFromFile(String filename) throws Exception {
        BufferedReader br = null;
        try {
            Gson gson = new Gson();

            br = new BufferedReader(new FileReader(filename));
            JsonObject configuration = gson.fromJson(br, JsonElement.class).getAsJsonObject();
            return configuration;
        }
        catch (JsonSyntaxException ex) {
            System.out.println("Failed to parse file '" + filename + "'. Please ensure the file has correct json format.");
            throw ex;
        }
        finally {
            if (br != null) {
                br.close();
            }
        }
    }
}

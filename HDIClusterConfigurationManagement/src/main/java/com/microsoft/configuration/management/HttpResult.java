package com.microsoft.configuration.management;
/**
 * Created by jeezra on 3/10/2016.
 */
public class HttpResult {
    public int ResponseCode;
    public String Content;

    public HttpResult(int responseCode, String content)
    {
        this.ResponseCode = responseCode;
        this.Content = content;
    }
}

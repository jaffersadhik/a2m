package com.itextos.beacon.commonlib.httpclient;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.httpclient.helper.HttpUtility;

public sealed class BasicHttpConnector permits BasicHttpConnector.AsyncConnector {
    
    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(2);
    private static final Log log = LogFactory.getLog(BasicHttpConnector.class);
    
    // HTTP Client with modern Java HTTP Client API
    private static final HttpClient sharedHttpClient = HttpClient.newBuilder()
        .connectTimeout(DEFAULT_CONNECT_TIMEOUT)
        .followRedirects(HttpClient.Redirect.NORMAL)
        .executor(Executors.newVirtualThreadPerTaskExecutor()) // JDK 21+ virtual threads
        .build();

    private BasicHttpConnector() {}

    // Synchronous methods
    public static HttpResult connect(String completeUrl) {
        return connect(completeUrl, false);
    }

    public static HttpResult connect(String completeUrl, boolean returnResponseErrorString) {
        return connect(completeUrl, DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT, returnResponseErrorString);
    }

    public static HttpResult connect(String completeUrl, Duration connectTimeout, Duration readTimeout) {
        return connect(completeUrl, connectTimeout, readTimeout, false);
    }

    public static HttpResult connect(String completeUrl, Duration connectTimeout, 
                                   Duration readTimeout, boolean returnResponseErrorString) {
        return connect(completeUrl, null, null, connectTimeout, readTimeout, returnResponseErrorString);
    }

    public static HttpResult connect(String url, HttpParameter<String, String> parameterMap) {
        return connect(url, parameterMap, false);
    }

    public static HttpResult connect(String url, HttpParameter<String, String> parameterMap, 
                                   boolean returnResponseErrorString) {
        return connect(url, parameterMap, DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT, returnResponseErrorString);
    }

    public static HttpResult connect(String url, HttpParameter<String, String> parameterMap,
                                   Duration connectTimeout, Duration readTimeout) {
        return connect(url, parameterMap, connectTimeout, readTimeout, false);
    }

    public static HttpResult connect(String url, HttpParameter<String, String> parameterMap,
                                   Duration connectTimeout, Duration readTimeout, 
                                   boolean returnResponseErrorString) {
        return connect(url, parameterMap, null, connectTimeout, readTimeout, returnResponseErrorString);
    }

    public static HttpResult connect(String url, HttpParameter<String, String> parameterMap,
                                   HttpHeader<String, String> headerMap) {
        final String completeUrl = HttpUtility.populateParameters(url, parameterMap);
        return connect(completeUrl, headerMap, DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT, false);
    }

    public static HttpResult connect(String url, HttpParameter<String, String> parameterMap,
                                   HttpHeader<String, String> headerMap, Duration connectTimeout,
                                   Duration readTimeout) {
        return connect(url, parameterMap, headerMap, connectTimeout, readTimeout, false);
    }

    public static HttpResult connect(String url, HttpParameter<String, String> parameterMap,
                                   HttpHeader<String, String> headerMap, Duration connectTimeout,
                                   Duration readTimeout, boolean returnResponseErrorString) {
        final String completeUrl = HttpUtility.populateParameters(url, parameterMap);
        return connect(completeUrl, headerMap, connectTimeout, readTimeout, returnResponseErrorString);
    }

    public static HttpResult connect(String completeUrl, HttpHeader<String, String> headerMap) {
        return connect(completeUrl, headerMap, DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT, false);
    }

    // Main connection method using modern HTTP Client
    private static HttpResult connect(String completeUrl, HttpHeader<String, String> headerMap,
                                    Duration connectTimeout, Duration readTimeout,
                                    boolean returnResponseErrorString) {
        
        if (log.isDebugEnabled()) {
            log.debug("URL to connect: '" + completeUrl + "'");
        }

        final HttpResult result = new HttpResult();

        try {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(completeUrl))
                .timeout(readTimeout)
                .GET(); // Default to GET, you can extend for other methods

            // Add headers if provided
            if (headerMap != null) {
                headerMap.forEach((key, value) -> 
                    requestBuilder.header(key, value));
            }

            HttpRequest request = requestBuilder.build();

            // Use the shared HTTP client with virtual threads
            HttpResponse<String> response = sharedHttpClient.send(request, 
                HttpResponse.BodyHandlers.ofString());

            final int statusCode = response.statusCode();

            if (log.isDebugEnabled()) {
                log.debug("Status: '" + statusCode + "', URL [" + completeUrl + "]");
            }

            result.setSuccess(true);
            result.setStatusCode(statusCode);

            if (returnResponseErrorString) {
                result.setResponseString(response.body());
                // For error stream, we'd need to handle differently in new API
                result.setErrorString(statusCode >= 400 ? response.body() : "");
            } else {
                result.setResponseString(response.body());
            }

        } catch (final Exception e) {
            log.error("Exception while connecting to URL '" + completeUrl + "'", e);
            handleModernException(e, result);
        }

        if (log.isDebugEnabled()) {
            log.debug("Result: '" + result + "', URL [" + completeUrl + "]");
        }

        return result;
    }

    private static void handleModernException(Exception e, HttpResult result) {
        result.setSuccess(false);
        result.setException(e);
        
        if (e instanceof java.net.http.HttpTimeoutException) {
            result.setStatusCode(408); // Request Timeout
        } else if (e instanceof java.net.ConnectException) {
            result.setStatusCode(503); // Service Unavailable
        } else if (e instanceof java.net.UnknownHostException) {
            result.setStatusCode(502); // Bad Gateway
        } else {
            result.setStatusCode(500); // Internal Server Error
        }
    }

    // Async methods for non-blocking operations
    public static CompletableFuture<HttpResult> connectAsync(String completeUrl) {
        return connectAsync(completeUrl, false);
    }

    public static CompletableFuture<HttpResult> connectAsync(String completeUrl, 
                                                           boolean returnResponseErrorString) {
        return connectAsync(completeUrl, null, DEFAULT_CONNECT_TIMEOUT, 
                          DEFAULT_READ_TIMEOUT, returnResponseErrorString);
    }

    public static CompletableFuture<HttpResult> connectAsync(String completeUrl, 
                                                           HttpHeader<String, String> headerMap,
                                                           Duration connectTimeout, 
                                                           Duration readTimeout,
                                                           boolean returnResponseErrorString) {
        
        if (log.isDebugEnabled()) {
            log.debug("Async URL to connect: '" + completeUrl + "'");
        }

        try {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(completeUrl))
                .timeout(readTimeout)
                .GET();

            if (headerMap != null) {
                headerMap.forEach((key, value) -> 
                    requestBuilder.header(key, value));
            }

            HttpRequest request = requestBuilder.build();

            return sharedHttpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    HttpResult result = new HttpResult();
                    result.setSuccess(true);
                    result.setStatusCode(response.statusCode());
                    result.setResponseString(response.body());
                    
                    if (returnResponseErrorString && response.statusCode() >= 400) {
                        result.setErrorString(response.body());
                    }
                    
                    return result;
                })
                .exceptionally(throwable -> {
                    HttpResult result = new HttpResult();
                    handleModernException((Exception) throwable.getCause(), result);
                    return result;
                });

        } catch (final Exception e) {
            log.error("Exception while creating async request for URL '" + completeUrl + "'", e);
            HttpResult result = new HttpResult();
            handleModernException(e, result);
            return CompletableFuture.completedFuture(result);
        }
    }

    // Legacy compatibility methods (if you need to maintain backward compatibility)
    public static HttpResult connectLegacy(String completeUrl, HttpHeader<String, String> headerMap,
                                         int connectTimeoutMillis, int readTimeoutMillis,
                                         boolean returnResponseErrorString) {
        return connect(completeUrl, headerMap, 
                      Duration.ofMillis(connectTimeoutMillis),
                      Duration.ofMillis(readTimeoutMillis),
                      returnResponseErrorString);
    }

    // Configuration class for custom HTTP client settings
    public static final class HttpClientConfig {
        private Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT;
        private Duration readTimeout = DEFAULT_READ_TIMEOUT;
        private HttpClient.Redirect redirectPolicy = HttpClient.Redirect.NORMAL;
        private ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        public HttpClientConfig connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public HttpClientConfig readTimeout(Duration readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        public HttpClientConfig redirectPolicy(HttpClient.Redirect redirectPolicy) {
            this.redirectPolicy = redirectPolicy;
            return this;
        }

        public HttpClientConfig executor(ExecutorService executor) {
            this.executor = executor;
            return this;
        }

        public HttpClient buildClient() {
            return HttpClient.newBuilder()
                .connectTimeout(connectTimeout)
                .followRedirects(redirectPolicy)
                .executor(executor)
                .build();
        }
    }

    // Specialized async connector
    public static final class AsyncConnector extends BasicHttpConnector {
        private final HttpClient asyncClient;

        private AsyncConnector(HttpClientConfig config) {
            this.asyncClient = config.buildClient();
        }

        public static AsyncConnector create(HttpClientConfig config) {
            return new AsyncConnector(config);
        }

        public CompletableFuture<HttpResult> sendAsync(HttpRequest.Builder requestBuilder) {
            return asyncClient.sendAsync(requestBuilder.build(), HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    HttpResult result = new HttpResult();
                    result.setSuccess(true);
                    result.setStatusCode(response.statusCode());
                    result.setResponseString(response.body());
                    return result;
                });
        }
    }

    // Utility method to create custom configured clients
    public static AsyncConnector createAsyncConnector(HttpClientConfig config) {
        return AsyncConnector.create(config);
    }
}
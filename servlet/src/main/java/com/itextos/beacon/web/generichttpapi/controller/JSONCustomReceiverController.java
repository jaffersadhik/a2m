package com.itextos.beacon.web.generichttpapi.controller;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.itextos.beacon.commonlib.constants.InterfaceType;
import com.itextos.beacon.commonlib.constants.MiddlewareConstant;
import com.itextos.beacon.commonlib.prometheusmetricsutil.PrometheusMetrics;
import com.itextos.beacon.http.generichttpapi.common.utils.APIConstants;
import com.itextos.beacon.http.generichttpapi.common.utils.InterfaceInputParameters;
import com.itextos.beacon.http.generichttpapi.common.utils.Utility;
import com.itextos.beacon.http.interfaceutil.MessageSource;
import com.itextos.beacon.interfaces.generichttpapi.processor.reader.JSONRequestReader;
import com.itextos.beacon.interfaces.generichttpapi.processor.reader.RequestReader;
import com.itextos.beacon.smslog.JSONReceiverLog;
import com.itextos.beacon.smslog.TimeTakenInterfaceLog;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@RestController
@RequestMapping("/genericapi/JSONCustomReceiver")
public class JSONCustomReceiverController {

    private static final Log log = LogFactory.getLog(JSONCustomReceiverController.class);

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<String> handleJsonGetRequest(
            @RequestHeader(value = "X-Forwarded-For", required = false) String clientIp
            ,@RequestHeader(value = "Authorization", required = false) String authorization) {
        
        return processJsonRequest("GET", "", clientIp,authorization);
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<String> handleJsonPostRequest(
            @RequestBody Mono<String> requestBody,
            @RequestHeader(value = "X-Forwarded-For", required = false) String clientIp
            ,@RequestHeader(value = "Authorization", required = false) String authorization) {
        
        return requestBody.flatMap(body -> processJsonRequest("POST", body, clientIp,authorization));
    }

    private Mono<String> processJsonRequest(String method, String requestBody, String clientIp,String authorization) {
        final Instant startTime = Instant.now();
        final StringBuffer logBuffer = new StringBuffer();
        
        logBuffer.append("\n##########################################\n")
                .append("JSONGenericReceiver request received in ").append(method).append("\n");

        if (log.isDebugEnabled()) {
            log.debug("JSON request received via " + method);
        }

        Map<String, String> params=new HashMap<String,String>();
        
        params.put(MiddlewareConstant.MW_CLIENT_SOURCE_IP.getKey(), clientIp);
        
        params.put(InterfaceInputParameters.AUTHORIZATION, authorization);
        
        params.put("http_request_body", requestBody);
        
        // Track metrics
        PrometheusMetrics.apiIncrementAcceptCount(
            InterfaceType.HTTP_JAPI, 
            MessageSource.GENERIC_JSON, 
            APIConstants.CLUSTER_INSTANCE, 
            clientIp != null ? clientIp : "unknown"
        );

        // Process request reactively
        return Mono.fromCallable(new Callable<String>() {
            @Override
            public String call() throws Exception {
                try {
                   
                    final RequestReader reader = new JSONRequestReader(params, method, MessageSource.GENERIC_JSON, new StringBuffer());

                    
                    if ("GET".equalsIgnoreCase(method)) {
                        return reader.processGetRequest();
                    } else {
                        return reader.processPostRequest();
                    }
                    
                } catch (Exception e) {
                    log.error("Error processing JSON request", e);
                    throw new RuntimeException("JSON processing failed", e);
                }
            }
        })
        .subscribeOn(Schedulers.boundedElastic())
        .doOnSuccess(response -> {
            final Instant endTime = Instant.now();
            completeProcessing(startTime, endTime, logBuffer, method, true);
        })
        .doOnError(error -> {
            log.error("Exception processing JSON request via " + method, error);
            final Instant endTime = Instant.now();
            completeProcessing(startTime, endTime, logBuffer, method, false);
        })
        .onErrorResume(error -> {
            return Mono.just("{\"status\": \"error\", \"message\": \"" + error.getMessage() + "\"}");
        });
    }

    private void completeProcessing(Instant start, Instant end, StringBuffer logBuffer, String method, boolean success) {
        final long duration = java.time.Duration.between(start, end).toMillis();
        
        // Add timing information to log buffer
        logBuffer.append("Request Start time : '").append(Utility.getFormattedDateTime(start.toEpochMilli()))
                .append("' End time : '").append(Utility.getFormattedDateTime(end.toEpochMilli()))
                .append("' Processing time : '").append(duration).append(" milliseconds\n")
                .append("Status : ").append(success ? "SUCCESS" : "FAILED").append("\n")
                .append("\n##########################################\n");

        // Log to TimeTakenInterfaceLog
        TimeTakenInterfaceLog.log("Request Start time : '" + Utility.getFormattedDateTime(start.toEpochMilli()) + 
                "' End time : '" + Utility.getFormattedDateTime(end.toEpochMilli()) + 
                "' Processing time : '" + duration + "' milliseconds");

        // Log to JSONReceiverLog
        JSONReceiverLog.log(logBuffer.toString());

        // Log to application logs
        if (log.isInfoEnabled()) {
            log.info(method + " JSON request processed in " + duration + "ms - " + 
                    (success ? "SUCCESS" : "FAILED"));
        }
    }
}
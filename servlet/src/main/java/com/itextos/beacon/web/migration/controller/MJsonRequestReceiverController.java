package com.itextos.beacon.web.migration.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import com.itextos.beacon.commonlib.constants.InterfaceType;
import com.itextos.beacon.commonlib.constants.MiddlewareConstant;
import com.itextos.beacon.commonlib.utility.ClientIP;
import com.itextos.beacon.http.generichttpapi.common.utils.APIConstants;
import com.itextos.beacon.http.generichttpapi.common.utils.InterfaceInputParameters;
import com.itextos.beacon.http.generichttpapi.common.utils.Utility;
import com.itextos.beacon.http.interfaceutil.MessageSource;
import com.itextos.beacon.interfaces.generichttpapi.processor.reader.JSONRequestReader;
import com.itextos.beacon.interfaces.generichttpapi.processor.reader.RequestReader;
import com.itextos.beacon.interfaces.migration.processor.reader.MJsonRequestReader;
import com.itextos.beacon.interfaces.migration.processor.reader.MRequestReader;
import com.itextos.beacon.smslog.JSONReceiverLog;
import com.itextos.beacon.smslog.TimeTakenInterfaceLog;
import com.itextos.beacon.web.generichttpapi.controller.JSONGenericReceiverController;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

@RestController
@RequestMapping("/migrationapi/MJsonRequestReceiver")
public class MJsonRequestReceiverController {

    private static final Log log = LogFactory.getLog(JSONGenericReceiverController.class);

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<String> handleJsonGetRequest(
            @RequestHeader(value = "X-Forwarded-For", required = false) String clientIp
            ,@RequestHeader(value = "Authorization", required = false) String authorization,
            ServerHttpRequest request) {
        
        return processJsonRequest("GET", "", clientIp,authorization,request);
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<String> handleJsonPostRequest(
            @RequestBody Mono<String> requestBody,
            @RequestHeader(value = "X-Forwarded-For", required = false) String clientIp
            ,@RequestHeader(value = "Authorization", required = false) String authorization,
            ServerHttpRequest request) {
        
        return requestBody.flatMap(body -> processJsonRequest("POST", body, clientIp,authorization,request));
    }

    private Mono<String> processJsonRequest(String method, String requestBody, String clientIp,String authorization,
            ServerHttpRequest request) {
        final Instant startTime = Instant.now();
        final StringBuffer logBuffer = new StringBuffer();
        
        logBuffer.append("\n##########################################\n")
                .append("JSONGenericReceiver request received in ").append(method).append("\n");

        if (log.isDebugEnabled()) {
            log.debug("JSON request received via " + method);
        }

        Map<String, String> params=new HashMap<String,String>();
        
        params.put(MiddlewareConstant.MW_CLIENT_SOURCE_IP.getKey(), ClientIP.getClientIpAddress(clientIp, request));
        
        params.put(InterfaceInputParameters.AUTHORIZATION, authorization);
        
        params.put("http_request_body", requestBody);
        
        

        // Process request reactively
        return Mono.fromCallable(new Callable<String>() {
            @Override
            public String call() throws Exception {
                try {
                   
                	final MRequestReader lMRequestReader = new MJsonRequestReader(params, MessageSource.GENERIC_JSON, MessageSource.GENERIC_JSON,new StringBuffer());

                    
                   
                        return lMRequestReader.processPostRequest();

                  
                    
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
package com.itextos.beacon.web.generichttpapi.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.MediaType;
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
import com.itextos.beacon.smslog.TimeTakenInterfaceLog;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Callable;

@RestController
@RequestMapping("/genericapi/QSCustomReceiver")
public class QSCustomReceiverController {

    private static final Log log = LogFactory.getLog(QSCustomReceiverController.class);

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<String> handleCustomQSGet(
            @RequestParam Map<String, String> allParams,
            @RequestHeader(value = "X-Forwarded-For", required = false) String clientIp
            ,@RequestHeader(value = "Authorization", required = false) String authorization,
            ServerHttpRequest request) {
        
        return processCustomQSRequest("GET", allParams, clientIp,authorization,request);
    }

    @PostMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<String> handleCustomQSPost(
            @RequestBody(required = false) Mono<String> requestBody,
            @RequestParam Map<String, String> allParams,
            @RequestHeader(value = "X-Forwarded-For", required = false) String clientIp
            ,@RequestHeader(value = "Authorization", required = false) String authorization,
            ServerHttpRequest request) {
        
        return requestBody.defaultIfEmpty("")
            .flatMap(body -> processCustomQSRequest("POST", allParams, clientIp,authorization,request));
    }

    private Mono<String> processCustomQSRequest(String method, Map<String, String> params, String clientIp, String authorization,
            ServerHttpRequest request) {
        final Instant startTime = Instant.now();
        
        if (log.isDebugEnabled()) {
            log.debug("Custom QS request received via " + method);
        }
        

        params.put(MiddlewareConstant.MW_CLIENT_SOURCE_IP.getKey(), ClientIP.getClientIpAddress(clientIp, request));
        
        params.put(InterfaceInputParameters.AUTHORIZATION, authorization);

        

        // Process request reactively
        return Mono.fromCallable(new Callable<String>() {
            @Override
            public String call() throws Exception {
                try {
                    StringBuffer sb = new StringBuffer();
                    
                    // Create reactive adapter for your QSRequestReader
                    ReactiveCustomQSReader reactiveReader = new ReactiveCustomQSReader(
                        params, method, "custom", sb
                    );
                    
                    return reactiveReader.processGetRequest();
                    
                } catch (Exception e) {
                    log.error("Error processing custom QS request", e);
                    return "{\"status\": \"error\", \"message\": \"Processing failed\"}";
                }
            }
        })
        .subscribeOn(Schedulers.boundedElastic())
        .doOnSuccess(response -> {
            final Instant endTime = Instant.now();
            logProcessingTime(startTime, endTime, method);
        })
        .doOnError(error -> {
            log.error("Exception processing custom QS request", error);
            final Instant endTime = Instant.now();
            logProcessingTime(startTime, endTime, method);
        });
    }

    private void logProcessingTime(Instant start, Instant end, String method) {
        final long duration = java.time.Duration.between(start, end).toMillis();
        
        // Log to TimeTakenInterfaceLog
        TimeTakenInterfaceLog.log("Request Start time : '" + Utility.getFormattedDateTime(start.toEpochMilli()) + 
                "' End time : '" + Utility.getFormattedDateTime(end.toEpochMilli()) + 
                "' Processing time : '" + duration + "' milliseconds");

        // Log to application logs
        if (log.isInfoEnabled()) {
            log.info(method + " Request Start time: '" + Utility.getFormattedDateTime(start.toEpochMilli()) + 
                    "' End time: '" + Utility.getFormattedDateTime(end.toEpochMilli()) + 
                    "' Processing time: '" + duration + "' milliseconds");
        }
    }
}
package com.itextos.beacon.web.generichttpapi.controller;

import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.itextos.beacon.commonlib.constants.InterfaceType;
import com.itextos.beacon.commonlib.constants.MiddlewareConstant;
import com.itextos.beacon.commonlib.utility.ClientIP;
import com.itextos.beacon.http.generichttpapi.common.utils.APIConstants;
import com.itextos.beacon.http.generichttpapi.common.utils.InterfaceInputParameters;
import com.itextos.beacon.http.generichttpapi.common.utils.Utility;
import com.itextos.beacon.http.interfaceutil.MessageSource;
import com.itextos.beacon.smslog.QSReceiverLog;
import com.itextos.beacon.smslog.TimeTakenInterfaceLog;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@RestController
@RequestMapping("/genericapi/QSGenericReceiver")
public class QSGenericReceiverController {

    private static final Log log = LogFactory.getLog(QSGenericReceiverController.class);

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<String> handleQSGetRequest(
            @RequestParam java.util.Map<String, String> allParams,
            @RequestHeader(value = "X-Forwarded-For", required = false) String clientIp,@RequestHeader(value = "Authorization", required = false) String authorization,
            ServerHttpRequest request) {
        
        return processQSRequest("GET", allParams, clientIp,authorization,request);
    }

    @PostMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<String> handleQSPostRequest(
            @RequestBody(required = false) Mono<String> requestBody,
            @RequestParam java.util.Map<String, String> allParams,
            @RequestHeader(value = "X-Forwarded-For", required = false) String clientIp,@RequestHeader(value = "Authorization", required = false) String authorization,
            ServerHttpRequest request) {
        
        return requestBody.defaultIfEmpty("")
            .flatMap(body -> processQSRequest("POST", allParams, clientIp,authorization,request));
    }

    private Mono<String> processQSRequest(String method, java.util.Map<String, String> params, String clientIp,String authorization,
            ServerHttpRequest request) {
        final Instant processStart = Instant.now();
        final AtomicReference<StringBuffer> logBuffer = new AtomicReference<>(new StringBuffer());
        
        if (log.isDebugEnabled()) {
            log.debug("QS request received via " + method);
        }

        params.put(MiddlewareConstant.MW_CLIENT_SOURCE_IP.getKey(),ClientIP.getClientIpAddress(clientIp, request));
        
        params.put(InterfaceInputParameters.AUTHORIZATION, authorization);

        // Initialize log buffer
        StringBuffer sb = new StringBuffer();
        sb.append("\n##########################################\n");
        sb.append("QS request received in ").append(method).append("\n");
        logBuffer.set(sb);

     

        // Process request reactively
        return Mono.fromCallable(new Callable<String>() {
            @Override
            public String call() throws Exception {
                try {
                    // Adapt your existing QSRequestReader to reactive context
                    ReactiveQSRequestReader reactiveReader = new ReactiveQSRequestReader(
                        params, method, MessageSource.GENERIC_QS, logBuffer.get()
                    );
                    return reactiveReader.processRequest();
                } catch (Exception e) {
                    log.error("Error processing QS request", e);
                    return "{\"status\": \"error\", \"message\": \"Processing failed\"}";
                }
            }
        })
        .subscribeOn(Schedulers.boundedElastic())
        .doOnSuccess(response -> {
            final Instant processEnd = Instant.now();
            final long processTaken = java.time.Duration.between(processStart, processEnd).toMillis();
            
            // Log processing time
            logProcessingTime(processStart, processEnd, processTaken, logBuffer.get());
        })
        .doOnError(error -> {
            log.error("Exception processing QS request", error);
            final Instant processEnd = Instant.now();
            final long processTaken = java.time.Duration.between(processStart, processEnd).toMillis();
            logProcessingTime(processStart, processEnd, processTaken, logBuffer.get());
        });
    }

    private void logProcessingTime(Instant start, Instant end, long duration, StringBuffer sb) {
        if (log.isInfoEnabled()) {
            log.info("Request Start time: '" + Utility.getFormattedDateTime(start.toEpochMilli()) + 
                    "' End time: '" + Utility.getFormattedDateTime(end.toEpochMilli()) + 
                    "' Processing time: '" + duration + "' milliseconds");
        }
        
        sb.append("Request Start time : '" + Utility.getFormattedDateTime(start.toEpochMilli()) + 
                 "' End time : '" + Utility.getFormattedDateTime(end.toEpochMilli()) + 
                 "' Processing time : '" + duration + "' milliseconds").append("\n");
        sb.append("\n##########################################\n");

        // Log to external systems
        TimeTakenInterfaceLog.log("Request Start time : '" + Utility.getFormattedDateTime(start.toEpochMilli()) + 
                "' End time : '" + Utility.getFormattedDateTime(end.toEpochMilli()) + 
                "' Processing time : '" + duration + "' milliseconds");

        QSReceiverLog.log(sb.toString());
    }
}
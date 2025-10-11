package com.itextos.beacon.platform.dnr.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import com.itextos.beacon.commonlib.constants.InterfaceType;
import com.itextos.beacon.commonlib.constants.MiddlewareConstant;
import com.itextos.beacon.http.generichttpapi.common.utils.APIConstants;
import com.itextos.beacon.http.generichttpapi.common.utils.InterfaceInputParameters;
import com.itextos.beacon.http.generichttpapi.common.utils.Utility;
import com.itextos.beacon.http.interfaceparameters.InterfaceParameter;
import com.itextos.beacon.http.interfaceutil.MessageSource;
import com.itextos.beacon.smslog.QSReceiverLog;
import com.itextos.beacon.smslog.TimeTakenInterfaceLog;
import com.mysql.cj.protocol.x.MessageConstants;

import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

@RestController
@RequestMapping("/dnr/dlrreceiver")
public class DLRReceiverController {

    private static final Log log = LogFactory.getLog(DLRReceiverController.class);

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<String> handleQSGetRequest(
            @RequestParam java.util.Map<String, String> allParams,
            @RequestHeader(value = "X-Forwarded-For", required = false) String clientIp,@RequestHeader(value = "Authorization", required = false) String authorization) {
        
        return processQSRequest("GET", allParams, clientIp,authorization);
    }

    @PostMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<String> handleQSPostRequest(
            @RequestBody(required = false) Mono<String> requestBody,
            @RequestParam java.util.Map<String, String> allParams,
            @RequestHeader(value = "X-Forwarded-For", required = false) String clientIp,@RequestHeader(value = "Authorization", required = false) String authorization) {
        
        return requestBody.defaultIfEmpty("")
            .flatMap(body -> processQSRequest("POST", allParams, clientIp,authorization));
    }

    private Mono<String> processQSRequest(String method, java.util.Map<String, String> params, String clientIp,String authorization) {
        final Instant processStart = Instant.now();
        final AtomicReference<StringBuffer> logBuffer = new AtomicReference<>(new StringBuffer());
        
        if (log.isDebugEnabled()) {
            log.debug("QS request received via " + method);
        }

        params.put(MiddlewareConstant.MW_CLIENT_SOURCE_IP.getKey(), clientIp);
        
        params.put(InterfaceInputParameters.AUTHORIZATION, authorization);

        // Initialize log buffer
        StringBuffer sb = new StringBuffer();
        sb.append("\n##########################################\n");
        sb.append("QS request received in ").append(method).append("\n");
        logBuffer.set(sb);

        // Track metrics
        

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
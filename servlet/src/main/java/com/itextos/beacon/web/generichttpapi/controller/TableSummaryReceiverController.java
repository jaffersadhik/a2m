package com.itextos.beacon.web.generichttpapi.controller;

import java.time.Instant;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.itextos.beacon.commonlib.constants.MiddlewareConstant;
import com.itextos.beacon.commonlib.utility.ClientIP;
import com.itextos.beacon.http.generichttpapi.common.utils.InterfaceInputParameters;
import com.itextos.beacon.http.generichttpapi.common.utils.Utility;
import com.itextos.beacon.inmemory.loader.process.InmemoryProcessor;
import com.itextos.beacon.smslog.QSReceiverLog;
import com.itextos.beacon.smslog.TimeTakenInterfaceLog;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/inmemory/tablesummary")
public class TableSummaryReceiverController {

    private static final Log log = LogFactory.getLog(TableSummaryReceiverController.class);

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> handleQSGetRequest(
            @RequestParam java.util.Map<String, String> allParams,
            @RequestHeader(value = "X-Forwarded-For", required = false) String clientIp,
            @RequestHeader(value = "Authorization", required = false) String authorization,
            HttpServletRequest request) {
        
        return processQSRequest("GET", allParams, clientIp, authorization, request, null);
    }

    @PostMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> handleQSPostRequest(
            @RequestBody(required = false) String requestBody,
            @RequestParam java.util.Map<String, String> allParams,
            @RequestHeader(value = "X-Forwarded-For", required = false) String clientIp,
            @RequestHeader(value = "Authorization", required = false) String authorization,
            HttpServletRequest request) {
        
        return processQSRequest("POST", allParams, clientIp, authorization, request, requestBody);
    }

    private ResponseEntity<String> processQSRequest(String method, java.util.Map<String, String> params, 
                                                   String clientIp, String authorization,
                                                   HttpServletRequest request, String requestBody) {
        final Instant processStart = Instant.now();
        final StringBuffer logBuffer = new StringBuffer();
        
        if (log.isDebugEnabled()) {
            log.debug("QS request received via " + method);
        }

        params.put(MiddlewareConstant.MW_CLIENT_SOURCE_IP.getKey(), ClientIP.getClientIpAddress(clientIp, request));
        params.put(InterfaceInputParameters.AUTHORIZATION, authorization);

        // Initialize log buffer
        logBuffer.append("\n##########################################\n");
        logBuffer.append("QS request received in ").append(method).append("\n");

        try {
            // Process request synchronously
            String response = InmemoryProcessor.TABLESUMMARY.toString();
            
            final Instant processEnd = Instant.now();
            final long processTaken = java.time.Duration.between(processStart, processEnd).toMillis();
            
            // Log processing time for success case
            logProcessingTime(processStart, processEnd, processTaken, logBuffer);
            
            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(response);
                    
        } catch (Exception e) {
            log.error("Exception processing QS request", e);
            final Instant processEnd = Instant.now();
            final long processTaken = java.time.Duration.between(processStart, processEnd).toMillis();
            
            // Log processing time for error case
            logProcessingTime(processStart, processEnd, processTaken, logBuffer);
            
            return ResponseEntity.badRequest()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body("{\"status\": \"error\", \"message\": \"Processing failed: " + e.getMessage() + "\"}");
        }
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
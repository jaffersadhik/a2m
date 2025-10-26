package com.itextos.beacon.platform.dnr.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.itextos.beacon.commonlib.constants.MiddlewareConstant;
import com.itextos.beacon.http.generichttpapi.common.utils.InterfaceInputParameters;
import com.itextos.beacon.http.generichttpapi.common.utils.Utility;
import com.itextos.beacon.http.interfaceutil.MessageSource;
import com.itextos.beacon.smslog.QSReceiverLog;
import com.itextos.beacon.smslog.TimeTakenInterfaceLog;

import java.time.Instant;

@RestController
@RequestMapping("/dnr/dlrreceiver")
public class DLRReceiverController {

    private static final Log log = LogFactory.getLog(DLRReceiverController.class);

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> handleQSGetRequest(
            @RequestParam java.util.Map<String, String> allParams,
            @RequestHeader(value = "X-Forwarded-For", required = false) String clientIp,
            @RequestHeader(value = "Authorization", required = false) String authorization) {
        
        return processQSRequest("GET", allParams, clientIp, authorization, null);
    }

    @PostMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> handleQSPostRequest(
            @RequestBody(required = false) String requestBody,
            @RequestParam java.util.Map<String, String> allParams,
            @RequestHeader(value = "X-Forwarded-For", required = false) String clientIp,
            @RequestHeader(value = "Authorization", required = false) String authorization) {
        
        return processQSRequest("POST", allParams, clientIp, authorization, requestBody);
    }

    private ResponseEntity<String> processQSRequest(String method, java.util.Map<String, String> params, 
                                                   String clientIp, String authorization, String requestBody) {
        final Instant processStart = Instant.now();
        final StringBuffer logBuffer = new StringBuffer();
        
        if (log.isDebugEnabled()) {
            log.debug("QS request received via " + method);
        }

        params.put(MiddlewareConstant.MW_CLIENT_SOURCE_IP.getKey(), clientIp);
        params.put(InterfaceInputParameters.AUTHORIZATION, authorization);

        // Initialize log buffer
        logBuffer.append("\n##########################################\n");
        logBuffer.append("QS request received in ").append(method).append("\n");

        try {
            // Process request synchronously
            String response = processRequestSync(params, method, logBuffer, requestBody);
            
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

    private String processRequestSync(java.util.Map<String, String> params, String method, 
                                     StringBuffer logBuffer, String requestBody) throws Exception {
        // Use your existing synchronous QSRequestReader
        // Replace ReactiveQSRequestReader with your original synchronous implementation
        QSRequestReader requestReader = new QSRequestReader(
            params, method, MessageSource.GENERIC_QS, logBuffer
        );
        return requestReader.processRequest();
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
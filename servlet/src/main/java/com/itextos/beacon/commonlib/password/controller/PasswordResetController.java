package com.itextos.beacon.commonlib.password.controller;

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
@RequestMapping("/password/reset")
public class PasswordResetController {

    private static final Log log = LogFactory.getLog(PasswordResetController.class);

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
            QSRequestReader requestReader = new QSRequestReader(
                params, method, MessageSource.GENERIC_QS, logBuffer
            );
            String response = requestReader.processRequest();
            
            final Instant processEnd = Instant.now();
            final long processTaken = java.time.Duration.between(processStart, processEnd).toMillis();
            
            // Log processing time
            logProcessingTime(processStart, processEnd, processTaken, logBuffer);
            
            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(response);
                    
        } catch (Exception e) {
            log.error("Exception processing QS request", e);
            final Instant processEnd = Instant.now();
            final long processTaken = java.time.Duration.between(processStart, processEnd).toMillis();
            
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

    // Synchronous QSRequestReader class
    private static class QSRequestReader {
        private java.util.Map<String, String> params;
        private String method;
        private String messageSource;
        private StringBuffer logBuffer;

        public QSRequestReader(java.util.Map<String, String> params, String method, 
                              String messageSource, StringBuffer logBuffer) {
            this.params = params;
            this.method = method;
            this.messageSource = messageSource;
            this.logBuffer = logBuffer;
        }

        public String processRequest() throws Exception {
            // Your existing synchronous processing logic for password reset
            logBuffer.append("Processing password reset request synchronously\n");
            logBuffer.append("Method: ").append(method).append("\n");
            logBuffer.append("Parameters: ").append(params).append("\n");
            
            // Implement your actual password reset logic here
            String result = executePasswordReset(params);
            
            logBuffer.append("Password reset completed successfully\n");
            
            return "{\"status\": \"success\", \"message\": \"Password reset processed successfully\"}";
        }
        
        private String executePasswordReset(java.util.Map<String, String> params) {
            // Your actual password reset business logic
            // This is a placeholder - replace with your real implementation
            String username = params.get("username");
            String newPassword = params.get("newPassword");
            
            // Perform password reset operations
            // ...
            
            return "Password reset successful for user: " + username;
        }
    }
}
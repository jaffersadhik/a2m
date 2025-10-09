package com.itextos.beacon.fileprocessor.controller;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.http.codec.multipart.Part;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;

import com.winnovature.dltfileprocessor.services.FileReadService;
import com.winnovature.dltfileprocessor.utils.Constants;
import com.winnovature.dltfileprocessor.utils.Utility;
import com.winnovature.dltfileprocessor.utils.ZipHandler;
import com.winnovature.logger.DLTFileLog;
import com.winnovature.utils.singletons.ConfigParamsTon;
import com.winnovature.utils.utils.JsonUtility;
import com.winnovature.utils.utils.UploadedFilesTrackingUtility;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@RestController
@RequestMapping("/FP-DltFileProcessor-0.0.1/dlttemplate")
public class DltTemplatesFilesSaverController {

    private static final DLTFileLog log = DLTFileLog.getInstance();
    private Map<String, String> configMap = null;

    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<Map<String, Object>> handleFileUpload(
            @RequestPart("username") String username,
            @RequestPart("telco") String telco,
            @RequestPart Map<String, Part> parts) {

        if (log.isDebugEnabled()) {
            log.debug("[DltTemplatesFilesSaver] [handleFileUpload] request received.");
        }

        Instant startTime = Instant.now();
        String requestFrom = Constants.DLT;

        // Validate required parameters
        if (StringUtils.isBlank(username)) {
            return createErrorResponse(Constants.ERROR_CODE_REQUIRED_PARAMS_MISSING,
                    "Bad Request", "Bad Request", HttpStatus.BAD_REQUEST);
        }

        if (StringUtils.isBlank(telco)) {
            return createErrorResponse(Constants.ERROR_CODE_REQUIRED_PARAMS_MISSING,
                    "Bad Request", "Bad Request", HttpStatus.BAD_REQUEST);
        }

        List<String> filesList = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Long> sentToTrackingRedis = new AtomicReference<>();

        return getFileStoreLocation(username)
                .flatMap(fileStoreLocation -> {
                    // Process all file parts
                    return Flux.fromIterable(parts.entrySet())
                            .filter(entry -> entry.getValue() instanceof FilePart)
                            .flatMap(entry -> processFilePart((FilePart) entry.getValue(), fileStoreLocation, filesList, startTime))
                            .collectList()
                            .flatMap(response -> {
                                // Process zip files and extract contents
                                return processZipFiles(response, fileStoreLocation, filesList)
                                        .then(Mono.just(response));
                            })
                            .flatMap(response -> {
                                // Add all files to tracking list
                                response.stream()
                                        .filter(map -> map != null && map.get("r_filename") != null)
                                        .map(map -> fileStoreLocation + map.get("r_filename").toString().trim())
                                        .forEach(filesList::add);

                                // Send files to tracking Redis
                                Long trackingResult = UploadedFilesTrackingUtility.setUploadedFilesInfo(requestFrom, username, filesList);
                                sentToTrackingRedis.set(trackingResult);

                                // Process files with FileReadService
                                return processFilesWithService(response, fileStoreLocation, username, telco, filesList);
                            })
                            .flatMap(processedResults -> {
                                // Build final response
                                return buildFinalResponse(processedResults);
                            });
                })
                .onErrorResume(throwable -> {
                    log.error("[DltTemplatesFilesSaver] [handleFileUpload] Exception", throwable);
                    // Ensure files are tracked for cleanup even on error
                    if (sentToTrackingRedis.get() == null) {
                        UploadedFilesTrackingUtility.setUploadedFilesInfo(requestFrom, username, filesList);
                    }
                    return createErrorResponse(Constants.INTERNAL_SERVER_ERROR_STATUS_CODE,
                            "Internal Server Error", "Server Error", HttpStatus.INTERNAL_SERVER_ERROR);
                })
                .doOnSuccess(response -> {
                    if (log.isDebugEnabled()) {
                        try {
                            String json = new JsonUtility().mapToJson(response);
                            log.debug("[DltTemplatesFilesSaver] [handleFileUpload] time taken to process request is "
                                    + Utility.getTimeDifference(startTime) + " milliseconds and response is " + json);
                        } catch (Exception e) {
                            log.debug("[DltTemplatesFilesSaver] [handleFileUpload] time taken to process request is "
                                    + Utility.getTimeDifference(startTime) + " milliseconds");
                        }
                    }
                });
    }

    private Mono<String> getFileStoreLocation(String username) {
        return Mono.fromCallable(() -> {
            configMap = ConfigParamsTon.getInstance().getConfigurationFromconfigParams();
            String fileStoreLocation = configMap.get(Constants.DLT_TEMPLATE_FILE_STORE_PATH);
            fileStoreLocation = fileStoreLocation + username.toLowerCase() + "/";
            Files.createDirectories(Paths.get(fileStoreLocation));
            return fileStoreLocation;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    private Mono<Map<String, Object>> processFilePart(FilePart filePart, String fileStoreLocation, 
                                                     List<String> filesList, Instant startTime) {
        return Mono.fromCallable(() -> {
            String originalFileName = filePart.filename();
            if (originalFileName == null) {
                return Collections.<String, Object>emptyMap();
            }

            String extension = "." + FilenameUtils.getExtension(originalFileName);
            UUID uuid = UUID.randomUUID();
            String storedFileName = StringUtils.replace(originalFileName, extension, "")
                    .concat("_" + uuid.toString()).concat(extension);

            // For CSV files, create a temporary file first
            if (extension.equalsIgnoreCase(".csv")) {
                String tempFileName = StringUtils.replace(originalFileName, extension, "")
                        .concat("_" + uuid.toString())
                        .concat("_" + com.winnovature.utils.utils.Utility.getCustomDateAsString("yyyy-MM-dd_HHmmssSSS"))
                        .concat(extension);
                Path csvTempPath = Paths.get(fileStoreLocation + tempFileName);
                
                // Transfer file content
                filePart.transferTo(csvTempPath).block();
                
                // Process CSV file
                com.winnovature.utils.utils.Utility.storeCSVFile(
                    fileStoreLocation + tempFileName, 
                    fileStoreLocation + storedFileName
                );
                filesList.add(fileStoreLocation + tempFileName);
            } else {
                // Transfer regular files directly
                Path filePath = Paths.get(fileStoreLocation + storedFileName);
                filePart.transferTo(filePath).block();
            }

            if (log.isDebugEnabled()) {
                log.debug("[DltTemplatesFilesSaver] [processFilePart] time taken to save " + originalFileName + " is "
                        + Utility.getTimeDifference(startTime) + " milliseconds.");
            }

            Map<String, Object> fileData = new HashMap<>();
            fileData.put("filename", originalFileName);
            fileData.put("r_filename", storedFileName);
            fileData.put("extension", extension.toLowerCase());

            return fileData;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    private Mono<Void> processZipFiles(List<Map<String, Object>> response, String fileStoreLocation, List<String> filesList) {
        return Flux.fromIterable(response)
                .filter(fileData -> ".zip".equalsIgnoreCase((String) fileData.get("extension")))
                .flatMap(fileData -> {
                    String storedFileName = (String) fileData.get("r_filename");
                    String originalFileName = (String) fileData.get("filename");
                    
                    return Mono.fromCallable(() -> {
                        Instant zipExtractStartTime = Instant.now();
                        List<Map<String, Object>> zipContent = new ZipHandler()
                                .extractZipFileContent(fileStoreLocation + storedFileName, fileStoreLocation);
                        
                        if (log.isDebugEnabled()) {
                            log.debug("[DltTemplatesFilesSaver] [processZipFiles] time taken to extract "
                                    + originalFileName + " is " + Utility.getTimeDifference(zipExtractStartTime)
                                    + " milliseconds.");
                        }
                        
                        // Add zip file to cleanup list
                        if (StringUtils.isNotBlank(storedFileName)) {
                            filesList.add(fileStoreLocation + storedFileName);
                        }
                        
                        // Add extracted files to response
                        response.addAll(zipContent);
                        return zipContent;
                    }).subscribeOn(Schedulers.boundedElastic());
                })
                .then();
    }

    private Mono<ProcessedResults> processFilesWithService(
            List<Map<String, Object>> response,
            String fileStoreLocation,
            String username,
            String telco,
            List<String> filesList) {

        // Add username to each file data
        response.forEach(fileData -> fileData.put("username", username));

        return Flux.fromIterable(response)
                .flatMap(fileData -> processSingleFile(fileData, fileStoreLocation, telco))
                .collectList()
                .map(fileResults -> {
                    List<Map<String, Object>> successFiles = new ArrayList<>();
                    List<Map<String, Object>> failedFiles = new ArrayList<>();
                    AtomicLong totalRecords = new AtomicLong(0);

                    // Process file results
                    for (Map<String, Object> result : fileResults) {
                        if (result.containsKey("error")) {
                            failedFiles.add(result);
                        } else {
                            long fileCount = Long.parseLong(result.get("count").toString());
                            if (fileCount < 1) {
                                // Remove files with 0 rows
                                Map<String, Object> errorResult = new HashMap<>();
                                errorResult.put("error", "Invalid File");
                                errorResult.put("message", "File is empty");
                                errorResult.put("filename", result.get("filename"));
                                errorResult.put("missing", new ArrayList<String>());
                                failedFiles.add(errorResult);
                            } else {
                                // Check if file is valid
                                boolean isValidFile = Boolean.parseBoolean(result.get("isValidFile").toString());
                                if (isValidFile) {
                                    successFiles.add(result);
                                    totalRecords.addAndGet(fileCount);
                                } else {
                                    Map<String, Object> errorResult = new HashMap<>();
                                    errorResult.put("error", "Invalid Template File");
                                    errorResult.put("message", "Invalid Template File");
                                    errorResult.put("filename", result.get("filename"));
                                    errorResult.put("missing", result.get("missing"));
                                    failedFiles.add(errorResult);
                                    
                                    if (log.isDebugEnabled()) {
                                        log.debug("[DltTemplatesFilesSaver] [processFilesWithService] File does not have all required columns.");
                                    }
                                }
                            }
                        }
                    }

                    return new ProcessedResults(successFiles, failedFiles, totalRecords.get());
                });
    }

    private Mono<Map<String, Object>> processSingleFile(Map<String, Object> fileData, String fileStoreLocation, String telco) {
        return Mono.fromCallable(() -> {
            try {
                FileReadService fileReadService = new FileReadService(fileData, fileStoreLocation, telco);
                return fileReadService.call();
            } catch (Exception e) {
                Map<String, Object> errorResult = new HashMap<>();
                if (e.getMessage().contains(Constants.UNSUPPORTED_FILE_TYPE)) {
                    errorResult.put("error", Constants.UNSUPPORTED_FILE_TYPE);
                    errorResult.put("message", Constants.UNSUPPORTED_FILE_TYPE);
                    if (StringUtils.split(e.getMessage(), "~").length > 1) {
                        errorResult.put("filename", StringUtils.split(e.getMessage(), "~")[1].trim());
                    }
                } else {
                    throw e;
                }
                return errorResult;
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    private Mono<Map<String, Object>> buildFinalResponse(ProcessedResults processedResults) {
        Map<String, Object> finalResponse = new HashMap<>();
        Map<String, Object> nestedResponse = new HashMap<>();
        
        nestedResponse.put("success", processedResults.successFiles);
        nestedResponse.put("failed", processedResults.failedFiles);

        finalResponse.put("total", processedResults.totalRecords);
        finalResponse.put("total_human", Utility.humanReadableNumberFormat(processedResults.totalRecords));
        finalResponse.put("uploaded_files", nestedResponse);
        finalResponse.put("statusCode", Constants.SUCCESS_STATUS_CODE);

        return Mono.just(finalResponse);
    }

    private Mono<Map<String, Object>> createErrorResponse(int statusCode, String error, String message, HttpStatus httpStatus) {
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("statusCode", statusCode);
        errorResponse.put("error", error);
        errorResponse.put("message", message);
        return Mono.just(errorResponse);
    }

    // Helper class to hold processing results
    private static class ProcessedResults {
        final List<Map<String, Object>> successFiles;
        final List<Map<String, Object>> failedFiles;
        final long totalRecords;

        ProcessedResults(List<Map<String, Object>> successFiles, List<Map<String, Object>> failedFiles, long totalRecords) {
            this.successFiles = successFiles;
            this.failedFiles = failedFiles;
            this.totalRecords = totalRecords;
        }
    }
}
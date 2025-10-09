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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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

import com.winnovature.fileuploads.services.FileReadService;
import com.winnovature.fileuploads.utils.Constants;
import com.winnovature.fileuploads.utils.Utility;
import com.winnovature.fileuploads.utils.ZipHandler;
import com.winnovature.logger.FileUploadLog;
import com.winnovature.utils.singletons.ConfigParamsTon;
import com.winnovature.utils.utils.JsonUtility;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@RestController
@RequestMapping("/FP-FileUpload-0.0.1/save")
public class FilesSaverController {

    private Map<String, String> configMap = null;

    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<Map<String, Object>> handleFileUpload(
            @RequestPart("username") String username,
            @RequestPart("frompage") String requestFrom,
            @RequestPart Map<String, Part> parts) {

        FileUploadLog.getInstance().debug("[FilesSaver] [handleFileUpload] request received.");

        Instant startTime = Instant.now();
        List<String> filesList = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean sentToTrackingRedis = new AtomicBoolean(false);

        // Validate required parameters
        if (StringUtils.isBlank(username)) {
            return createErrorResponse(Constants.APPLICATION_ERROR_CODE, Constants.APPLICATION_ERROR, 
                    "username is required", HttpStatus.BAD_REQUEST);
        }

        if (StringUtils.isBlank(requestFrom)) {
            return createErrorResponse(Constants.APPLICATION_ERROR_CODE, Constants.APPLICATION_ERROR, 
                    "frompage is required", HttpStatus.BAD_REQUEST);
        }

        return getFileStoreLocation(username, requestFrom)
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

                                FileUploadLog.getInstance().debug("filesList : " + filesList);

                                // Send files to tracking Redis
                                boolean trackingResult = Utility.sendFilesToTrackingRedis(requestFrom, username, filesList);
                                sentToTrackingRedis.set(trackingResult);
                                FileUploadLog.getInstance().debug("sentToTrackingRedis : " + trackingResult);

                                // Process files with FileReadService
                                return processFilesWithService(response, fileStoreLocation, filesList);
                            })
                            .flatMap(processedResults -> {
                                // Build final response
                                return buildFinalResponse(processedResults);
                            });
                })
                .onErrorResume(throwable -> {
                    FileUploadLog.getInstance().error("[FilesSaver] [handleFileUpload] Exception", throwable);

                    // Ensure files are tracked for cleanup even on error
                    if (!sentToTrackingRedis.get()) {
                        Utility.sendFilesToTrackingRedis(requestFrom, username, filesList);
                    }

                    return createErrorResponse(Constants.INTERNAL_SERVER_ERROR_STATUS_CODE, 
                            Constants.INTERNAL_SERVER_ERROR, Constants.GENERAL_ERROR_MESSAGE, 
                            HttpStatus.INTERNAL_SERVER_ERROR);
                })
                .doOnSuccess(response -> {
                    FileUploadLog.getInstance().debug("finalResponse : " + response);
                    
                    try {
                        String json = new JsonUtility().mapToJson(response);
                        FileUploadLog.getInstance().debug("[FilesSaver] response json : " + json);
                        FileUploadLog.getInstance().debug("[FilesSaver] [handleFileUpload] time taken to process request is "
                                + Utility.getTimeDifference(startTime) + " milliseconds.");
                    } catch (Exception e) {
                        FileUploadLog.getInstance().debug("[FilesSaver] [handleFileUpload] time taken to process request is "
                                + Utility.getTimeDifference(startTime) + " milliseconds.");
                    }
                });
    }

    private Mono<String> getFileStoreLocation(String username, String requestFrom) {
        return Mono.fromCallable(() -> {
            configMap = ConfigParamsTon.getInstance().getConfigurationFromconfigParams();
            String fileStoreLocation = configMap.get(Constants.FILE_STORE_PATH);
            
            if (requestFrom.equalsIgnoreCase(Constants.CAMPAIGN)) {
                fileStoreLocation = configMap.get(Constants.CAMPAIGNS_FILE_STORE_PATH);
            } else if (requestFrom.equalsIgnoreCase(Constants.GROUP)) {
                fileStoreLocation = configMap.get(Constants.GROUP_FILE_STORE_PATH);
            }
            
            // creating folder with username and storing files inside it.
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

            FileUploadLog.getInstance().debug("[FilesSaver] [processFilePart] time taken to save " + originalFileName + " is "
                    + Utility.getTimeDifference(startTime) + " milliseconds.");

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
                        
                        FileUploadLog.getInstance().debug("[FilesSaver] [processZipFiles] time taken to extract "
                                + originalFileName + " is " + Utility.getTimeDifference(zipExtractStartTime) + " milliseconds.");
                        
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
            List<String> filesList) {

        return Flux.fromIterable(response)
                .flatMap(fileData -> processSingleFile(fileData, fileStoreLocation))
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
                            successFiles.add(result);
                            totalRecords.addAndGet(Long.parseLong(result.get("count").toString()));
                        }
                    }

                    return new ProcessedResults(successFiles, failedFiles, totalRecords.get());
                });
    }

    private Mono<Map<String, Object>> processSingleFile(Map<String, Object> fileData, String fileStoreLocation) {
        return Mono.fromCallable(() -> {
            try {
                FileReadService fileReadService = new FileReadService(fileData, fileStoreLocation, false);
                return fileReadService.call();
            } catch (Exception e) {
                Map<String, Object> errorResult = new HashMap<>();
                if (e.getMessage().contains(Constants.UNSUPPORTED_FILE_TYPE)) {
                    errorResult.put("error", Constants.UNSUPPORTED_FILE_TYPE);
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
        errorResponse.put("code", statusCode);
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
package com.itextos.beacon.fileprocessor.controller;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

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
import com.winnovature.utils.singletons.ConfigParamsTon;
import com.winnovature.utils.utils.JsonUtility;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@RestController
@RequestMapping("/FP-FileUpload-0.0.1/template")
public class TemplateFilesSaverController {

    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<Map<String, Object>> handleTemplateFileUpload(
            @RequestPart("username") String username,
            @RequestPart Map<String, Part> parts) {

        String requestFrom = Constants.TEMPLATE;
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

        return getFileStoreLocation(username)
                .flatMap(fileStoreLocation -> {
                    // Process file parts
                    return processFileParts(parts, fileStoreLocation, filesList)
                            .flatMap(processedFile -> {
                                if (processedFile.supportedFileType) {
                                    // Process the file with FileReadService
                                    return processFileWithService(processedFile.fileData, fileStoreLocation, filesList, 
                                            requestFrom, username)
                                            .doOnSuccess(result -> sentToTrackingRedis.set(true));
                                } else {
                                    // Return unsupported file type error
                                    return Mono.just(createUnsupportedFileResponse(processedFile.unsupportedFileName));
                                }
                            });
                })
                .onErrorResume(throwable -> {
                    org.apache.commons.logging.LogFactory.getLog(Constants.FileUploadLogger)
                            .error("[TemplateFilesSaver] [handleTemplateFileUpload] Exception", throwable);
                    
                    // Ensure files are tracked for cleanup even on error
                    if (!sentToTrackingRedis.get()) {
                        Utility.sendFilesToTrackingRedis(requestFrom, username, filesList);
                    }
                    
                    return createErrorResponse(Constants.INTERNAL_SERVER_ERROR_STATUS_CODE,
                            Constants.INTERNAL_SERVER_ERROR, Constants.GENERAL_ERROR_MESSAGE,
                            HttpStatus.INTERNAL_SERVER_ERROR);
                })
                .doOnSuccess(result -> {
                    try {
                        String json = new JsonUtility().mapToJson(result);
                        org.apache.commons.logging.LogFactory.getLog(Constants.FileUploadLogger)
                                .debug("[TemplateFilesSaver] [handleTemplateFileUpload] Response: " + json);
                    } catch (Exception e) {
                        // Log without JSON conversion if it fails
                        org.apache.commons.logging.LogFactory.getLog(Constants.FileUploadLogger)
                                .debug("[TemplateFilesSaver] [handleTemplateFileUpload] Response: " + result);
                    }
                });
    }

    private Mono<String> getFileStoreLocation(String username) {
        return Mono.fromCallable(() -> {
            Map<String, String> configMap = ConfigParamsTon.getInstance().getConfigurationFromconfigParams();
            String fileStoreLocation = configMap.get(Constants.TEMPLATE_FILE_STORE_PATH);
            fileStoreLocation = fileStoreLocation + username.toLowerCase() + "/";
            Files.createDirectories(Paths.get(fileStoreLocation));
            return fileStoreLocation;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    private Mono<ProcessedFile> processFileParts(Map<String, Part> parts, String fileStoreLocation, List<String> filesList) {
        return Mono.fromCallable(() -> {
            boolean supportedFileType = true;
            String unsupportedFileName = null;
            Map<String, Object> fileData = null;

            for (Map.Entry<String, Part> entry : parts.entrySet()) {
                Part part = entry.getValue();
                if (!(part instanceof FilePart)) {
                    continue;
                }

                FilePart filePart = (FilePart) part;
                String originalFileName = filePart.filename();
                if (originalFileName == null) {
                    continue;
                }

                // Check for unsupported file types
                if (originalFileName.endsWith(".zip") || originalFileName.endsWith(".txt")) {
                    supportedFileType = false;
                    unsupportedFileName = originalFileName;
                    break;
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

                filesList.add(fileStoreLocation + storedFileName);

                // Store file data for processing
                fileData = new HashMap<>();
                fileData.put("filename", originalFileName);
                fileData.put("r_filename", storedFileName);

                // Only process the first file (as per original logic)
                break;
            }

            return new ProcessedFile(supportedFileType, unsupportedFileName, fileData);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    private Mono<Map<String, Object>> processFileWithService(Map<String, Object> fileData, 
                                                            String fileStoreLocation, 
                                                            List<String> filesList,
                                                            String requestFrom, 
                                                            String username) {
        return Mono.fromCallable(() -> {
            // Send files to tracking Redis
            boolean trackingResult = Utility.sendFilesToTrackingRedis(requestFrom, username, filesList);
            
            if (!trackingResult) {
                throw new RuntimeException("Failed to track files in Redis");
            }

            // Process file with FileReadService
            FileReadService fileReadService = new FileReadService(fileData, fileStoreLocation, true);
            return fileReadService.call();
        })
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorResume(throwable -> {
            // Handle specific FileReadService exceptions
            if (throwable.getMessage().contains(Constants.UNSUPPORTED_FILE_TYPE)) {
                Map<String, Object> errorResult = new HashMap<>();
                errorResult.put("statusCode", Constants.APPLICATION_ERROR_CODE);
                errorResult.put("code", Constants.APPLICATION_ERROR_CODE);
                errorResult.put("filename", fileData.get("filename"));
                errorResult.put("error", Constants.APPLICATION_ERROR);
                errorResult.put("message", Constants.UNSUPPORTED_FILE_TYPE);
                return Mono.just(errorResult);
            } else {
                return Mono.error(throwable);
            }
        });
    }

    private Map<String, Object> createUnsupportedFileResponse(String unsupportedFileName) {
        Map<String, Object> result = new HashMap<>();
        result.put("statusCode", Constants.APPLICATION_ERROR_CODE);
        result.put("code", Constants.APPLICATION_ERROR_CODE);
        result.put("error", Constants.APPLICATION_ERROR);
        result.put("message", Constants.UNSUPPORTED_FILE_TYPE);
        result.put("filename", unsupportedFileName);
        return result;
    }

    private Mono<Map<String, Object>> createErrorResponse(int statusCode, String error, String message, HttpStatus httpStatus) {
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("statusCode", statusCode);
        errorResponse.put("code", statusCode);
        errorResponse.put("error", error);
        errorResponse.put("message", message);
        return Mono.just(errorResponse);
    }

    // Helper class to hold processed file information
    private static class ProcessedFile {
        final boolean supportedFileType;
        final String unsupportedFileName;
        final Map<String, Object> fileData;

        ProcessedFile(boolean supportedFileType, String unsupportedFileName, Map<String, Object> fileData) {
            this.supportedFileType = supportedFileType;
            this.unsupportedFileName = unsupportedFileName;
            this.fileData = fileData;
        }
    }
}
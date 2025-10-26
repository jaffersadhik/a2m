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

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.winnovature.fileuploads.services.FileReadService;
import com.winnovature.fileuploads.utils.Constants;
import com.winnovature.fileuploads.utils.Utility;
import com.winnovature.utils.singletons.ConfigParamsTon;
import com.winnovature.utils.utils.JsonUtility;

@RestController
@RequestMapping("/FP-FileUpload-0.0.1/template")
public class TemplateFilesSaverController {

    private static final Log log = LogFactory.getLog(Constants.FileUploadLogger);

    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> handleTemplateFileUpload(
            @RequestPart("username") String username,
            @RequestPart("files") MultipartFile[] files) {

        String requestFrom = Constants.TEMPLATE;
        List<String> filesList = new ArrayList<>();
        boolean sentToTrackingRedis = false;

        // Validate required parameters
        if (StringUtils.isBlank(username)) {
            return ResponseEntity.badRequest()
                    .body(createErrorResponse(Constants.APPLICATION_ERROR_CODE, Constants.APPLICATION_ERROR, 
                            "username is required"));
        }

        if (files == null || files.length == 0) {
            return ResponseEntity.badRequest()
                    .body(createErrorResponse(Constants.APPLICATION_ERROR_CODE, Constants.APPLICATION_ERROR, 
                            "files are required"));
        }

        try {
            String fileStoreLocation = getFileStoreLocation(username);
            
            // Process file parts
            ProcessedFile processedFile = processFileParts(files, fileStoreLocation, filesList);
            
            Map<String, Object> result;
            if (processedFile.supportedFileType) {
                // Process the file with FileReadService
                result = processFileWithService(processedFile.fileData, fileStoreLocation, filesList, 
                        requestFrom, username);
                sentToTrackingRedis = true;
            } else {
                // Return unsupported file type error
                result = createUnsupportedFileResponse(processedFile.unsupportedFileName);
            }

            // Log response
            try {
                String json = new JsonUtility().mapToJson(result);
                log.debug("[TemplateFilesSaver] [handleTemplateFileUpload] Response: " + json);
            } catch (Exception e) {
                // Log without JSON conversion if it fails
                log.debug("[TemplateFilesSaver] [handleTemplateFileUpload] Response: " + result);
            }

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            log.error("[TemplateFilesSaver] [handleTemplateFileUpload] Exception", e);
            
            // Ensure files are tracked for cleanup even on error
            if (!sentToTrackingRedis) {
                Utility.sendFilesToTrackingRedis(requestFrom, username, filesList);
            }
            
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse(Constants.INTERNAL_SERVER_ERROR_STATUS_CODE,
                            Constants.INTERNAL_SERVER_ERROR, Constants.GENERAL_ERROR_MESSAGE));
        }
    }

    private String getFileStoreLocation(String username) throws Exception {
        Map<String, String> configMap = ConfigParamsTon.getInstance().getConfigurationFromconfigParams();
        String fileStoreLocation = configMap.get(Constants.TEMPLATE_FILE_STORE_PATH);
        fileStoreLocation = fileStoreLocation + username.toLowerCase() + "/";
        Files.createDirectories(Paths.get(fileStoreLocation));
        return fileStoreLocation;
    }

    private ProcessedFile processFileParts(MultipartFile[] files, String fileStoreLocation, List<String> filesList) throws Exception {
        boolean supportedFileType = true;
        String unsupportedFileName = null;
        Map<String, Object> fileData = null;

        for (MultipartFile file : files) {
            if (file.isEmpty()) {
                continue;
            }

            String originalFileName = file.getOriginalFilename();
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
                file.transferTo(csvTempPath.toFile());
                
                // Process CSV file
                com.winnovature.utils.utils.Utility.storeCSVFile(
                    fileStoreLocation + tempFileName, 
                    fileStoreLocation + storedFileName
                );
                filesList.add(fileStoreLocation + tempFileName);
            } else {
                // Transfer regular files directly
                Path filePath = Paths.get(fileStoreLocation + storedFileName);
                file.transferTo(filePath.toFile());
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
    }

    private Map<String, Object> processFileWithService(Map<String, Object> fileData, 
                                                      String fileStoreLocation, 
                                                      List<String> filesList,
                                                      String requestFrom, 
                                                      String username) throws Exception {
        
        // Send files to tracking Redis
        boolean trackingResult = Utility.sendFilesToTrackingRedis(requestFrom, username, filesList);
        
        if (!trackingResult) {
            throw new RuntimeException("Failed to track files in Redis");
        }

        try {
            // Process file with FileReadService
            FileReadService fileReadService = new FileReadService(fileData, fileStoreLocation, true);
            return fileReadService.call();
        } catch (Exception e) {
            // Handle specific FileReadService exceptions
            if (e.getMessage().contains(Constants.UNSUPPORTED_FILE_TYPE)) {
                Map<String, Object> errorResult = new HashMap<>();
                errorResult.put("statusCode", Constants.APPLICATION_ERROR_CODE);
                errorResult.put("code", Constants.APPLICATION_ERROR_CODE);
                errorResult.put("filename", fileData.get("filename"));
                errorResult.put("error", Constants.APPLICATION_ERROR);
                errorResult.put("message", Constants.UNSUPPORTED_FILE_TYPE);
                return errorResult;
            } else {
                throw e;
            }
        }
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

    private Map<String, Object> createErrorResponse(int statusCode, String error, String message) {
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("statusCode", statusCode);
        errorResponse.put("code", statusCode);
        errorResponse.put("error", error);
        errorResponse.put("message", message);
        return errorResponse;
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
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

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
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
import com.winnovature.fileuploads.utils.ZipHandler;
import com.winnovature.logger.FileUploadLog;
import com.winnovature.utils.singletons.ConfigParamsTon;
import com.winnovature.utils.utils.JsonUtility;

@RestController
@RequestMapping("/FP-FileUpload-0.0.1/save")
public class FilesSaverController {

    private Map<String, String> configMap = null;

    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> handleFileUpload(
            @RequestPart("username") String username,
            @RequestPart("frompage") String requestFrom,
            @RequestPart("files") MultipartFile[] files) {

        FileUploadLog.getInstance().debug("[FilesSaver] [handleFileUpload] request received.");

        Instant startTime = Instant.now();
        List<String> filesList = new ArrayList<>();
        boolean sentToTrackingRedis = false;

        // Validate required parameters
        if (StringUtils.isBlank(username)) {
            return createErrorResponse(Constants.APPLICATION_ERROR_CODE, Constants.APPLICATION_ERROR, 
                    "username is required", HttpStatus.BAD_REQUEST);
        }

        if (StringUtils.isBlank(requestFrom)) {
            return createErrorResponse(Constants.APPLICATION_ERROR_CODE, Constants.APPLICATION_ERROR, 
                    "frompage is required", HttpStatus.BAD_REQUEST);
        }

        if (files == null || files.length == 0) {
            return createErrorResponse(Constants.APPLICATION_ERROR_CODE, Constants.APPLICATION_ERROR, 
                    "files are required", HttpStatus.BAD_REQUEST);
        }

        try {
            String fileStoreLocation = getFileStoreLocation(username, requestFrom);
            
            // Process all files
            List<Map<String, Object>> processedFiles = new ArrayList<>();
            for (MultipartFile file : files) {
                if (!file.isEmpty()) {
                    Map<String, Object> fileData = processFilePart(file, fileStoreLocation, filesList, startTime);
                    if (!fileData.isEmpty()) {
                        processedFiles.add(fileData);
                    }
                }
            }

            // Process zip files and extract contents
            processedFiles = processZipFiles(processedFiles, fileStoreLocation, filesList);

            // Add all files to tracking list
            for (Map<String, Object> map : processedFiles) {
                if (map != null && map.get("r_filename") != null) {
                    filesList.add(fileStoreLocation + map.get("r_filename").toString().trim());
                }
            }

            FileUploadLog.getInstance().debug("filesList : " + filesList);

            // Send files to tracking Redis
            sentToTrackingRedis = Utility.sendFilesToTrackingRedis(requestFrom, username, filesList);
            FileUploadLog.getInstance().debug("sentToTrackingRedis : " + sentToTrackingRedis);

            // Process files with FileReadService
            ProcessedResults processedResults = processFilesWithService(processedFiles, fileStoreLocation, filesList);

            // Build final response
            Map<String, Object> finalResponse = buildFinalResponse(processedResults);

            FileUploadLog.getInstance().debug("finalResponse : " + finalResponse);
            
            try {
                String json = new JsonUtility().mapToJson(finalResponse);
                FileUploadLog.getInstance().debug("[FilesSaver] response json : " + json);
                FileUploadLog.getInstance().debug("[FilesSaver] [handleFileUpload] time taken to process request is "
                        + Utility.getTimeDifference(startTime) + " milliseconds.");
            } catch (Exception e) {
                FileUploadLog.getInstance().debug("[FilesSaver] [handleFileUpload] time taken to process request is "
                        + Utility.getTimeDifference(startTime) + " milliseconds.");
            }

            return ResponseEntity.ok(finalResponse);

        } catch (Exception e) {
            FileUploadLog.getInstance().error("[FilesSaver] [handleFileUpload] Exception", e);

            // Ensure files are tracked for cleanup even on error
            if (!sentToTrackingRedis) {
                Utility.sendFilesToTrackingRedis(requestFrom, username, filesList);
            }

            return createErrorResponse(Constants.INTERNAL_SERVER_ERROR_STATUS_CODE, 
                    Constants.INTERNAL_SERVER_ERROR, Constants.GENERAL_ERROR_MESSAGE, 
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private String getFileStoreLocation(String username, String requestFrom) throws Exception {
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
    }

    private Map<String, Object> processFilePart(MultipartFile file, String fileStoreLocation, 
                                               List<String> filesList, Instant startTime) throws Exception {
        String originalFileName = file.getOriginalFilename();
        if (originalFileName == null) {
            return Collections.emptyMap();
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

        FileUploadLog.getInstance().debug("[FilesSaver] [processFilePart] time taken to save " + originalFileName + " is "
                + Utility.getTimeDifference(startTime) + " milliseconds.");

        Map<String, Object> fileData = new HashMap<>();
        fileData.put("filename", originalFileName);
        fileData.put("r_filename", storedFileName);
        fileData.put("extension", extension.toLowerCase());

        return fileData;
    }

    private List<Map<String, Object>> processZipFiles(List<Map<String, Object>> response, String fileStoreLocation, List<String> filesList) {
        List<Map<String, Object>> updatedResponse = new ArrayList<>(response);
        
        for (Map<String, Object> fileData : response) {
            if (".zip".equalsIgnoreCase((String) fileData.get("extension"))) {
                String storedFileName = (String) fileData.get("r_filename");
                String originalFileName = (String) fileData.get("filename");
                
                try {
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
                    updatedResponse.addAll(zipContent);
                    
                } catch (Exception e) {
                    FileUploadLog.getInstance().error("Error processing zip file: " + originalFileName, e);
                }
            }
        }
        
        return updatedResponse;
    }

    private ProcessedResults processFilesWithService(
            List<Map<String, Object>> response,
            String fileStoreLocation,
            List<String> filesList) throws Exception {

        List<Map<String, Object>> successFiles = new ArrayList<>();
        List<Map<String, Object>> failedFiles = new ArrayList<>();
        long totalRecords = 0;

        // Process files using FileReadService
        for (Map<String, Object> fileData : response) {
            try {
                Map<String, Object> result = processSingleFile(fileData, fileStoreLocation);
                
                if (result.containsKey("error")) {
                    failedFiles.add(result);
                } else {
                    successFiles.add(result);
                    totalRecords += Long.parseLong(result.get("count").toString());
                }
            } catch (Exception e) {
                FileUploadLog.getInstance().error("Error processing file: " + fileData.get("filename"), e);
                Map<String, Object> errorResult = new HashMap<>();
                errorResult.put("error", "Processing Error");
                errorResult.put("message", e.getMessage());
                errorResult.put("filename", fileData.get("filename"));
                failedFiles.add(errorResult);
            }
        }

        return new ProcessedResults(successFiles, failedFiles, totalRecords);
    }

    private Map<String, Object> processSingleFile(Map<String, Object> fileData, String fileStoreLocation) throws Exception {
        try {
            FileReadService fileReadService = new FileReadService(fileData, fileStoreLocation, false);
            return fileReadService.call();
        } catch (Exception e) {
            if (e.getMessage().contains(Constants.UNSUPPORTED_FILE_TYPE)) {
                Map<String, Object> errorResult = new HashMap<>();
                errorResult.put("error", Constants.UNSUPPORTED_FILE_TYPE);
                if (StringUtils.split(e.getMessage(), "~").length > 1) {
                    errorResult.put("filename", StringUtils.split(e.getMessage(), "~")[1].trim());
                }
                return errorResult;
            } else {
                throw e;
            }
        }
    }

    private Map<String, Object> buildFinalResponse(ProcessedResults processedResults) {
        Map<String, Object> finalResponse = new HashMap<>();
        Map<String, Object> nestedResponse = new HashMap<>();
        
        nestedResponse.put("success", processedResults.getSuccessFiles());
        nestedResponse.put("failed", processedResults.getFailedFiles());

        finalResponse.put("total", processedResults.getTotalRecords());
        finalResponse.put("total_human", Utility.humanReadableNumberFormat(processedResults.getTotalRecords()));
        finalResponse.put("uploaded_files", nestedResponse);
        finalResponse.put("statusCode", Constants.SUCCESS_STATUS_CODE);

        return finalResponse;
    }

    private ResponseEntity<Map<String, Object>> createErrorResponse(int statusCode, String error, String message, HttpStatus httpStatus) {
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("statusCode", statusCode);
        errorResponse.put("code", statusCode);
        errorResponse.put("error", error);
        errorResponse.put("message", message);
        return new ResponseEntity<>(errorResponse, httpStatus);
    }

    // Helper class to hold processing results
    private static class ProcessedResults {
        private final List<Map<String, Object>> successFiles;
        private final List<Map<String, Object>> failedFiles;
        private final long totalRecords;

        public ProcessedResults(List<Map<String, Object>> successFiles, List<Map<String, Object>> failedFiles, long totalRecords) {
            this.successFiles = successFiles;
            this.failedFiles = failedFiles;
            this.totalRecords = totalRecords;
        }

        public List<Map<String, Object>> getSuccessFiles() { return successFiles; }
        public List<Map<String, Object>> getFailedFiles() { return failedFiles; }
        public long getTotalRecords() { return totalRecords; }
    }
}
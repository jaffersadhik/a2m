package com.itextos.beacon.fileprocessor.controller;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.winnovature.fileuploads.utils.ZipHandler;
import com.winnovature.utils.daos.GenericDao;
import com.winnovature.utils.dtos.Templates;
import com.winnovature.utils.singletons.ConfigParamsTon;
import com.winnovature.utils.utils.JsonUtility;

@RestController
@RequestMapping("/FP-FileUpload-0.0.1/templateplaceholders")
public class CampaignTemplateFilesSaverController {

    private static final Log log = LogFactory.getLog(Constants.FileUploadLogger);
    private Map<String, String> configMap = null;
    private final GenericDao genericDao = new GenericDao();
   
    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> handleFileUpload(
            @RequestPart("username") String username,
            @RequestPart("cli_id") String clientId,
            @RequestPart("temp_id") String templateId,
            @RequestPart("files") MultipartFile[] files) {

        if (log.isDebugEnabled()) {
            log.debug("[CampaignTemplateFilesSaver] [handleFileUpload] request received.");
        }

        Instant startTime = Instant.now();
        String requestFrom = Constants.CAMPAIGN;

        // Validate required parameters
        if (StringUtils.isBlank(username)) {
            return createErrorResponse(Constants.ERROR_CODE_REQUIRED_PARAMS_MISSING,
                    "Bad Request", "username is required", HttpStatus.BAD_REQUEST);
        }

        if (StringUtils.isBlank(clientId)) {
            return createErrorResponse(Constants.ERROR_CODE_REQUIRED_PARAMS_MISSING,
                    "Bad Request", "cli_id is required", HttpStatus.BAD_REQUEST);
        }

        if (StringUtils.isBlank(templateId)) {
            return createErrorResponse(Constants.ERROR_CODE_REQUIRED_PARAMS_MISSING,
                    "Bad Request", "temp_id is required", HttpStatus.BAD_REQUEST);
        }

        try {
            // Get and validate template
            Templates template = getTemplateAndValidate(templateId, clientId);
            
            // Process file upload
            Map<String, Object> response = processFileUpload(username, template, files, requestFrom, startTime);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("[CampaignTemplateFilesSaver] [handleFileUpload] Exception", e);
            return createErrorResponse(Constants.INTERNAL_SERVER_ERROR_STATUS_CODE,
                    "Internal Server Error", "Server Error: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private Templates getTemplateAndValidate(String templateId, String clientId) throws Exception {
        Templates template = genericDao.getTemplateById(templateId);
        if (template == null) {
            throw new RuntimeException("No template found with the given temp_id.");
        }
        if (!clientId.equalsIgnoreCase(template.getClientId())) {
            throw new RuntimeException("Template does not belong to cli_id.");
        }
        return template;
    }

    private Map<String, Object> processFileUpload(String username, Templates template, 
                                                 MultipartFile[] files, String requestFrom, 
                                                 Instant startTime) throws Exception {
        List<String> filesList = new ArrayList<>();
        long totalRecords = 0;

        String fileStoreLocation = getFileStoreLocation(username);
        
        // Extract template information
        boolean isStaticTemplate = false;
        boolean isColumnBasedTemplate = template.getTemplateType().equalsIgnoreCase("column");
        String mobileColumnName = template.getPhoneNumberField().trim().toLowerCase();
        
        String fields = com.winnovature.utils.utils.Utility
                .getPlaceholdersFromTemplateMessage(template.getMsg_text(), template.getUnicode(), isColumnBasedTemplate);
        List<String> requiredPlaceholders = new ArrayList<>();
        if (StringUtils.isNotBlank(fields)) {
            requiredPlaceholders = Arrays.asList(fields.split(","));
            requiredPlaceholders.replaceAll(String::toLowerCase);
        } else {
            isStaticTemplate = true;
        }

        if (log.isDebugEnabled()) {
            log.debug("[CampaignTemplateFilesSaver] [handleFileUpload] Selected template :: " + template);
        }

        // Process all files
        List<Map<String, Object>> processedFiles = new ArrayList<>();
        for (MultipartFile file : files) {
            if (!file.isEmpty()) {
                Map<String, Object> fileData = processFilePart(file, fileStoreLocation, filesList);
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

        // Send files to tracking Redis
        boolean sentToTrackingRedis = Utility.sendFilesToTrackingRedis(requestFrom, username, filesList);

        // Process files with FileReadService
        ProcessedResults processedResults = processFilesWithService(processedFiles, fileStoreLocation, filesList,
                mobileColumnName, isColumnBasedTemplate, requiredPlaceholders,
                isStaticTemplate, sentToTrackingRedis);

        totalRecords = processedResults.getTotalRecords();

        // Build final response
        Map<String, Object> finalResponse = buildFinalResponse(processedResults, totalRecords, isStaticTemplate);

        if (log.isDebugEnabled()) {
            try {
                String json = new JsonUtility().mapToJson(finalResponse);
                log.debug("[CampaignTemplateFilesSaver] [handleFileUpload] time taken to process request is "
                        + Utility.getTimeDifference(startTime) + " milliseconds and response is " + json);
            } catch (Exception e) {
                log.debug("[CampaignTemplateFilesSaver] [handleFileUpload] time taken to process request is "
                        + Utility.getTimeDifference(startTime) + " milliseconds");
            }
        }

        return finalResponse;
    }

    private String getFileStoreLocation(String username) throws Exception {
        configMap = ConfigParamsTon.getInstance().getConfigurationFromconfigParams();
        String fileStoreLocation = configMap.get(Constants.CAMPAIGNS_FILE_STORE_PATH);
        fileStoreLocation = fileStoreLocation + username.toLowerCase() + "/";
        Files.createDirectories(Paths.get(fileStoreLocation));
        return fileStoreLocation;
    }

    private Map<String, Object> processFilePart(MultipartFile file, String fileStoreLocation, List<String> filesList) throws Exception {
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
                    
                    if (log.isDebugEnabled()) {
                        log.debug("[CampaignTemplateFilesSaver] [processZipFiles] time taken to extract "
                                + originalFileName + " is " + Utility.getTimeDifference(zipExtractStartTime)
                                + " milliseconds.");
                    }
                    
                    // Add zip file to cleanup list
                    if (StringUtils.isNotBlank(storedFileName)) {
                        filesList.add(fileStoreLocation + storedFileName);
                    }
                    
                    // Add extracted files to response
                    updatedResponse.addAll(zipContent);
                    
                } catch (Exception e) {
                    log.error("Error processing zip file: " + originalFileName, e);
                }
            }
        }
        
        return updatedResponse;
    }

    private ProcessedResults processFilesWithService(
            List<Map<String, Object>> response,
            String fileStoreLocation,
            List<String> filesList,
            String mobileColumnName,
            boolean isColumnBasedTemplate,
            List<String> requiredPlaceholders,
            boolean isStaticTemplate,
            boolean sentToTrackingRedis) throws Exception {

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
                    long fileCount = Long.parseLong(result.get("count").toString());
                    if (fileCount < 1) {
                        // Remove files with 0 rows
                        Map<String, Object> errorResult = new HashMap<>();
                        errorResult.put("error", "Invalid File");
                        errorResult.put("message", "File is empty");
                        errorResult.put("filename", result.get("filename"));
                        failedFiles.add(errorResult);
                    } else {
                        // Validate template requirements
                        boolean isValid = validateTemplateRequirements(result, mobileColumnName, 
                                isColumnBasedTemplate, requiredPlaceholders, isStaticTemplate);
                        if (!isValid) {
                            failedFiles.add(result);
                        } else {
                            successFiles.add(result);
                            totalRecords += fileCount;
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Error processing file: " + fileData.get("filename"), e);
                Map<String, Object> errorResult = new HashMap<>();
                errorResult.put("error", "Processing Error");
                errorResult.put("message", e.getMessage());
                errorResult.put("filename", fileData.get("filename"));
                failedFiles.add(errorResult);
            }
        }

        // Process placeholders for success files
        processPlaceholdersForSuccessFiles(successFiles, isColumnBasedTemplate, requiredPlaceholders, isStaticTemplate);

        return new ProcessedResults(successFiles, failedFiles, sentToTrackingRedis, totalRecords);
    }

    private boolean validateTemplateRequirements(Map<String, Object> result, String mobileColumnName,
                                                boolean isColumnBasedTemplate, List<String> requiredPlaceholders,
                                                boolean isStaticTemplate) {
        long fileCount = Long.parseLong(result.get("count").toString());
        if (fileCount > 0) {
            if (isColumnBasedTemplate) {
                if (result.get("file_contents_column") != null) {
                    List<List<String>> columns = (List<List<String>>) result.get("file_contents_column");
                    List<String> fileHeaders = columns.get(0);
                    fileHeaders.replaceAll(String::toLowerCase);
                    
                    // Check for mobile column
                    if (!fileHeaders.contains(mobileColumnName)) {
                        result.put("error", "Invalid File");
                        result.put("message", "Missing Mobile Column : " + mobileColumnName);
                        return false;
                    }
                    
                    // Check for required placeholders (unless it's a static template)
                    if (!isStaticTemplate) {
                        boolean atLeastOnePlaceHolderPresentInFile = false;
                        for (String placeholder : requiredPlaceholders) {
                            if (fileHeaders.contains(placeholder)) {
                                atLeastOnePlaceHolderPresentInFile = true;
                                break;
                            }
                        }
                        
                        if (!atLeastOnePlaceHolderPresentInFile) {
                            result.put("error", "Invalid Template File");
                            result.put("message", "Invalid Template File");
                            if (log.isDebugEnabled()) {
                                log.debug("[CampaignTemplateFilesSaver] [validateTemplateRequirements] File[" + 
                                        result.get("filename") + "] does not have any of required columns " + fileHeaders);
                            }
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    private void processPlaceholdersForSuccessFiles(List<Map<String, Object>> successFiles,
                                                  boolean isColumnBasedTemplate,
                                                  List<String> requiredPlaceholders,
                                                  boolean isStaticTemplate) {
        for (Map<String, Object> map : successFiles) {
            long thisFileCount = Long.parseLong(map.get("count").toString());
            if (thisFileCount > 0) {
                map.remove("statusCode");
                map.put("isStatic", isStaticTemplate);
                
                if (isColumnBasedTemplate) {
                    map.remove("file_contents_index");
                    if (map.get("file_contents_column") != null) {
                        processColumnBasedPlaceholders(map, requiredPlaceholders);
                    }
                } else {
                    map.remove("file_contents_column");
                    if (map.get("file_contents_index") != null) {
                        processIndexBasedPlaceholders(map, requiredPlaceholders);
                    }
                }
            }
        }
    }

    private void processColumnBasedPlaceholders(Map<String, Object> map, List<String> requiredPlaceholders) {
        List<List<String>> columns = (List<List<String>>) map.remove("file_contents_column");
        List<String> headers = columns.get(0);
        List<String> data = columns.get(1);
        Map<String, String> headerAndDataPairFromFile = new HashMap<>();
        
        for (int i = 0; i < headers.size(); i++) {
            headerAndDataPairFromFile.put(headers.get(i).toLowerCase(), data.get(i));
        }
        
        Map<String, String> placeholders = new HashMap<>();
        List<String> missing = new ArrayList<>();
        for (String placeholder : requiredPlaceholders) {
            if (headerAndDataPairFromFile.containsKey(placeholder)) {
                placeholders.put(placeholder, headerAndDataPairFromFile.get(placeholder));
            } else {
                missing.add(placeholder);
            }
        }
        map.put("placeholders", placeholders);
        map.put("missing", missing);
    }

    private void processIndexBasedPlaceholders(Map<String, Object> map, List<String> requiredPlaceholders) {
        List<List<String>> columns = (List<List<String>>) map.remove("file_contents_index");
        List<String> headers = columns.get(0);
        List<String> data = columns.get(1);
        Map<String, String> headerAndDataPairFromFile = new HashMap<>();
        
        for (int i = 0; i < headers.size(); i++) {
            headerAndDataPairFromFile.put(headers.get(i), data.get(i));
        }
        
        Map<String, String> placeholders = new HashMap<>();
        for (String placeholder : requiredPlaceholders) {
            if (headerAndDataPairFromFile.containsKey(placeholder)) {
                placeholders.put(placeholder, headerAndDataPairFromFile.get(placeholder));
            } else {
                placeholders.put(placeholder, "");
            }
        }
        map.put("placeholders", placeholders);
        map.put("missing", new ArrayList<String>());
    }

    private Map<String, Object> processSingleFile(Map<String, Object> fileData, String fileStoreLocation) throws Exception {
        try {
            FileReadService fileReadService = new FileReadService(fileData, fileStoreLocation, true);
            return fileReadService.call();
        } catch (Exception e) {
            if (e.getMessage().contains(Constants.UNSUPPORTED_FILE_TYPE)) {
                Map<String, Object> errorResult = new HashMap<>();
                errorResult.put("error", Constants.UNSUPPORTED_FILE_TYPE);
                errorResult.put("message", Constants.UNSUPPORTED_FILE_TYPE);
                if (StringUtils.split(e.getMessage(), "~").length > 1) {
                    errorResult.put("filename", StringUtils.split(e.getMessage(), "~")[1].trim());
                }
                return errorResult;
            } else {
                throw e;
            }
        }
    }

    private Map<String, Object> buildFinalResponse(ProcessedResults processedResults, long total, boolean isStaticTemplate) {
        Map<String, Object> finalResponse = new HashMap<>();
        Map<String, Object> nestedResponse = new HashMap<>();
        
        nestedResponse.put("success", processedResults.getSuccessFiles());
        nestedResponse.put("failed", processedResults.getFailedFiles());

        finalResponse.put("total", total);
        finalResponse.put("total_human", Utility.humanReadableNumberFormat(total));
        finalResponse.put("uploaded_files", nestedResponse);
        finalResponse.put("statusCode", Constants.SUCCESS_STATUS_CODE);
        finalResponse.put("isStatic", isStaticTemplate);

        return finalResponse;
    }

    private ResponseEntity<Map<String, Object>> createErrorResponse(int statusCode, String error, String message, HttpStatus httpStatus) {
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("statusCode", statusCode);
        errorResponse.put("error", error);
        errorResponse.put("message", message);
        return new ResponseEntity<>(errorResponse, httpStatus);
    }

    // Helper class to hold processing results
    private static class ProcessedResults {
        private final List<Map<String, Object>> successFiles;
        private final List<Map<String, Object>> failedFiles;
        private final boolean sentToTrackingRedis;
        private final long totalRecords;

        public ProcessedResults(List<Map<String, Object>> successFiles, List<Map<String, Object>> failedFiles, 
                               boolean sentToTrackingRedis, long totalRecords) {
            this.successFiles = successFiles;
            this.failedFiles = failedFiles;
            this.sentToTrackingRedis = sentToTrackingRedis;
            this.totalRecords = totalRecords;
        }

        public List<Map<String, Object>> getSuccessFiles() { return successFiles; }
        public List<Map<String, Object>> getFailedFiles() { return failedFiles; }
        public boolean isSentToTrackingRedis() { return sentToTrackingRedis; }
        public long getTotalRecords() { return totalRecords; }
    }
}
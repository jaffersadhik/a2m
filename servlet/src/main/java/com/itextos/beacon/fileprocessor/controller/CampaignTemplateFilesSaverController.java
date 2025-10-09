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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import com.winnovature.utils.daos.GenericDao;
import com.winnovature.utils.dtos.Templates;
import com.winnovature.utils.singletons.ConfigParamsTon;
import com.winnovature.utils.utils.JsonUtility;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@RestController
@RequestMapping("/FP-FileUpload-0.0.1/templateplaceholders")
public class CampaignTemplateFilesSaverController {

    private static final Log log = LogFactory.getLog(Constants.FileUploadLogger);
    private Map<String, String> configMap = null;
    private final GenericDao genericDao = new GenericDao();
   
    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<Map<String, Object>> handleFileUpload(
            @RequestPart("username") String username,
            @RequestPart("cli_id") String clientId,
            @RequestPart("temp_id") String templateId,
            @RequestPart Map<String, Part> parts) {

        if (log.isDebugEnabled()) {
            log.debug("[CampaignTemplateFilesSaver] [handleFileUpload] request received.");
        }

        Instant startTime = Instant.now();
        String requestFrom = Constants.CAMPAIGN;

        // Validate required parameters
        if (StringUtils.isBlank(username)) {
            return createErrorResponse(Constants.ERROR_CODE_REQUIRED_PARAMS_MISSING,
                    "Bad Request", "Bad Request", HttpStatus.BAD_REQUEST);
        }

        if (StringUtils.isBlank(clientId)) {
            return createErrorResponse(Constants.ERROR_CODE_REQUIRED_PARAMS_MISSING,
                    "Bad Request", "Bad Request", HttpStatus.BAD_REQUEST);
        }

        if (StringUtils.isBlank(templateId)) {
            return createErrorResponse(Constants.ERROR_CODE_REQUIRED_PARAMS_MISSING,
                    "Bad Request", "Bad Request", HttpStatus.BAD_REQUEST);
        }

        return getTemplateAndValidate(templateId, clientId)
                .flatMap(template -> processFileUpload(username, template, parts, requestFrom, startTime))
                .onErrorResume(throwable -> {
                    log.error("[CampaignTemplateFilesSaver] [handleFileUpload] Exception", throwable);
                    return createErrorResponse(Constants.INTERNAL_SERVER_ERROR_STATUS_CODE,
                            "Internal Server Error", "Server Error", HttpStatus.INTERNAL_SERVER_ERROR);
                })
                .doOnSuccess(response -> {
                    if (log.isDebugEnabled()) {
                        try {
                            String json = new JsonUtility().mapToJson(response);
                            log.debug("[CampaignTemplateFilesSaver] [handleFileUpload] time taken to process request is "
                                    + Utility.getTimeDifference(startTime) + " milliseconds and response is " + json);
                        } catch (Exception e) {
                            log.debug("[CampaignTemplateFilesSaver] [handleFileUpload] time taken to process request is "
                                    + Utility.getTimeDifference(startTime) + " milliseconds");
                        }
                    }
                });
    }

    private Mono<Templates> getTemplateAndValidate(String templateId, String clientId) {
        return Mono.fromCallable(() -> {
            Templates template = genericDao.getTemplateById(templateId);
            if (template == null) {
                throw new RuntimeException("No template found with the given temp_id.");
            }
            if (!clientId.equalsIgnoreCase(template.getClientId())) {
                throw new RuntimeException("Template does not belong to cli_id.");
            }
            return template;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    private Mono<Map<String, Object>> processFileUpload(String username, Templates template, 
                                                       Map<String, Part> parts, String requestFrom, 
                                                       Instant startTime) {
        List<String> filesList = Collections.synchronizedList(new ArrayList<>());
        AtomicLong totalRecords = new AtomicLong(0);

        return getFileStoreLocation(username)
                .flatMap(fileStoreLocation -> {
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

                    final boolean finalIsStaticTemplate = isStaticTemplate;
                    final List<String> finalRequiredPlaceholders = requiredPlaceholders;
                    final boolean finalIsColumnBasedTemplate = isColumnBasedTemplate;

                    if (log.isDebugEnabled()) {
                        log.debug("[CampaignTemplateFilesSaver] [handleFileUpload] Selected template :: " + template);
                    }

                    // Process all file parts
                    return Flux.fromIterable(parts.entrySet())
                            .filter(entry -> entry.getValue() instanceof FilePart)
                            .flatMap(entry -> processFilePart((FilePart) entry.getValue(), fileStoreLocation, filesList))
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
                                boolean sentToTrackingRedis = Utility.sendFilesToTrackingRedis(requestFrom, username, filesList);

                                // Process files with FileReadService
                                return processFilesWithService(response, fileStoreLocation, filesList,
                                        mobileColumnName, finalIsColumnBasedTemplate, finalRequiredPlaceholders,
                                        finalIsStaticTemplate, totalRecords, sentToTrackingRedis);
                            })
                            .flatMap(processedResults -> {
                                // Build final response
                                return buildFinalResponse(processedResults, totalRecords.get(), finalIsStaticTemplate);
                            });
                });
    }

    private Mono<String> getFileStoreLocation(String username) {
        return Mono.fromCallable(() -> {
            configMap = ConfigParamsTon.getInstance().getConfigurationFromconfigParams();
            String fileStoreLocation = configMap.get(Constants.CAMPAIGNS_FILE_STORE_PATH);
            fileStoreLocation = fileStoreLocation + username.toLowerCase() + "/";
            Files.createDirectories(Paths.get(fileStoreLocation));
            return fileStoreLocation;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    private Mono<Map<String, Object>> processFilePart(FilePart filePart, String fileStoreLocation, List<String> filesList) {
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
                            log.debug("[CampaignTemplateFilesSaver] [processZipFiles] time taken to extract "
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
            List<String> filesList,
            String mobileColumnName,
            boolean isColumnBasedTemplate,
            List<String> requiredPlaceholders,
            boolean isStaticTemplate,
            AtomicLong totalRecords,
            boolean sentToTrackingRedis) {

        return Flux.fromIterable(response)
                .flatMap(fileData -> processSingleFile(fileData, fileStoreLocation))
                .collectList()
                .map(fileResults -> {
                    List<Map<String, Object>> successFiles = new ArrayList<>();
                    List<Map<String, Object>> failedFiles = new ArrayList<>();

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
                                failedFiles.add(errorResult);
                            } else {
                                // Validate template requirements
                                boolean isValid = validateTemplateRequirements(result, mobileColumnName, 
                                        isColumnBasedTemplate, requiredPlaceholders, isStaticTemplate);
                                if (!isValid) {
                                    failedFiles.add(result);
                                } else {
                                    successFiles.add(result);
                                    totalRecords.addAndGet(fileCount);
                                }
                            }
                        }
                    }

                    // Process placeholders for success files
                    processPlaceholdersForSuccessFiles(successFiles, isColumnBasedTemplate, requiredPlaceholders, isStaticTemplate);

                    return new ProcessedResults(successFiles, failedFiles, sentToTrackingRedis);
                });
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

    private Mono<Map<String, Object>> processSingleFile(Map<String, Object> fileData, String fileStoreLocation) {
        return Mono.fromCallable(() -> {
            try {
                FileReadService fileReadService = new FileReadService(fileData, fileStoreLocation, true);
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

    private Mono<Map<String, Object>> buildFinalResponse(ProcessedResults processedResults, long total, boolean isStaticTemplate) {
        Map<String, Object> finalResponse = new HashMap<>();
        Map<String, Object> nestedResponse = new HashMap<>();
        
        nestedResponse.put("success", processedResults.successFiles);
        nestedResponse.put("failed", processedResults.failedFiles);

        finalResponse.put("total", total);
        finalResponse.put("total_human", Utility.humanReadableNumberFormat(total));
        finalResponse.put("uploaded_files", nestedResponse);
        finalResponse.put("statusCode", Constants.SUCCESS_STATUS_CODE);
        finalResponse.put("isStatic", isStaticTemplate);

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
        final boolean sentToTrackingRedis;

        ProcessedResults(List<Map<String, Object>> successFiles, List<Map<String, Object>> failedFiles, boolean sentToTrackingRedis) {
            this.successFiles = successFiles;
            this.failedFiles = failedFiles;
            this.sentToTrackingRedis = sentToTrackingRedis;
        }
    }
}
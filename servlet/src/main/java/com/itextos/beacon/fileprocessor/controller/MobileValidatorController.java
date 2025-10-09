package com.itextos.beacon.fileprocessor.controller;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.itextos.beacon.commonlib.utility.mobilevalidation.MobileNumberValidator;
import com.itextos.beacon.inmemdata.account.UserInfo;
import com.winnovature.fileuploads.utils.Constants;
import com.winnovature.fileuploads.utils.Utility;
import com.winnovature.utils.utils.JsonUtility;
import com.winnovature.utils.utils.UserDetails;

import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/FP-FileUpload-0.0.1/validatemobile")
public class MobileValidatorController {

    @PostMapping(consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    public Mono<Map<String, Object>> validateMobileNumbers(
            @RequestParam("cli_id") String clientId,
            @RequestParam("mobile") String mobile) {

        return Mono.fromCallable(() -> {
            Instant startTime = Instant.now();
            
            if (StringUtils.isBlank(clientId)) {
                return createErrorResponse(Constants.INTERNAL_SERVER_ERROR_STATUS_CODE,
                        Constants.INTERNAL_SERVER_ERROR, "Client ID is required", HttpStatus.BAD_REQUEST);
            }

            if (StringUtils.isBlank(mobile)) {
                return createErrorResponse(Constants.INTERNAL_SERVER_ERROR_STATUS_CODE,
                        Constants.INTERNAL_SERVER_ERROR, "Mobile numbers are required", HttpStatus.BAD_REQUEST);
            }

            // Parse mobile numbers from input
            List<String> mobiList = parseMobileNumbers(mobile);
            
            if (mobiList.isEmpty()) {
                return createErrorResponse(Constants.INTERNAL_SERVER_ERROR_STATUS_CODE,
                        Constants.INTERNAL_SERVER_ERROR, "No valid mobile numbers found", HttpStatus.BAD_REQUEST);
            }

            // Get user info and validation parameters
            UserInfo userInfo = UserDetails.getUserInfo(clientId);
            if (userInfo == null) {
                return createErrorResponse(Constants.INTERNAL_SERVER_ERROR_STATUS_CODE,
                        Constants.INTERNAL_SERVER_ERROR, "User not found for client ID: " + clientId, 
                        HttpStatus.BAD_REQUEST);
            }

            Map<String, Object> info = com.winnovature.utils.utils.MobileValidator.getRequiredInfo(userInfo);
            boolean isIntlServiceEnabled = (boolean) info.get("isIntlServiceEnabled");
            boolean considerDefaultLengthAsDomestic = (boolean) info.get("considerDefaultLengthAsDomestic");
            boolean isDomesticSpecialSeriesAllow = (boolean) info.get("isDomesticSpecialSeriesAllow");
            String countryCode = (String) info.get("countryCode");

            // Validate mobile numbers
            ValidationResult result = validateMobileNumbers(mobiList, countryCode, isIntlServiceEnabled,
                    considerDefaultLengthAsDomestic, isDomesticSpecialSeriesAllow);

            // Build response
            Map<String, Object> finalResponse = new HashMap<>();
            finalResponse.put("total", mobiList.size());
            finalResponse.put("valid", result.validList);
            finalResponse.put("valid_cnt", result.validList.size());
            finalResponse.put("invalid", result.invalidList);
            finalResponse.put("invalid_cnt", result.invalidList.size());
            finalResponse.put("duplicate", result.duplicateList);
            finalResponse.put("duplicate_cnt", result.duplicateList.size());
            finalResponse.put("statusCode", Constants.SUCCESS_STATUS_CODE);

            // Log response
            if (org.apache.commons.logging.LogFactory.getLog(Constants.FileUploadLogger).isDebugEnabled()) {
                String json = new JsonUtility().mapToJson(finalResponse);
                org.apache.commons.logging.LogFactory.getLog(Constants.FileUploadLogger)
                        .debug("[MobileValidator] [validateMobileNumbers] time taken to process request " + 
                               mobile + " is " + Utility.getTimeDifference(startTime) + 
                               " milliseconds. Response = " + json);
            }

            return finalResponse;
        })
        .onErrorResume(throwable -> {
            org.apache.commons.logging.LogFactory.getLog(Constants.FileUploadLogger)
                    .error("[MobileValidator] [validateMobileNumbers] Exception", throwable);
            
            return Mono.just(createErrorResponse(Constants.INTERNAL_SERVER_ERROR_STATUS_CODE,
                    Constants.INTERNAL_SERVER_ERROR, Constants.GENERAL_ERROR_MESSAGE, 
                    HttpStatus.INTERNAL_SERVER_ERROR));
        });
    }

    private List<String> parseMobileNumbers(String mobileInput) {
        if (StringUtils.isBlank(mobileInput)) {
            return Collections.emptyList();
        }

        return Stream.of(mobileInput.split(","))
                .flatMap(part -> Stream.of(part.split("\n")))
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
    }

    private ValidationResult validateMobileNumbers(List<String> mobileNumbers, String countryCode,
                                                  boolean isIntlServiceEnabled, 
                                                  boolean considerDefaultLengthAsDomestic,
                                                  boolean isDomesticSpecialSeriesAllow) {
        
        List<String> validList = new ArrayList<>();
        List<String> invalidList = new ArrayList<>();
        List<String> duplicateList = new ArrayList<>();
        Set<String> seenValidNumbers = new HashSet<>();
        Set<String> seenInvalidNumbers = new HashSet<>();

        for (String mobileNumber : mobileNumbers) {
            MobileNumberValidator validator = com.winnovature.utils.utils.MobileValidator.validate(
                    mobileNumber, countryCode, isIntlServiceEnabled, 
                    considerDefaultLengthAsDomestic, isDomesticSpecialSeriesAllow);
            
            if (validator.isValidMobileNumber()) {
                String validatedNumber = validator.getMobileNumber();
                if (seenValidNumbers.contains(validatedNumber)) {
                    duplicateList.add(mobileNumber);
                } else {
                    validList.add(validatedNumber);
                    seenValidNumbers.add(validatedNumber);
                }
            } else {
                if (seenInvalidNumbers.contains(mobileNumber)) {
                    duplicateList.add(mobileNumber);
                } else {
                    invalidList.add(mobileNumber);
                    seenInvalidNumbers.add(mobileNumber);
                }
            }
        }

        return new ValidationResult(validList, invalidList, duplicateList);
    }

    private Map<String, Object> createErrorResponse(int statusCode, String error, String message, HttpStatus httpStatus) {
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("statusCode", statusCode);
        errorResponse.put("code", statusCode);
        errorResponse.put("error", error);
        errorResponse.put("message", message);
        return errorResponse;
    }

    // Helper class to hold validation results
    private static class ValidationResult {
        final List<String> validList;
        final List<String> invalidList;
        final List<String> duplicateList;

        ValidationResult(List<String> validList, List<String> invalidList, List<String> duplicateList) {
            this.validList = validList;
            this.invalidList = invalidList;
            this.duplicateList = duplicateList;
        }
    }
}
package com.itextos.beacon.logprocessor.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.itextos.beacon.queryprocessor.commonutils.CommonVariables;
import com.itextos.beacon.queryprocessor.commonutils.Utility;
import com.itextos.beacon.queryprocessor.databaseconnector.ConnectionPoolSingleton;
import com.itextos.beacon.queryprocessor.databaseconnector.SQLStatementExecutor;
import com.itextos.beacon.queryprocessor.requestreceiver.QueryEngine;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Get data from MARIADB or POSTGRES
 */
@RestController
@RequestMapping("/get_data")
public class GetDataController {

    private static final Log log = LogFactory.getLog(GetDataController.class);
    private static ConnectionPoolSingleton connPool = null;
    public static String API_NAME = CommonVariables.GET_DATA_API;

    private static String validateColumns(List<String> lstParamColumns) {
        if (connPool == null) {
            return "Connection pool not initialized";
        }

        final Set<String> setIVcols = lstParamColumns.stream()
                .filter(val -> (!val.startsWith("s.") && !val.startsWith("d."))).collect(Collectors.toSet());

        final Set<String> ivSubCols = lstParamColumns.stream().filter(val -> val.startsWith("s."))
                .collect(Collectors.toSet());
        ivSubCols.removeAll(connPool.setSubCols);

        final Set<String> ivDelCols = lstParamColumns.stream().filter(val -> val.startsWith("d."))
                .collect(Collectors.toSet());
        ivDelCols.removeAll(connPool.setDelCols);

        setIVcols.addAll(ivSubCols);
        setIVcols.addAll(ivDelCols);

        if (setIVcols.size() > 0)
            return String.join(", ", setIVcols);

        return null;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<JSONObject> getData(@RequestBody String requestBody) {
        try {
            // Initialize connection pool if needed
            if (connPool == null) {
                connPool = ConnectionPoolSingleton.getInstance();
            }

            // Parse and validate request
            RequestContext context = parseAndValidateRequest(requestBody);
            if (context.hasError()) {
                return ResponseEntity.status(context.statusCode).body(createErrorResponse(context.errorMessage));
            }

            // Validate dates
            context = validateDates(context);
            if (context.hasError()) {
                return ResponseEntity.status(context.statusCode).body(createErrorResponse(context.errorMessage));
            }

            // Validate columns
            context = validateColumns(context);
            if (context.hasError()) {
                return ResponseEntity.status(context.statusCode).body(createErrorResponse(context.errorMessage));
            }

            // Process data retrieval
            JSONObject result = processDataRetrieval(context);
            return ResponseEntity.ok(result);

        } catch (Exception e) {
            log.error("Unexpected error occurred", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Unable to retrieve the data"));
        }
    }

    private RequestContext parseAndValidateRequest(String requestBody) {
        RequestContext context = new RequestContext();
        
        try {
            final JSONParser parser = new JSONParser();
            JSONObject reqJson = (JSONObject) parser.parse(requestBody);
            JSONObject paramJson = (JSONObject) parser.parse(reqJson.get(CommonVariables.R_PARAM).toString());

            log.info("Parameter list");
            log.info(paramJson.toJSONString());

            context.reqJson = reqJson;
            context.paramJson = paramJson;
            
        } catch (Exception e) {
            log.error("JSON Parsing Error", e);
            context.errorMessage = "Invalid JSON";
            context.statusCode = HttpStatus.BAD_REQUEST;
        }
        
        return context;
    }

    private RequestContext validateDates(RequestContext context) {
        if (context.hasError()) {
            return context;
        }

        try {
            final String DATE_FORMAT_S = "yyyy-M-dd HH:mm:ss";
            final DateTimeFormatter formatWithS = DateTimeFormatter.ofPattern(DATE_FORMAT_S);
            final LocalDateTime paramStartTime = LocalDateTime
                    .parse(context.paramJson.get(CommonVariables.START_DATE).toString(), formatWithS);
            final LocalDateTime paramEndTime = LocalDateTime
                    .parse(context.paramJson.get(CommonVariables.END_DATE).toString(), formatWithS);

            final LocalDate ldt_cur_dt = LocalDate.now();

            log.info(String.format("Param Start Time: %s", Utility.formatDateTime(paramStartTime)));
            log.info(String.format("Param End Time: %s", Utility.formatDateTime(paramEndTime)));

            if (paramStartTime.isAfter(paramEndTime)) {
                log.error("Start Time greater than End Time");
                context.errorMessage = "Start Time greater than End Time";
                context.statusCode = HttpStatus.BAD_REQUEST;
                return context;
            }

            final LocalDate paramStartDate = paramStartTime.toLocalDate();
            final int max_past_days = Utility.getInteger(QueryEngine.mySQL_cfg_val.getProperty("maxPastDays"));
            final LocalDate ldt_max_old_dt = paramStartDate.plusDays(-max_past_days);

            if (ldt_max_old_dt.isAfter(paramStartDate)) {
                log.error("Start Time cannot be Older than " + max_past_days + " Days");
                context.errorMessage = "Start Time cannot be Older than " + max_past_days + " Days";
                context.statusCode = HttpStatus.BAD_REQUEST;
                return context;
            }

            context.paramStartTime = paramStartTime;
            context.paramEndTime = paramEndTime;
            return context;
        } catch (Exception e) {
            log.error("Error validating dates", e);
            context.errorMessage = "Error processing date parameters";
            context.statusCode = HttpStatus.BAD_REQUEST;
            return context;
        }
    }

    private RequestContext validateColumns(RequestContext context) {
        if (context.hasError()) {
            return context;
        }

        try {
            final String paramColumns = Utility.nullCheck(context.paramJson.get("columns"), true);

            if ("".equals(paramColumns)) {
                log.error("Columns parameter is empty");
                context.errorMessage = "Columns parameter is empty";
                context.statusCode = HttpStatus.BAD_REQUEST;
                return context;
            }

            final List<String> lstParamColumns = Arrays.asList(paramColumns.trim().split("\\s*,\\s*"));
            final String invalidColumns = GetDataController.validateColumns(lstParamColumns);

            if (invalidColumns != null) {
                log.error("Invalid Columns: " + invalidColumns);
                context.errorMessage = "Invalid Columns: " + invalidColumns;
                context.statusCode = HttpStatus.BAD_REQUEST;
                return context;
            }

            final List<String> lstJSONCols = new ArrayList<>();
            for (final String pCol : lstParamColumns) {
                final String colName = pCol.substring(2);
                if (pCol.startsWith("s."))
                    lstJSONCols.add(connPool.hmMapSubPG_MYSQL.get(colName));
                else if (pCol.startsWith("d."))
                    lstJSONCols.add(connPool.hmMapDelPG_MYSQL.get(colName));
            }
            log.info("JSON Columns List: " + String.join(",", lstJSONCols));

            context.lstJSONCols = lstJSONCols;
            context.lstParamColumns = lstParamColumns;
            return context;
        } catch (Exception e) {
            log.error("Error validating columns", e);
            context.errorMessage = "Error validating columns";
            context.statusCode = HttpStatus.BAD_REQUEST;
            return context;
        }
    }

    private JSONObject processDataRetrieval(RequestContext context) {
        if (context.hasError()) {
            return createErrorResponse(context.errorMessage);
        }

        try {
            int maxlimit = Utility.getInteger(QueryEngine.mySQL_cfg_val.getProperty("recordLimit"));
            @SuppressWarnings("unchecked")
            final List<Long> client_ids = (List<Long>) context.paramJson.get(CommonVariables.R_CLI_ID);
            final List<String> lstCli = client_ids.stream().map(String::valueOf).collect(Collectors.toList());

            int limit = Utility.getInteger(String.valueOf(context.paramJson.get("limit")));
            if (limit == 0)
                limit = maxlimit;
            maxlimit = Math.min(limit, maxlimit);

            final String timeZoneName = Utility.nullCheck(context.paramJson.get("zone_name"), true);

            final LocalDate startDate = context.paramStartTime.toLocalDate();
            final LocalDate endDate = context.paramEndTime.toLocalDate();
            List<LocalDateTime> listOfDates;

            final long totalDays = ChronoUnit.DAYS.between(startDate, endDate);

            if (totalDays == 0)
                listOfDates = Collections.singletonList(startDate.atStartOfDay());
            else
                listOfDates = startDate.datesUntil(endDate.plusDays(1)).map(LocalDate::atStartOfDay)
                        .collect(Collectors.toList());

            log.info(String.format("Total requested days : %d", (totalDays + 1)));

            context.maxlimit = maxlimit;
            context.lstCli = lstCli;
            context.timeZoneName = timeZoneName;
            context.listOfDates = listOfDates;
            context.startDate = startDate;
            context.endDate = endDate;

            // Retrieve data for all dates
            return retrieveDataForAllDates(context);
            
        } catch (Exception e) {
            log.error("Error during data retrieval", e);
            return createErrorResponse("Unable to retrieve the data");
        }
    }

    private JSONObject retrieveDataForAllDates(RequestContext context) {
        JSONArray finalDataList = new JSONArray();
        int recordCount = 0;

        for (LocalDateTime date : context.listOfDates) {
            // Stop if we've reached the limit
            if (recordCount >= context.maxlimit) {
                break;
            }

            DataResult dataResult = retrieveDataForDate(context, date);
            if (dataResult.hasError) {
                log.error("Error retrieving data for date: " + Utility.formatDateTime(date.toLocalDate()));
                return createErrorResponse("Unable to retrieve the data");
            }
            
            if (dataResult.dataList != null) {
                finalDataList.addAll(dataResult.dataList);
                recordCount = finalDataList.size();
                
                if (dataResult.dataList.size() > 0) {
                    log.info(String.format("Retrieved %d records for date: %s", 
                        dataResult.dataList.size(), 
                        Utility.formatDateTime(date.toLocalDate())));
                } else {
                    log.info(String.format("No Data available for the Date: %s", 
                        Utility.formatDateTime(date.toLocalDate())));
                }
            }
        }

        log.info(String.format("Get data completed, Record Count: %d", recordCount));

        JSONObject resJson = new JSONObject();
        resJson.put(CommonVariables.SERVER_TIMESTAMP, System.currentTimeMillis());
        resJson.put("record-count", recordCount);
        resJson.put("records", finalDataList);
        return resJson;
    }

    private DataResult retrieveDataForDate(RequestContext context, LocalDateTime date) {
        try {
            LocalDateTime qStartDate;
            LocalDateTime qEndDate;

            final String dateStr = Utility.formatDateTime(date.toLocalDate());

            if (context.startDate.isEqual(date.toLocalDate()))
                qStartDate = context.paramStartTime;
            else
                qStartDate = date;

            if (context.endDate.isEqual(date.toLocalDate()))
                qEndDate = context.paramEndTime.plusSeconds(1);
            else
                qEndDate = date.plusDays(1);

            log.info(String.format("Query Param Start Time : %s", Utility.formatDateTime(qStartDate)));
            log.info(String.format("Query Param End Time : %s", Utility.formatDateTime(qEndDate)));

            JSONArray dataList;
            if (date.toLocalDate().isAfter(LocalDate.now().plusDays(-2))) {
                log.info("Fetching Data from MariaDB");
                dataList = SQLStatementExecutor.getMariaDBDataForAPI(
                    connPool.defaultMDBCliJNDI_ID, context.lstCli, qStartDate,
                    qEndDate, context.paramJson, context.maxlimit, 
                    context.lstJSONCols, context.timeZoneName);
            } else {
                log.info("Fetching Data from Postgresql");
                dataList = SQLStatementExecutor.getPGDataForAPI(
                    connPool.defaultPGCliJNDI_ID, context.lstCli, qStartDate, qEndDate,
                    context.paramJson, context.maxlimit, context.lstJSONCols, context.timeZoneName);
            }

            if (dataList == null) {
                log.error(String.format("Unable to get Data for the Date: %s", dateStr));
                return new DataResult(null, true);
            }

            return new DataResult(dataList, false);
        } catch (Exception e) {
            log.error("Error retrieving data for date: " + Utility.formatDateTime(date.toLocalDate()), e);
            return new DataResult(null, true);
        }
    }

    private JSONObject createErrorResponse(String message) {
        JSONObject errorResponse = new JSONObject();
        errorResponse.put(CommonVariables.STATUS_MESSAGE, message);
        return errorResponse;
    }

    // Helper classes for context management
    private static class RequestContext {
        JSONObject reqJson;
        JSONObject paramJson;
        LocalDateTime paramStartTime;
        LocalDateTime paramEndTime;
        List<String> lstJSONCols;
        List<String> lstParamColumns;
        List<String> lstCli;
        String timeZoneName;
        int maxlimit;
        List<LocalDateTime> listOfDates;
        LocalDate startDate;
        LocalDate endDate;
        String errorMessage;
        HttpStatus statusCode;

        boolean hasError() {
            return errorMessage != null;
        }
    }

    private static class DataResult {
        final JSONArray dataList;
        final boolean hasError;

        DataResult(JSONArray dataList, boolean hasError) {
            this.dataList = dataList;
            this.hasError = hasError;
        }
    }
}
package com.itextos.beacon.interfaces.generichttpapi.processor.reader;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import com.itextos.beacon.commonlib.constants.ConfigParamConstants;
import com.itextos.beacon.commonlib.constants.InterfaceStatusCode;
import com.itextos.beacon.commonlib.constants.InterfaceType;
import com.itextos.beacon.commonlib.constants.MiddlewareConstant;
import com.itextos.beacon.commonlib.prometheusmetricsutil.PrometheusMetrics;
import com.itextos.beacon.commonlib.utility.CommonUtility;
import com.itextos.beacon.http.generichttpapi.common.data.InterfaceMessage;
import com.itextos.beacon.http.generichttpapi.common.data.InterfaceRequestStatus;
import com.itextos.beacon.http.generichttpapi.common.interfaces.IRequestProcessor;
import com.itextos.beacon.http.generichttpapi.common.utils.APIConstants;
import com.itextos.beacon.http.generichttpapi.common.utils.InterfaceInputParameters;
import com.itextos.beacon.http.generichttpapi.common.utils.Utility;
import com.itextos.beacon.http.interfaceparameters.InterfaceParameter;
import com.itextos.beacon.http.interfaceparameters.InterfaceParameterLoader;
import com.itextos.beacon.http.interfaceutil.MessageSource;
import com.itextos.beacon.interfaces.generichttpapi.processor.request.JSONRequestProcessor;

import io.prometheus.client.Histogram.Timer;

public class QSRequestReader
        extends
        AbstractReader
{

    private static final Log log = LogFactory.getLog(QSRequestReader.class);
    private  String     mRequestType;

    StringBuffer sb;
    Map<String, String> params;
    public QSRequestReader(Map<String, String> params, String method, String aRequestType,
			StringBuffer sb)
    {
        super(params,aRequestType);
        this.params=params;
        mRequestType = aRequestType;
        this.sb=sb;

        sb.append(" aRequestType : "+aRequestType ).append("\n");
    }

    @Override
    public String processGetRequest()
    {

        try
        {
            final JSONObject jsonObject = new JSONObject();

            if (mRequestType == null)
            {
                buildJson(jsonObject, null, mRequestType);
                sb.append("buildJson Json").append("\n");
                return doProcess(jsonObject);
            }
            else
            {
                if (log.isDebugEnabled())
                    log.debug("Custimize QueryString Request processing..");

                buildBadsicInfo(jsonObject);
                sb.append("buildBadsicInfo Json").append("\n");
                return doProcess(jsonObject);
            }
        }
        catch (final Exception e)
        {
            log.error("Exception while parsing the JSON", e);
        }
        
        return null;
    }

    @Override
    public String doProcess(
            JSONObject aJsonObj)
    {
       String lUserName      = NO_USER;
        Timer  overAllProcess = null;
        Timer  jsonProcess    = null;

        try
        {
            overAllProcess = PrometheusMetrics.apiStartTimer(InterfaceType.HTTP_JAPI, MessageSource.GENERIC_QS, APIConstants.CLUSTER_INSTANCE, params.get(MiddlewareConstant.MW_CLIENT_SOURCE_IP.getKey()), OVERALL);

            String jsonString = aJsonObj.toString();

            if (mRequestType != null)
                jsonString = buildBadsicInfo(aJsonObj);

            final IRequestProcessor requestProcessor = new JSONRequestProcessor(jsonString, params.get(MiddlewareConstant.MW_CLIENT_SOURCE_IP.getKey()), System.currentTimeMillis(), MessageSource.GENERIC_QS,
                    MessageSource.GENERIC_QS,sb);

            requestProcessor.parseBasicInfo(params.get(InterfaceInputParameters.AUTHORIZATION));

            final InterfaceRequestStatus reqStatus = requestProcessor.validateBasicInfo();

            sb.append("reqStatus : "+reqStatus).append("\n");
            
            if (reqStatus.getStatusCode() == InterfaceStatusCode.SUCCESS)
            {
                lUserName   = getUserName(requestProcessor);

                jsonProcess = PrometheusMetrics.apiStartTimer(InterfaceType.HTTP_JAPI, MessageSource.GENERIC_QS, APIConstants.CLUSTER_INSTANCE, params.get(MiddlewareConstant.MW_CLIENT_SOURCE_IP.getKey()), lUserName);
                PrometheusMetrics.apiIncrementAcceptCount(InterfaceType.HTTP_JAPI, MessageSource.GENERIC_QS, APIConstants.CLUSTER_INSTANCE, params.get(MiddlewareConstant.MW_CLIENT_SOURCE_IP.getKey()), lUserName);

                updateRequestObjectBasedOnRequestType(requestProcessor, aJsonObj);

                final int messagesCount = requestProcessor.getMessagesCount();

                if (log.isDebugEnabled())
                    log.debug(" messageCount:  '" + messagesCount + "'");
                
                sb.append(" messageCount:  '" + messagesCount + "'").append("\n");
                
                if (messagesCount == 0)
                {
                    if (log.isDebugEnabled())
                        log.debug(" messageCount==0:  '" + InterfaceStatusCode.INVALID_JSON + "' Message Object Missing");

                    handleNoMessage(requestProcessor, reqStatus);
                }
                else
                {
                    if (log.isDebugEnabled())
                        log.debug("Processing valid messages");

                    processValidMessages(requestProcessor, reqStatus);
                }
            }
            return sendResponse(requestProcessor);
        }
        catch (final Exception e)
        {
            log.error("Excception while processig QueryString request", e);
           return  handleException(aJsonObj.toJSONString());
        }
        finally
        {
            PrometheusMetrics.apiEndTimer(InterfaceType.HTTP_JAPI, APIConstants.CLUSTER_INSTANCE, jsonProcess);
            PrometheusMetrics.apiEndTimer(InterfaceType.HTTP_JAPI, APIConstants.CLUSTER_INSTANCE, overAllProcess);
        }
        
    }

    private String handleException(
            String aJsonString)
    {
        final IRequestProcessor      requestProcessor = new JSONRequestProcessor(aJsonString, params.get(MiddlewareConstant.MW_CLIENT_SOURCE_IP.getKey()), System.currentTimeMillis(), MessageSource.GENERIC_QS,
                MessageSource.GENERIC_QS,sb);
        final InterfaceRequestStatus status           = new InterfaceRequestStatus(InterfaceStatusCode.INVALID_REQUEST, "");
        requestProcessor.setRequestStatus(status);
        return  sendResponse(requestProcessor);
    }

    private void updateRequestObjectBasedOnRequestType(
            IRequestProcessor aRequestProcessor,
            JSONObject aJsonObj)
            throws ParseException
    {

        if (mRequestType != null)
        {
            buildJson(aJsonObj, aRequestProcessor.getBasicInfo().getClientId(), mRequestType);

            final String aJSonString = aJsonObj.toString();

            if (log.isDebugEnabled())
                log.debug("Custom Json String :" + aJSonString);

            aRequestProcessor.setRequestString(aJSonString);
            aRequestProcessor.resetRequestJson(aJsonObj);
        }
    }

    private void processValidMessages(
            IRequestProcessor aRequestProcessor,
            InterfaceRequestStatus aReqStatus)
    {
        final int messagesCount = aRequestProcessor.getMessagesCount();

        if (messagesCount == 1)
            processSingleMessage(aRequestProcessor, aReqStatus,sb);
        else
            processMultipleMessage(aRequestProcessor, aReqStatus);
    }

    private static void handleNoMessage(
            IRequestProcessor aRequestProcessor,
            InterfaceRequestStatus aReqStatus)
    {
        aReqStatus = new InterfaceRequestStatus(InterfaceStatusCode.INVALID_JSON, "Message Object Missing");
        aRequestProcessor.setRequestStatus(aReqStatus);
    }

    private static void processMultipleMessage(
            IRequestProcessor aRequestProcessor,
            InterfaceRequestStatus aReqStatus)
    {
        final String messageId = aReqStatus.getMessageId();
        if (log.isDebugEnabled())
            log.debug(" MultipleMessage:  '" + InterfaceStatusCode.SUCCESS + "'");

        aReqStatus = aRequestProcessor.getMultipleMessages(false);

        if (aReqStatus == null)
        {
            aReqStatus = new InterfaceRequestStatus(InterfaceStatusCode.SUCCESS, "");
            aReqStatus.setMessageId(messageId);
        }
        aRequestProcessor.setRequestStatus(aReqStatus);
    }

    private static void processSingleMessage(
            IRequestProcessor aRequestProcessor,
            InterfaceRequestStatus aReqStatus,StringBuffer sb)
    {
        final InterfaceMessage message   = aRequestProcessor.getSingleMessage(sb);
        final String           messageId = aReqStatus.getMessageId();

        if (message == null)
        {
            aReqStatus = new InterfaceRequestStatus(InterfaceStatusCode.SUCCESS, "");
            aReqStatus.setMessageId(messageId);
            aRequestProcessor.setRequestStatus(aReqStatus);
        }
        else
        {
            if (log.isDebugEnabled())
                log.debug(" singleMessage:  '" + message.getRequestStatus() + "'");

            aReqStatus = message.getRequestStatus();

            aReqStatus.setMessageId(messageId);
            aRequestProcessor.setRequestStatus(aReqStatus);
            /*
             * if (aReqStatus.getStatusCode() == InterfaceStatusCode.SUCCESS)
             * {
             * aReqStatus.setMessageId(messageId);
             * aRequestProcessor.setRequestStatus(aReqStatus);
             * }
             * else
             * aRequestProcessor.setRequestStatus(aReqStatus);
             */
        }
    }

    public String buildBadsicInfo(
            JSONObject jsonObject)
    {
        if (log.isDebugEnabled())
            log.debug("buildBadsicInfo() - QS Request :" + params);

        final String version       = params.get(InterfaceInputParameters.REQ_PARAMETER_VERSION);

        final String accessKey     = getAccessKeyFromCustomer();

        final String encrypt       = params.get(InterfaceInputParameters.REQ_PARAMETER_ENCRYPTED);
        final String lScheduleTime = params.get(InterfaceInputParameters.REQ_PARAMETER_SCHEDULE_AT);

        jsonObject.put(InterfaceInputParameters.REQ_PARAMETER_VERSION, version);
        if (accessKey != null)
            jsonObject.put(InterfaceInputParameters.REQ_PARAMETER_KEY, accessKey);

        jsonObject.put(InterfaceInputParameters.REQ_PARAMETER_ENCRYPTED, encrypt);
        jsonObject.put(InterfaceInputParameters.REQ_PARAMETER_SCHEDULE_AT, lScheduleTime);

        return jsonObject.toString();
    }

    private String getAccessKeyFromCustomer()
    {
        String accessKey = params.get(InterfaceInputParameters.REQ_PARAMETER_KEY);

        if (accessKey == null)
        {
            final String possbileAccessKeys = CommonUtility.nullCheck(Utility.getConfigParamsValueAsString(ConfigParamConstants.ACCESS_KEY_PARAMS), true);

            if (log.isDebugEnabled())
                log.debug("possbileAccessKeys  " + possbileAccessKeys);

            if (!possbileAccessKeys.isBlank())
            {
                final String[] lInputAccessKeyList = possbileAccessKeys.split(",");

                for (final String lKey : lInputAccessKeyList)
                {
                    accessKey = params.get(CommonUtility.nullCheck(lKey, true));

                    if (log.isDebugEnabled())
                        log.debug("Customized AccessKey Param '" + lKey + "' Value : '" + accessKey + "'");

                    if (accessKey != null)
                        return accessKey;
                }
            }
        }

        return accessKey;
    }

    public void buildJson(
            JSONObject jsonObject,
            String aClientId,
            String aRequestType)
            throws ParseException
    {
        if (log.isDebugEnabled())
            log.debug("QS Query String :" + params);
        String lScheduleTime   = null;
        String lDest           = null;
        String lMessage        = null;
        String lHeader         = null;
        String lMsgType        = null;
        String lUdhi           = null;
        String lUdh            = null;
        String lAppendCountry  = null;
        String lUrlTrack       = null;
        String lDcs            = null;
        String lSpecialPort    = null;
        String lMsgExpiry      = null;
        String lCountryCode    = null;
        String lCustRef        = null;
        String lTemplateId     = null;
        String lTemplateValues = null;
        String lDltEntityId    = null;
        String lDltTemplateId  = null;
        String lDltTMAId  = null;

        String lMsgtag         = null;
        String lParam1         = null;
        String lParam2         = null;
        String lParam3         = null;
        String lParam4         = null;
        String lParam5         = null;
        String lParam6         = null;
        String lParam7         = null;
        String lParam8         = null;
        String lParam9         = null;
        String lParam10        = null;
        String lDlrReq         = null;
        String lMaxsplit       = null;
        String lUrlShortner    = null;
        String lEmailTo    = null;
        String lEmailFrom    = null;
        String lEmailFromName    = null;
        String lEmailSubject    = null;

        if (aRequestType == null)
        {
            final String version   = params.get(InterfaceInputParameters.REQ_PARAMETER_VERSION);
            final String accessKey = params.get(InterfaceInputParameters.REQ_PARAMETER_KEY);
            final String encrypt   = params.get(InterfaceInputParameters.REQ_PARAMETER_ENCRYPTED);
            lScheduleTime   = params.get(InterfaceInputParameters.REQ_PARAMETER_SCHEDULE_AT);
            lDest           = params.get(InterfaceInputParameters.REQ_PARAMETER_DEST);
            lMessage        = params.get(InterfaceInputParameters.REQ_PARAMETER_MSG);
            lHeader         = params.get(InterfaceInputParameters.REQ_PARAMETER_HEADER);
            lMsgType        = params.get(InterfaceInputParameters.REQ_PARAMETER_TYPE);
            lUdhi           = params.get(InterfaceInputParameters.REQ_PARAMETER_UDHI);
            lUdh            = params.get(InterfaceInputParameters.REQ_PARAMETER_UDH);
            lAppendCountry  = params.get(InterfaceInputParameters.REQ_PARAMETER_APPEND_COUNTRY);
            lCountryCode    = params.get(InterfaceInputParameters.REQ_PARAMETER_COUNTRY_CODE);
            lUrlTrack       = params.get(InterfaceInputParameters.REQ_PARAMETER_URL_TRACK);
            lDcs            = params.get(InterfaceInputParameters.REQ_PARAMETER_DCS);
            lSpecialPort    = params.get(InterfaceInputParameters.REQ_PARAMETER_PORT);
            lMsgExpiry      = params.get(InterfaceInputParameters.REQ_PARAMETER_MSG_EXPIRY);
            lCustRef        = params.get(InterfaceInputParameters.REQ_PARAMETER_CUST_REF);
            lTemplateId     = params.get(InterfaceInputParameters.REQ_PARAMETER_TEMPLATE_ID);
            lTemplateValues = params.get(InterfaceInputParameters.REQ_PARAMETER_TEMPLATE_VALUES);
            lDltEntityId    = params.get(InterfaceInputParameters.REQ_PARAMETER_DLT_ENTITY_ID);
            lDltTemplateId  = params.get(InterfaceInputParameters.REQ_PARAMETER_DLT_TEMPLATE_ID);
            lDltTMAId  = params.get(InterfaceInputParameters.REQ_PARAMETER_DLT_TMA_ID);

            lMsgtag         = params.get(InterfaceInputParameters.REQ_PARAMETER_MSG_TAG);
            lParam1         = params.get(InterfaceInputParameters.REQ_PARAMETER_PARAM1);
            lParam2         = params.get(InterfaceInputParameters.REQ_PARAMETER_PARAM2);
            lParam3         = params.get(InterfaceInputParameters.REQ_PARAMETER_PARAM3);
            lParam4         = params.get(InterfaceInputParameters.REQ_PARAMETER_PARAM4);
            lParam5         = params.get(InterfaceInputParameters.REQ_PARAMETER_PARAM5);
            lParam6         = params.get(InterfaceInputParameters.REQ_PARAMETER_PARAM6);
            lParam7         = params.get(InterfaceInputParameters.REQ_PARAMETER_PARAM7);
            lParam8         = params.get(InterfaceInputParameters.REQ_PARAMETER_PARAM8);
            lParam9         = params.get(InterfaceInputParameters.REQ_PARAMETER_PARAM9);
            lParam10        = params.get(InterfaceInputParameters.REQ_PARAMETER_PARAM10);
            lDlrReq         = params.get(InterfaceInputParameters.REQ_PARAMETER_DLR_REQ);
            lMaxsplit       = params.get(InterfaceInputParameters.REQ_PARAMETER_MAX_SPLIT);
            lUrlShortner    = params.get(InterfaceInputParameters.REQ_PARAMETER_URL_SHORTNER);

            lEmailTo    = params.get(InterfaceInputParameters.REQ_PARAMETER_EMAIL_TO);
            lEmailFrom    = params.get(InterfaceInputParameters.REQ_PARAMETER_EMAIL_FROM);
            lEmailFromName    = params.get(InterfaceInputParameters.REQ_PARAMETER_EMAIL_FROM_NAME);
            lEmailSubject    = params.get(InterfaceInputParameters.REQ_PARAMETER_EMAIL_SUBJECT);

            jsonObject.put(InterfaceInputParameters.REQ_PARAMETER_VERSION, version);
            if (accessKey != null)
                jsonObject.put(InterfaceInputParameters.REQ_PARAMETER_KEY, accessKey);

            jsonObject.put(InterfaceInputParameters.REQ_PARAMETER_ENCRYPTED, encrypt);
            jsonObject.put(InterfaceInputParameters.REQ_PARAMETER_SCHEDULE_AT, lScheduleTime);
        }
        else
        {
            final InterfaceParameterLoader interfaceParams = InterfaceParameterLoader.getInstance();
            lMessage        = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.MESSAGE));
            lDest           = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.MOBILE_NUMBER));
            lHeader         = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.SIGNATURE));
            lMsgType        = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.MESSAGE_TYPE));
            lUdhi           = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.UDH_INCLUDE));
            lUdh            = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.UDH));
            lCountryCode    = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.COUNTRY_CODE));
            lDcs            = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.DATA_CODING));
            lSpecialPort    = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.DESTINATION_PORT));
            lMsgExpiry      = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.MESSAGE_EXPIRY));
            lCustRef        = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.CUSTOMER_MSSAGE_ID));
            lTemplateId     = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.TEMPLATE_ID));
            lTemplateValues = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.TEMPLATE_VALUES));
            // lScheduleTime =
            // mHttpRequest.getParameter(interfaceParams.getParamterKey(aClientId,
            // InterfaceType.HTTP_JAPI, InterfaceParameter.SCHEDULE_TIME));
            lAppendCountry  = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.APPEND_COUNTRY_CODE));
            lUrlTrack       = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.URL_TRACKING));
            lDltEntityId    = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.DLT_ENTITY_ID));
            lDltTemplateId  = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.DLT_TEMPLATE_ID));
            lDltTMAId  = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.DLT_TMA_ID));

            lMsgtag         = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.MSG_TAG));
            lParam1         = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.PARAM1));
            lParam2         = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.PARAM2));
            lParam3         = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.PARAM3));
            lParam4         = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.PARAM4));
            lParam5         = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.PARAM5));
            lParam6         = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.PARAM6));
            lParam7         = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.PARAM7));
            lParam8         = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.PARAM8));
            lParam9         = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.PARAM9));
            lParam10        = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.PARAM10));
            lDlrReq         = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.DLR_REQUIRED));
            lMaxsplit       = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.MAX_SPLIT));
            lUrlShortner    = params.get(interfaceParams.getParamterKey(aClientId, InterfaceType.HTTP_JAPI, InterfaceParameter.URL_SHORTNER));
        }

        if ((lMessage != null) && !lMessage.isBlank())
        {
            lMessage = lMessage.replaceAll("\\r", "\n");

            if (log.isDebugEnabled())
                log.debug("process() - Replce NewLine message ' " + lMessage + "  '");
        }

        final JSONArray  lMessagesList = new JSONArray();
        final JSONObject messageObject = new JSONObject();

        // jsonObject.put(InterfaceInputParameters.REQ_PARAMETER_SCHEDULE_AT,
        // CommonUtility.nullCheck(lScheduleTime, true));

        if (log.isDebugEnabled())
            log.debug("Message Tag Value :" + lMsgtag);

        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_TEMPLATE_VALUES, Utility.splitIntoJsonArray(lTemplateValues, "~"));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_MSG, CommonUtility.nullCheck(lMessage, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_HEADER, CommonUtility.nullCheck(lHeader, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_TYPE, CommonUtility.nullCheck(lMsgType, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_UDHI, CommonUtility.nullCheck(lUdhi, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_UDH, CommonUtility.nullCheck(lUdh, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_APPEND_COUNTRY, CommonUtility.nullCheck(lAppendCountry, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_URL_TRACK, CommonUtility.nullCheck(lUrlTrack, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_DCS, CommonUtility.nullCheck(lDcs, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_PORT, CommonUtility.nullCheck(lSpecialPort, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_MSG_EXPIRY, CommonUtility.nullCheck(lMsgExpiry, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_COUNTRY_CODE, CommonUtility.nullCheck(lCountryCode, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_CUST_REF, CommonUtility.nullCheck(lCustRef, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_TEMPLATE_ID, CommonUtility.nullCheck(lTemplateId, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_DLT_ENTITY_ID, CommonUtility.nullCheck(lDltEntityId, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_DLT_TEMPLATE_ID, CommonUtility.nullCheck(lDltTemplateId, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_DLT_TMA_ID, CommonUtility.nullCheck(lDltTMAId, true));

        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_MSG_TAG, CommonUtility.nullCheck(lMsgtag, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_PARAM1, CommonUtility.nullCheck(lParam1, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_PARAM2, CommonUtility.nullCheck(lParam2, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_PARAM3, CommonUtility.nullCheck(lParam3, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_PARAM4, CommonUtility.nullCheck(lParam4, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_PARAM5, CommonUtility.nullCheck(lParam5, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_PARAM6, CommonUtility.nullCheck(lParam6, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_PARAM7, CommonUtility.nullCheck(lParam7, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_PARAM8, CommonUtility.nullCheck(lParam8, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_PARAM9, CommonUtility.nullCheck(lParam9, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_PARAM10, CommonUtility.nullCheck(lParam10, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_DLR_REQ, CommonUtility.nullCheck(lDlrReq, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_MAX_SPLIT, CommonUtility.nullCheck(lMaxsplit, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_URL_SHORTNER, CommonUtility.nullCheck(lUrlShortner, true));

        
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_EMAIL_TO, CommonUtility.nullCheck(lEmailTo, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_EMAIL_FROM, CommonUtility.nullCheck(lEmailFrom, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_EMAIL_FROM_NAME, CommonUtility.nullCheck(lEmailFromName, true));
        messageObject.put(InterfaceInputParameters.REQ_PARAMETER_EMAIL_SUBJECT, CommonUtility.nullCheck(lEmailSubject, true));

        if ((lDest != null) && !lDest.isBlank())
            messageObject.put(InterfaceInputParameters.REQ_PARAMETER_DEST, Utility.splitIntoJsonArray(lDest, ","));

        lMessagesList.add(messageObject);
        jsonObject.put(InterfaceInputParameters.REQ_PARAMETER_MESSAGES, lMessagesList);
    }

   
    @Override
    public String doProcess()
    {
        log.debug("Abstract method..");
        
        return null;
    }

 

}
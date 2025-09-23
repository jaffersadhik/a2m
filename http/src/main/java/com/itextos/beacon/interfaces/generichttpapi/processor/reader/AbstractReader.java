package com.itextos.beacon.interfaces.generichttpapi.processor.reader;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONObject;

import com.itextos.beacon.commonlib.constants.MiddlewareConstant;
import com.itextos.beacon.commonlib.utility.CommonUtility;
import com.itextos.beacon.http.generichttpapi.common.interfaces.IRequestProcessor;
import com.itextos.beacon.http.generichttpapi.common.utils.InterfaceInputParameters;
import com.itextos.beacon.http.generichttpapi.common.utils.Utility;

public abstract class AbstractReader
        implements
        RequestReader
{

    private static final Log            log     = LogFactory.getLog(AbstractReader.class);
    protected static final String       NO_USER = "";
    protected static final String       OVERALL = "OVERALL";

    protected final String              mParameterName;
    String requestBody;
    private Map<String, String> params=null;
 
    // protected boolean isAsync = ("1".equals(APIConstants.IS_ASYNC_INSTANCE));

    AbstractReader(Map<String, String> params,
            String aParameterName
          )
    {
    	this.params = params;
        mParameterName = aParameterName;
  
    }

    
   
    protected static void basicJsonObject(
            JSONObject aJsonObject,
            String aReportingkey)
    {
        aJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_VERSION, "1.0");

        if (!"".equals(aReportingkey))
            aJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_REPORTING_KEY, aReportingkey);
    }

    protected static void basicJsonObject(
            JSONObject aJsonObject,
            String aHeader,
            String aDest,
            String aMessageBody,
            String aReportingKey,
            String aMaxSplit,
            String aAction,
            boolean isUnicodeMessageEncode,
            String aDltEntityId,
            String aDltTemplateId)
            throws Exception
    {
        aJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_VERSION, "1.0");

        if (!"".equals(aReportingKey))
            aJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_REPORTING_KEY, aReportingKey);

        final List<String> lDestList = new ArrayList<>();
        lDestList.add(aDest);

        final Map<String, Object> lMessageObject = new HashMap<>();
        lMessageObject.put(InterfaceInputParameters.REQ_PARAMETER_MAX_SPLIT, ("".equals(aMaxSplit) && isUnicodeMessageEncode) ? "1" : aMaxSplit);
        lMessageObject.put(InterfaceInputParameters.REQ_PARAMETER_DEST, lDestList);
        lMessageObject.put(InterfaceInputParameters.REQ_PARAMETER_HEADER, aHeader);
        lMessageObject.put(InterfaceInputParameters.REQ_PARAMETER_DLT_ENTITY_ID, aDltEntityId);
        lMessageObject.put(InterfaceInputParameters.REQ_PARAMETER_DLT_TEMPLATE_ID, aDltTemplateId);

        if (!"".equals(aAction))
            lMessageObject.put(InterfaceInputParameters.REQ_PARAMETER_PARAM1, aAction);

        if ("".equals(aMessageBody))
            lMessageObject.put(InterfaceInputParameters.REQ_PARAMETER_MSG, aMessageBody);

        final List<Map<String, Object>> lMessagelist = new ArrayList<>();
        lMessagelist.add(lMessageObject);

        aJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_MESSAGES, lMessagelist);
    }

    @Override
    public String processGetRequest()
    {
        String lInputRquest = params.get(mParameterName);

        if (log.isDebugEnabled())
            log.debug(" Input Request from Get method:  '" + lInputRquest + "'");

        lInputRquest = CommonUtility.nullCheck(lInputRquest, true);
        return doProcess();
    }

    @Override
    public String processPostRequest()
            throws Exception
    {
        String lInputRquest = params.get(mParameterName);

        if (log.isDebugEnabled())
            log.debug("Input Request from Post method:  '" + lInputRquest + "'");

        lInputRquest = CommonUtility.nullCheck(lInputRquest, true);

        return doProcess();
    }

    @Override
    public String sendResponse(
            IRequestProcessor aRequestProcessor)
    {

        try
        {
        	
        	String response=aRequestProcessor.generateResponse();
        	if (log.isDebugEnabled())
                log.debug(" response to user after sending to redis:  '" + response + "'");

            return response;
            

        }
        catch (final Exception e)
        {
            log.error("Error while closing the printwriter", e);
        }
        
        return null;
    }

    protected static String getUserName(
            IRequestProcessor aRequestProcessor)
    {
        return aRequestProcessor.getBasicInfo().getUserAccountInfo() == null ? NO_USER : (String) aRequestProcessor.getBasicInfo().getUserAccountInfo().get(MiddlewareConstant.MW_USER.getName());
    }

}
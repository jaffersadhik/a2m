package com.itextos.beacon.interfaces.migration.processor.reader;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.itextos.beacon.commonlib.utility.CommonUtility;
import com.itextos.beacon.http.generichttpapi.common.interfaces.IRequestProcessor;
import com.itextos.beacon.http.generichttpapi.common.utils.InterfaceInputParameters;
import com.itextos.beacon.http.generichttpapi.common.utils.InterfaceMessageClass;
import com.itextos.beacon.http.generichttpapi.common.utils.Utility;
import com.itextos.beacon.http.migration.common.utils.MJsonRequestParameters;

public abstract class MAbsractReader
        implements
        MRequestReader
{

    private static final Log            log     = LogFactory.getLog(MRequestReader.class);
    protected static final String       NO_USER = "";
    protected static final String       OVERALL = "OVERALL";

    protected final String              mParameterName;
    Map<String,String> params;
    MAbsractReader(Map<String,String> params,
            String aParameterName
           )
    {
    	this.params=params;
        mParameterName = aParameterName;
    
    }

    @Override
    public String processPostRequest()
            throws Exception
    {
        String lInputRequest = params.get(mParameterName);

        if (log.isDebugEnabled())
            log.debug("Input Request from Post method:  '" + lInputRequest + "'");

        lInputRequest = CommonUtility.nullCheck(lInputRequest, true);

        return doProcess();
    }

    @Override
    public String processGetRequest()
    {
        final JSONObject lJsonObject = new JSONObject();

        try
        {
            String           lMsgType       = CommonUtility.nullCheck(params.get(MJsonRequestParameters.REQ_PARAMETER_MSGTYPE), true);
            final JSONArray  lMessageArray  = new JSONArray();
            final JSONObject lmsgJsonObject = new JSONObject();
            lJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_VERSION, "1.0");
            lJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_USERNAME, CommonUtility.nullCheck(params.get(MJsonRequestParameters.REQ_PARAMETER_USERNAME), true));
            lJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_KEY, CommonUtility.nullCheck(params.get(MJsonRequestParameters.REQ_PARAMETER_PASSWORD), true));
            lJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_ENCRYPTED, "0");
            final String lDest = CommonUtility.nullCheck(params.get(MJsonRequestParameters.REQ_PARAMETER_TO), true);
            if ((lDest != null) && !lDest.isBlank())
                lmsgJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_DEST, Utility.splitIntoJsonArray(lDest, ","));
            // lmsgJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_DEST, lDestArray);
            final String lRequestMessage = CommonUtility.nullCheck(params.get(MJsonRequestParameters.REQ_PARAMETER_CONTENT), true);
            lmsgJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_MSG, lRequestMessage);
            lmsgJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_HEADER, CommonUtility.nullCheck(params.get(MJsonRequestParameters.REQ_PARAMETER_FROM), true));
            lmsgJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_DLT_ENTITY_ID, CommonUtility.nullCheck(params.get(MJsonRequestParameters.REQ_PARAMETER_ENTITY_ID), true));
            lmsgJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_DLT_TEMPLATE_ID, CommonUtility.nullCheck(params.get(MJsonRequestParameters.REQ_PARAMETER_TEMPLATE_ID), true));
            lmsgJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_PARAM1, CommonUtility.nullCheck(params.get(MJsonRequestParameters.REQ_PARAMETER_PARAM1), true));
            lmsgJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_PARAM2, CommonUtility.nullCheck(params.get(MJsonRequestParameters.REQ_PARAMETER_PARAM2), true));
            lmsgJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_PARAM3, CommonUtility.nullCheck(params.get(MJsonRequestParameters.REQ_PARAMETER_PARAM3), true));
            lmsgJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_PARAM4, CommonUtility.nullCheck(params.get(MJsonRequestParameters.REQ_PARAMETER_PARAM4), true));

            if ((lMsgType != null) && !lMsgType.isBlank())
            {
                lMsgType = doMsgTypeReplace(lMsgType);
                lmsgJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_TYPE, lMsgType);
            }
            else
                if (Utility.isMessageContainsUnicode(lRequestMessage))
                    lmsgJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_TYPE, InterfaceMessageClass.UNICODE.getMessageType());
                else
                    lmsgJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_TYPE, InterfaceMessageClass.PLAIN.getMessageType());
            lMessageArray.add(lmsgJsonObject);
            System.out.println("Message Array is : - " + lMessageArray);
            lJsonObject.put(InterfaceInputParameters.REQ_PARAMETER_MESSAGES, lMessageArray);
            // System.out.println("Json Object to Process is : - " + lJsonObject);
            return doProcess(lJsonObject);
        }
        catch (final Exception e)
        {
            log.error(e);
            
            return null;
        }
    }

    protected static String doMsgTypeReplace(
            String aMsgType)
    {
        String lmsgTypeToProcess = null;

        try
        {

            switch (aMsgType.toUpperCase())
            {
                case MJsonRequestParameters.REQ_PARAMETER_PLAIN_MESSAGE:
                    lmsgTypeToProcess = InterfaceMessageClass.PLAIN.getMessageType();
                    break;

                case MJsonRequestParameters.REQ_PARAMETER_UNICODE_MESSAGE:
                    lmsgTypeToProcess = InterfaceMessageClass.UNICODE.getMessageType();
                    break;

                case MJsonRequestParameters.REQ_PARAMETER_FLASH_MESSAGE:
                    lmsgTypeToProcess = InterfaceMessageClass.FLASH.getMessageType();
                    break;

                default:
                    lmsgTypeToProcess = InterfaceMessageClass.PLAIN.getMessageType();
                    break;
            }
        }
        catch (final Exception e)
        {
            log.error(e);
        }
        return lmsgTypeToProcess;
    }

    @Override
    public String doProcess()
    {
        return null;
    }

    @Override
    public String doProcess(
            JSONObject aJsonObj)
    {
        return null;
    }

    @Override
    public String sendResponse(
            IRequestProcessor aRequestProcessor)
    {

        try
        {
            final String response = aRequestProcessor.generateResponse();
            if (log.isDebugEnabled())
                log.debug(" response to user after sending to redis:  '" + response + "'");


           return response;
        }
        catch (final Exception e)
        {
            log.error("Error while closing the printwriter", e);
            
            return null;
        }
    }

}

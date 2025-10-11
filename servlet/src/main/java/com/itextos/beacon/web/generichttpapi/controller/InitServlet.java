package com.itextos.beacon.web.generichttpapi.controller;

import com.itextos.beacon.commonlib.componentconsumer.processor.ProcessorInfo;
import com.itextos.beacon.commonlib.constants.Component;
import com.itextos.beacon.commonlib.constants.InterfaceType;
import com.itextos.beacon.commonlib.messageidentifier.MessageIdentifier;
import com.itextos.beacon.commonlib.utility.CommonUtility;
import com.itextos.beacon.http.generichttpapi.common.utils.APIConstants;
import com.itextos.beacon.http.interfacefallback.inmem.FallbackQReaper;
import com.itextos.beacon.interfaces.generichttpapi.processor.pollers.FilePoller;

public final class InitServlet
        
{

   
    
    public static void init()
    {
    	
    //	com.itextos.beacon.App.createfolder();

     

        try
        {
            
            final MessageIdentifier lMsgIdentifier = MessageIdentifier.getInstance();
            lMsgIdentifier.init(InterfaceType.HTTP_JAPI);

            final String lAppInstanceId = lMsgIdentifier.getAppInstanceId();

         

            APIConstants.setAppInstanceId(lAppInstanceId);

            if (APIConstants.CLUSTER_INSTANCE == null)
            {
              //  System.exit(-1);
            }

            
            String module=System.getenv("module");
            if(module!=null&&(module.equals("japi")||module.equals("all"))) {
            	
            	FallbackQReaper.getInstance();

            	com.itextos.beacon.platform.dnrfallback.inmem.DlrFallbackQReaper.getInstance();

   //         	startConsumers();
            }
        }
        catch (final Exception e)
        {
        }
    }

    private static void startConsumers()
    {

        if (CommonUtility.isEnabled(APIConstants.START_CONSUMER))
        {
           
            try
            {
                final ProcessorInfo lProcessor = new ProcessorInfo(Component.INTERFACE_ASYNC_PROCESS, false);
                lProcessor.process();
            }
            catch (final Exception e)
            {
                System.exit(-1);
            }

            final FilePoller lFilePoller = new FilePoller();
        }
            
    }

    
    
}
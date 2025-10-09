package com.winnovature.initialstate.servlets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.prometheusmetricsutil.PrometheusMetrics;
import com.itextos.beacon.commonlib.utility.tp.ExecutorFilePoller;
import com.winnovature.initialstate.pollers.CampaignGroupsPoller;
import com.winnovature.initialstate.pollers.CampaignMasterPoller;
import com.winnovature.initialstate.utils.Constants;
import com.winnovature.logger.InitialStageLog;

public class InitializePoller {

	private static final long serialVersionUID = 1L;
	static Log log = LogFactory.getLog(Constants.InitialStageLogger);
	private static final String className = "InitializePoller";
	CampaignMasterPoller campaignMasterPoller = null;
	CampaignGroupsPoller campaignGroupsPoller = null;

	
	public void init()  {
		
		
		
	
		InitialStageLog.getInstance().debug(className+" : init()  " );

			try {

				// thread to fetch records from campaign_master & campaign_files tables and HO to fileSplitQ/groupQ
				if (log.isDebugEnabled())
					log.debug(className + " CampaignMasterPoller/CampaignGroupsPoller starting...");

				campaignMasterPoller = new CampaignMasterPoller("CampaignMasterPoller");
				campaignMasterPoller.setName("CampaignMasterPoller");
				ExecutorFilePoller.getInstance().addTask(campaignMasterPoller, "CampaignMasterPoller");
		//		campaignMasterPoller.start();
		//		ExecutorSheduler.addTask(campaignMasterPoller);

				InitialStageLog.getInstance().debug(className+" : campaignMasterPoller.start()  " );


				if (log.isDebugEnabled())
					log.debug(className + " CampaignMasterPoller started.");
				
				campaignGroupsPoller = new CampaignGroupsPoller("CampaignGroupsPoller");
				campaignGroupsPoller.setName("CampaignGroupsPoller");
				ExecutorFilePoller.getInstance().addTask(campaignGroupsPoller, "CampaignGroupsPoller");
			//	campaignGroupsPoller.start();
			//	ExecutorSheduler.addTask(campaignGroupsPoller);

				InitialStageLog.getInstance().debug(className+" : campaignGroupsPoller.start()  " );

				if (log.isDebugEnabled())
					log.debug(className + " CampaignGroupsPoller started.");
		        PrometheusMetrics.registerApiMetrics();
			} catch (Exception e) {
				log.error(className + " Exception:", e);
				log.error(className + " RESTART FP-InitialStage MODULE ");
			}

	
		
	}

	

}

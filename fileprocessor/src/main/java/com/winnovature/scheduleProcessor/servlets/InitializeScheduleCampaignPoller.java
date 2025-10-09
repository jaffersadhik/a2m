package com.winnovature.scheduleProcessor.servlets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.prometheusmetricsutil.PrometheusMetrics;
import com.itextos.beacon.commonlib.utility.tp.ExecutorFilePoller;
import com.winnovature.scheduleProcessor.pollers.ScheduleCampaignPoller;
import com.winnovature.scheduleProcessor.utils.Constants;

public class InitializeScheduleCampaignPoller  {

	private static final long serialVersionUID = 1L;
	static Log log = LogFactory.getLog(Constants.ScheduleProcessorLogger);
	private static final String className = "InitializeScheduleCampaignPoller";
	ScheduleCampaignPoller CSAPoller = null;


	public void init()  {
		
	
			

			try {
				
				// thread to fetch records from campaign_master & campaign_files tables and HO to fileSplitQ/groupQ
				if (log.isDebugEnabled())
					log.debug(className + " CampaignMasterPoller[CSAPoller] starting...");

				CSAPoller = new ScheduleCampaignPoller("CSAPoller");
				CSAPoller.setName("CSAPoller");
				ExecutorFilePoller.getInstance().addTask(CSAPoller, "CSAPoller");
	//			CSAPoller.start();
	//			ExecutorSheduler.addTask(CSAPoller);

		        PrometheusMetrics.registerApiMetrics();
				if (log.isDebugEnabled())
					log.debug(className + " CampaignMasterPoller[CSAPoller] started.");

			} catch (Exception e) {
				log.error(className + " Exception:", e);
				log.error(className + " RESTART THIS MODULE ");
			}

		
		
	}

	

}

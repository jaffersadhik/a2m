package com.winnovature.initialstate.servlets;

import java.io.IOException;

import javax.servlet.GenericServlet;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.prometheusmetricsutil.PrometheusMetrics;
import com.winnovature.initialstate.pollers.CampaignGroupsPoller;
import com.winnovature.initialstate.pollers.CampaignMasterPoller;
import com.winnovature.initialstate.utils.Constants;
import com.winnovature.logger.InitialStageLog;
import com.winnovature.utils.utils.ExecutorSheduler;

public class InitializePoller extends GenericServlet implements Servlet {

	private static final long serialVersionUID = 1L;
	static Log log = LogFactory.getLog(Constants.InitialStageLogger);
	private static final String className = "InitializePoller";
	CampaignMasterPoller campaignMasterPoller = null;
	CampaignGroupsPoller campaignGroupsPoller = null;

	@Override
	public void init() throws ServletException {
		super.init();
		
		
	
		InitialStageLog.getInstance().debug(className+" : init()  " );

			try {

				// thread to fetch records from campaign_master & campaign_files tables and HO to fileSplitQ/groupQ
				if (log.isDebugEnabled())
					log.debug(className + " CampaignMasterPoller/CampaignGroupsPoller starting...");

				campaignMasterPoller = new CampaignMasterPoller("CampaignMasterPoller");
				campaignMasterPoller.setName("CampaignMasterPoller");
				campaignMasterPoller.start();
		//		ExecutorSheduler.addTask(campaignMasterPoller);

				InitialStageLog.getInstance().debug(className+" : campaignMasterPoller.start()  " );


				if (log.isDebugEnabled())
					log.debug(className + " CampaignMasterPoller started.");
				
				campaignGroupsPoller = new CampaignGroupsPoller("CampaignGroupsPoller");
				campaignGroupsPoller.setName("CampaignGroupsPoller");
				campaignGroupsPoller.start();
			//	ExecutorSheduler.addTask(campaignGroupsPoller);

				InitialStageLog.getInstance().debug(className+" : campaignGroupsPoller.start()  " );

				if (log.isDebugEnabled())
					log.debug(className + " CampaignGroupsPoller started.");
				PrometheusMetrics.registerServer();
		        PrometheusMetrics.registerApiMetrics();
			} catch (Exception e) {
				log.error(className + " Exception:", e);
				log.error(className + " RESTART FP-InitialStage MODULE ");
			}

	
		
	}

	@Override
	public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {

	}

}

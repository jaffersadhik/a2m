package com.winnovature.campaignfinisher.servlets;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.prometheusmetricsutil.PrometheusMetrics;
import com.itextos.beacon.commonlib.utility.tp.ExecutorFilePoller;
import com.winnovature.campaignfinisher.consumers.DQRedisCleaner;
import com.winnovature.campaignfinisher.consumers.PollerCampaignFilesCompleted;
import com.winnovature.campaignfinisher.consumers.PollerCampaignMasterCompleted;
import com.winnovature.campaignfinisher.consumers.QueryExecutor;
import com.winnovature.campaignfinisher.singletons.CampaignFinisherPropertiesTon;
import com.winnovature.campaignfinisher.singletons.RedisConnectionFactory;
import com.winnovature.campaignfinisher.utils.Constants;
import com.winnovature.utils.dtos.RedisServerDetailsBean;

public class ServletInitializer  {
	private static final long serialVersionUID = 1L;
	static Log log = LogFactory.getLog(Constants.CampaignFinisherLogger);
	private static final String className = "[QueryExecutor]";
	QueryExecutor queryExecutionConsumer = null;
	PollerCampaignFilesCompleted pollerCampaignFilesCompleted = null;
	PollerCampaignMasterCompleted pollerCampaignMasterCompleted = null;
	DQRedisCleaner dqRedisCleaner = null;

	
	public  void init()  {
		
		
		
		
		

			try {
				int queryExecutionConsumersCount = CampaignFinisherPropertiesTon.getInstance().getPropertiesConfiguration()
						.getInt(Constants.QueryExcecutionConsumersCount, 1);

				Map<String, RedisServerDetailsBean> configurationFromconfigParams = RedisConnectionFactory.getInstance()
						.getConfigurationFromconfigParams();

				for (RedisServerDetailsBean bean : configurationFromconfigParams.values()) {
					for (int i = 1; i <= queryExecutionConsumersCount; i++) {
						queryExecutionConsumer = new QueryExecutor(bean);
						queryExecutionConsumer.setName("QueryExecutionConsumer" + i);
						queryExecutionConsumer.start();

						///ExecutorSheduler.addTask(queryExecutionConsumer);
						
						if (log.isDebugEnabled())
							log.debug(className + " QueryExecutionConsumer" + i + " started.");
					}
				}

				pollerCampaignFilesCompleted = new PollerCampaignFilesCompleted();
				pollerCampaignFilesCompleted.setName("PollerCampaignFilesCompleted");
				
				ExecutorFilePoller.getInstance().addTask(pollerCampaignFilesCompleted, "PollerCampaignFilesCompleted");
			//	pollerCampaignFilesCompleted.start();
			//	ExecutorSheduler.addTask(pollerCampaignFilesCompleted);
				
				pollerCampaignMasterCompleted = new PollerCampaignMasterCompleted();
				pollerCampaignMasterCompleted.setName("PollerCampaignMasterCompleted");
				ExecutorFilePoller.getInstance().addTask(pollerCampaignMasterCompleted, "PollerCampaignMasterCompleted");

			//	pollerCampaignMasterCompleted.start();
			//	ExecutorSheduler.addTask(pollerCampaignMasterCompleted);

				dqRedisCleaner = new DQRedisCleaner();
				dqRedisCleaner.setName("DQRedisCleaner");
				ExecutorFilePoller.getInstance().addTask(dqRedisCleaner, "DQRedisCleaner");

			//	dqRedisCleaner.start();
			//	ExecutorSheduler.addTask(dqRedisCleaner);
				
		        PrometheusMetrics.registerApiMetrics();

			} catch (Exception e) {
				log.error(className + " Exception:", e);
				log.error(className + " RESTART FP-CampaignFinisher MODULE ");
			}

		
	}

	
}

package com.winnovature.dltfileprocessor.servlets;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;

import com.itextos.beacon.commonlib.utility.tp.ExecutorFilePoller;
import com.winnovature.dltfileprocessor.consumers.DltFileQConsumer;
import com.winnovature.dltfileprocessor.pollers.DltTemplateRequestCompletionPoller;
import com.winnovature.dltfileprocessor.pollers.DltTemplateRequestPoller;
import com.winnovature.dltfileprocessor.singletons.DltFileProcessorPropertiesTon;
import com.winnovature.dltfileprocessor.singletons.RedisConnectionTon;
import com.winnovature.dltfileprocessor.utils.Constants;
import com.winnovature.logger.DLTFileLog;
import com.winnovature.utils.dtos.RedisServerDetailsBean;

public class InitializePollers{

	private static final long serialVersionUID = 1L;
	//static Log log = LogFactory.getLog(Constants.FileUploadLogger);
	static DLTFileLog log = DLTFileLog.getInstance();
	private static final String className = "[InitializePollers]";
	PropertiesConfiguration dltProperties = null;
	DltTemplateRequestPoller dltTemplateRequestPoller = null;
	DltFileQConsumer dltFileQConsumer = null;
	DltTemplateRequestCompletionPoller dltTemplateRequestCompletionPoller = null;

	public void init() {
		



			try {
				if (log.isDebugEnabled()) {
					log.debug(className + " InitializePollers Servlet started...");
				}
				
				dltProperties = DltFileProcessorPropertiesTon.getInstance().getPropertiesConfiguration();
				String instanceId = dltProperties.getString(Constants.MONITORING_INSTANCE_ID);
				String pollerRequired = dltProperties.getString(Constants.DLT_FILE_POLLER_REQUIRED);
				boolean runDltTemplateRequestPoller = false;
				if (StringUtils.isNotBlank(pollerRequired)) {
					runDltTemplateRequestPoller = pollerRequired.trim().equalsIgnoreCase("yes");
				}
				// picks request from dlt_template_request/dlt_template_files tables
				if (runDltTemplateRequestPoller) {
					dltTemplateRequestPoller = new DltTemplateRequestPoller("dltTemplateRequestPoller");
					dltTemplateRequestPoller.setName("dltTemplateRequestPoller");
					ExecutorFilePoller.getInstance().addTask(dltTemplateRequestPoller, "dltTemplateRequestPoller");
			//		dltTemplateRequestPoller.start();
				//	ExecutorSheduler.addTask(dltTemplateRequestPoller);

					if (log.isDebugEnabled()) {
						log.debug(className + " dltTemplateRequestPoller started.");
					}
					
					dltTemplateRequestCompletionPoller = new DltTemplateRequestCompletionPoller();
					dltTemplateRequestCompletionPoller.setName("DltTemplateRequestCompletionPoller");
					ExecutorFilePoller.getInstance().addTask(dltTemplateRequestCompletionPoller, "DltTemplateRequestCompletionPoller");
					
			//		dltTemplateRequestCompletionPoller.start();
			//		ExecutorSheduler.addTask(dltTemplateRequestCompletionPoller);

					if (log.isDebugEnabled()) {
						log.debug(className + " DltTemplateRequestCompletionPoller started.");
					}
				}
				
				String consumersRequired = dltProperties.getString(Constants.DLT_FILE_CONSUMER_REQUIRED);
				boolean runDltFileConsumers = false;
				if (StringUtils.isNotBlank(consumersRequired)) {
					runDltFileConsumers = consumersRequired.trim().equalsIgnoreCase("yes");
				}
				
				if(runDltFileConsumers) {
					int dltFileConsumersPerRedisServer = dltProperties.getInt(Constants.DLT_FILE_CONSUMER_COUNT);

					if (log.isDebugEnabled()) {
						log.debug(className + " dltFileConsumersPerRedisServer = " + dltFileConsumersPerRedisServer);
					}

					List<RedisServerDetailsBean> redisServerDetails = RedisConnectionTon.getInstance()
							.getConfigurationFromconfigParams();
					Iterator<RedisServerDetailsBean> iterator = redisServerDetails.iterator();
					while (iterator.hasNext()) {
						RedisServerDetailsBean bean = iterator.next();
						for (int i = 0; i < dltFileConsumersPerRedisServer; i++) {
							dltFileQConsumer = new DltFileQConsumer(bean, instanceId);
							dltFileQConsumer.setName("Thread" + (i+1) + "-" + "DltFileQConsumer");
							ExecutorFilePoller.getInstance().addTask(dltFileQConsumer, "Thread" + (i+1) + "-" + "DltFileQConsumer");
						//	dltFileQConsumer.start();
						//	ExecutorSheduler.addTask(dltFileQConsumer);

							if (log.isDebugEnabled())
								log.debug("[SplitStageServlet.init()] >>>>>> STARTING DltFileQConsumer  " + (i+1)
										+ " ThreadName:" + dltFileQConsumer.getName() + " bean:" + bean.getIpAddress());
						}
					}
				}

				
			} catch (Exception e) {
				log.error(className + " Exception:", e);
				log.error(className + " RESTART FP-DltFileProcessor MODULE ");
			}

		
	}

	

}

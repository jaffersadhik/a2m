package com.winnovature.handoverstage.servlets;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.constants.InterfaceType;
import com.itextos.beacon.commonlib.messageidentifier.MessageIdentifier;
import com.itextos.beacon.commonlib.utility.tp.ExecutorFilePoller;
import com.itextos.beacon.http.interfacefallback.inmem.FallbackQReaper;
import com.winnovature.handoverstage.consumers.SplitFileConsumer;
import com.winnovature.handoverstage.singletons.HandoverStagePropertiesTon;
import com.winnovature.handoverstage.singletons.RedisConnectionFactory;
import com.winnovature.handoverstage.utils.Constants;
import com.winnovature.logger.HandoverStageLog;
import com.winnovature.utils.dtos.RedisServerDetailsBean;
import com.winnovature.utils.singletons.ConfigParamsTon;
import com.winnovature.utils.utils.App;

public class InitializeConsumersServlet {
	private static final long serialVersionUID = 1L;
	static Log log = LogFactory.getLog(Constants.HandoverStageLogger);
	private static final String className = "[InitializeConsumersServlet]";
	PropertiesConfiguration prop = null;
	SplitFileConsumer consumer = null;
	final MessageIdentifier lMsgIdentifier = MessageIdentifier.getInstance();
	
	
	
	public void init()  {

		App.createfolder();

		HandoverStageLog.getInstance().debug(className+" init() ");

			try {
				lMsgIdentifier.init(InterfaceType.GUI);
				FallbackQReaper.getInstance();
				
				prop = HandoverStagePropertiesTon.getInstance()
						.getPropertiesConfiguration();
				String instanceId = prop
						.getString(com.winnovature.utils.utils.Constants.MONITORING_INSTANCE_ID);
				Map<String, RedisServerDetailsBean> configurationFromconfigParams = RedisConnectionFactory
						.getInstance().getConfigurationFromconfigParams();

				Map<String, String> configMap = (HashMap<String, String>) ConfigParamsTon
						.getInstance().getConfigurationFromconfigParams();

				String queueNameAndSize = configMap
						.get(Constants.HIGH_MEDIUM_LOW_QUEUES_NOOF_CONSUMERS_COUNTS);
				String[] queueNameAndSizeArray = queueNameAndSize.split(":");
				List<String> queueNameAndSizeList = Arrays
						.asList(queueNameAndSizeArray);

				for (RedisServerDetailsBean bean : configurationFromconfigParams
						.values()) {

					for (String queueNameAndConsumer : queueNameAndSizeList) {
						String noofconsumer = queueNameAndConsumer.split("~")[1];
						String queueName = queueNameAndConsumer.split("~")[0];

						for (int i = 0; i < Integer.parseInt(noofconsumer); i++) {

							consumer = new SplitFileConsumer(queueName, bean, instanceId);
							consumer.setName("Thread" + i + "-" + queueName);
							ExecutorFilePoller.getInstance().addTask(consumer, "Thread" + i + "-" + queueName);
					//		consumer.start();
					//		ExecutorSheduler.addTask(consumer);

							HandoverStageLog.getInstance().debug(className+" consumer.start() : "+consumer.getName());

							if (log.isDebugEnabled())
								log.debug(className + "[init] >>>>>> STARTING Handover Consumer  " + i + " QUEUE NAME "
										+ queueName + " bean:" + bean.getIpAddress() + " instanceId:" + instanceId);

						} // end of for loop
					}

				}
				
			} catch (Exception e) {
				log.error(className + "[init]  Exception:", e);
			}

		
	}

}

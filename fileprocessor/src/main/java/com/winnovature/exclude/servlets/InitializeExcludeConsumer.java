package com.winnovature.exclude.servlets;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.utility.tp.ExecutorFilePoller;
import com.winnovature.exclude.consumers.ExcludeConsumer;
import com.winnovature.exclude.singletons.ExcludeProcessorPropertiesTon;
import com.winnovature.exclude.singletons.RedisConnectionFactory;
import com.winnovature.exclude.utils.Constants;
import com.winnovature.utils.dtos.RedisServerDetailsBean;
import com.winnovature.utils.singletons.ConfigParamsTon;

public class InitializeExcludeConsumer {
	private static final long serialVersionUID = 1L;
	static Log log = LogFactory.getLog(Constants.ExcludeLogger);
	private static final String className = "[InitializeExcludeConsumer]";
	PropertiesConfiguration prop = null;
	ExcludeConsumer consumer = null;
	
	
	
	public void init()  {
		


			try {
				prop = ExcludeProcessorPropertiesTon.getInstance()
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
						queueName = queueName + com.winnovature.utils.utils.Constants.EXCLUDE;

						for (int i = 0; i < Integer.parseInt(noofconsumer); i++) {
							consumer = new ExcludeConsumer(queueName, bean, instanceId);
							consumer.setName("Thread" + i + "-" + queueName);
							ExecutorFilePoller.getInstance().addTask(consumer, "Thread" + i + "-" + queueName);
						//	consumer.start();
						//	ExecutorSheduler.addTask(consumer);

							log.info(className
									+ "[init] >>>>>> STARTING ExcludeConsumer  "
									+ i + " QUEUE NAME " + queueName + " bean:"
									+ bean.getIpAddress() + " instanceId:"
									+ instanceId);

							log.info(className + "[init] >>>>>>>> Starting "
									+ queueName + " Consumer... Done");
						} // end of for loop
					}

				}
				
			} catch (Exception e) {
				log.error(className + "[init]  Exception:", e);
			}

		
	}

}

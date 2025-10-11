package com.winnovature.splitstage.servlets;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;

import com.itextos.beacon.commonlib.utility.tp.ExecutorFilePoller;
import com.winnovature.logger.SplitStageLog;
import com.winnovature.splitstage.consumers.FileSplitQConsumer;
import com.winnovature.splitstage.singletons.RedisConnectionTon;
import com.winnovature.splitstage.singletons.SplitStagePropertiesTon;
import com.winnovature.splitstage.utils.Constants;
import com.winnovature.utils.dtos.RedisServerDetailsBean;
import com.winnovature.utils.singletons.ConfigParamsTon;

public class SplitStageServlet  {

	private static final long serialVersionUID = 4534056481918798836L;
	//static Log log = LogFactory.getLog(Constants.SplitStageLogger);
	static SplitStageLog log= SplitStageLog.getInstance();
	String className = "SplitStageServlet";
	FileSplitQConsumer fileSplitQConsumer = null;
	PropertiesConfiguration prop = null;

	

	public void init()  {
		String methodName = " [init] ";

		if (log.isDebugEnabled()) {
			log.debug(className + methodName + "begin ..");
		}


		SplitStageLog.getInstance().debug(className+" init() ");
		

			try {

				prop = SplitStagePropertiesTon.getInstance().getPropertiesConfiguration();

				String instanceId = prop.getString(Constants.MONITORING_INSTANCE_ID);

				Map<String, String> configMap = (HashMap<String, String>) ConfigParamsTon.getInstance()
						.getConfigurationFromconfigParams();

				int splitConsumersPerRedisServer = Integer.parseInt(configMap.get(Constants.SPLIT_CONSUMERS_PER_REDIS));

				SplitStageLog.getInstance().debug(className+" splitConsumersPerRedisServer : "+splitConsumersPerRedisServer);

				if (log.isDebugEnabled()) {
					log.debug(className + methodName + " splitConsumersPerRedisServer = " + splitConsumersPerRedisServer);
				}

				List<RedisServerDetailsBean> redisServerDetails = RedisConnectionTon.getInstance()
						.getConfigurationFromconfigParams();

				Iterator<RedisServerDetailsBean> iterator = redisServerDetails.iterator();

				while (iterator.hasNext()) {

					RedisServerDetailsBean bean = iterator.next();

					for (int i = 0; i < splitConsumersPerRedisServer; i++) {
						fileSplitQConsumer = new FileSplitQConsumer(bean, instanceId);
						fileSplitQConsumer.setName("Thread" + (i+1) + "-" + "SplitQConsumer");
						ExecutorFilePoller.getInstance().addTask(fileSplitQConsumer, "Thread" + (i+1) + "-" + "SplitQConsumer");
					//	fileSplitQConsumer.start();
					//	ExecutorSheduler.addTask(fileSplitQConsumer);

						SplitStageLog.getInstance().debug(className+" fileSplitQConsumer.start() : "+fileSplitQConsumer.getName());

						if (log.isDebugEnabled())
							log.debug("[SplitStageServlet.init()] >>>>>> STARTING FileSplitQConsumer[SplitQConsumer]  " + (i+1)
									+ " ThreadName:" + fileSplitQConsumer.getName() + " bean:" + bean.getIpAddress());
					}

				} // end of REDIS servers iteration

				
			} catch (Exception e) {
				log.error(className + methodName + " >>>> Exception: ", e);
				log.error(className + methodName + " >>>> Please restart SplitStage module. ");
			} catch (Throwable t) {
				log.error(className + methodName + " >>>> Throwable: ", t);
				log.error(className + methodName + " >>>> Please restart SplitStage module. ");
			}

			if (log.isDebugEnabled()) {
				log.debug(className + methodName + " end..");
			}

		
		
	}
}

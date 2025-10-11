package com.winnovature.downloadhandler.servlets;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;

import com.itextos.beacon.commonlib.utility.tp.ExecutorFilePoller;
import com.winnovature.downloadhandler.consumers.CsvToExcelConvertionRequestConsumer;
import com.winnovature.downloadhandler.consumers.PollerDownloadReq;
import com.winnovature.downloadhandler.singletons.DownloadHandlerPropertiesTon;
import com.winnovature.logger.LogDonwloadLog;

public class ServletInitializer {
	private static final long serialVersionUID = 1L;
//	static Log log = LogFactory.getLog(Constants.DownloadHandlerLogger);
	static LogDonwloadLog log =LogDonwloadLog.getInstance();

	private static final String className = "[ServletInitializer]";
	PropertiesConfiguration propertiesConfiguration = null;
	PollerDownloadReq pollerDownladReq = null;
	CsvToExcelConvertionRequestConsumer csvToExcelConvertor = null;
	boolean isPollerDownloadReqRequired = false;

	public void init() {
		
		

		

			try {
				if (log.isDebugEnabled()) {
					log.debug(className + " InitializePollers Servlet started...");
				}
				propertiesConfiguration = DownloadHandlerPropertiesTon.getInstance().getPropertiesConfiguration();

				String isPollerRequired = propertiesConfiguration.getString("download_req.poller.required");

				if (StringUtils.isNotBlank(isPollerRequired)) {
					isPollerDownloadReqRequired = isPollerRequired.trim().equalsIgnoreCase("yes");
				}

				if (isPollerDownloadReqRequired) {
					pollerDownladReq = new PollerDownloadReq();
					pollerDownladReq.setName("PollerDownladReq");
					ExecutorFilePoller.getInstance().addTask(pollerDownladReq, "PollerDownladReq");
			//		pollerDownladReq.start();
				//	ExecutorSheduler.addTask(pollerDownladReq);

				}

				int consumersCount = propertiesConfiguration.getInt("csv.excel.convertion.consumers.count", 5);
				for (int i = 0; i < consumersCount; i++) {
					csvToExcelConvertor = new CsvToExcelConvertionRequestConsumer();
					csvToExcelConvertor.setName("CsvToExcelConvertionRequestConsumer" + (i + 1));
					ExecutorFilePoller.getInstance().addTask(csvToExcelConvertor, "CsvToExcelConvertionRequestConsumer" + (i + 1));
			//		csvToExcelConvertor.start();
			//		ExecutorSheduler.addTask(csvToExcelConvertor);

				}

				
			} catch (Exception e) {
				log.error(className + " Exception:", e);
				log.error(className + " RESTART FP-DownloadHandler MODULE ");
			}

		
	}


}

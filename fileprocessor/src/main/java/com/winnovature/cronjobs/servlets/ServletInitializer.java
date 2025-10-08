package com.winnovature.cronjobs.servlets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.prometheusmetricsutil.PrometheusMetrics;
import com.itextos.beacon.commonlib.utility.tp.ExecutorFilePoller;
import com.winnovature.cronjobs.consumers.CurrencyRatesUpdater;
import com.winnovature.cronjobs.consumers.UnwantedFilesRemoval;
import com.winnovature.cronjobs.utils.Constants;

public class ServletInitializer  {
	private static final long serialVersionUID = 1L;
	private static final String className = "[ServletInitializer]";
	static Log log = LogFactory.getLog(Constants.CronJobLogger);

	CurrencyRatesUpdater currencyRatesUpdater = null;
	UnwantedFilesRemoval unwantedFilesRemoval = null;

	public void init() {
		


			try {
				currencyRatesUpdater = new CurrencyRatesUpdater();
				currencyRatesUpdater.setName("CurrencyRatesUpdater");
				ExecutorFilePoller.getInstance().addTask(currencyRatesUpdater, "CurrencyRatesUpdater");
			//	currencyRatesUpdater.start();
			//	ExecutorSheduler.addTask(currencyRatesUpdater);

			} catch (Exception e) {
				log.error(className + " Exception:", e);
				log.error(className + " RESTART FP-CronJobs MODULE ");
			}
			
			try {
				/* 
				 * Unwanted files includes:
				 * 1. Files(abandoned) uploaded through UI but not used (removed, page changed etc).
				 * 2. Files exceeds their usage time - N days (8 days):
				 * 2.1. Campaign files can be removed after n days after campaigns completion.
				 * 2.2. Groups files can be removed after n days after group completion.
				 * 2.3. DLT Template files can be removed after n days after DLT completion.
				*/
				unwantedFilesRemoval = new UnwantedFilesRemoval();
				unwantedFilesRemoval.setName("UnwantedFilesRemoval");
				ExecutorFilePoller.getInstance().addTask(unwantedFilesRemoval, "UnwantedFilesRemoval");
		//		unwantedFilesRemoval.start();
			//	ExecutorSheduler.addTask(unwantedFilesRemoval);
				
		        PrometheusMetrics.registerApiMetrics();

			} catch (Exception e) {
				log.error(className + " Exception:", e);
				log.error(className + " RESTART FP-CronJobs MODULE ");
			}


		
	}

	

}

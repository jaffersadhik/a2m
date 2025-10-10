package com.winnovature.utils.utils;

public class Main {

	public static void startFileProcessor() {
		
		new com.winnovature.campaignfinisher.servlets.ServletInitializer().init();
	
		new com.winnovature.cronjobs.servlets.ServletInitializer().init();
		
		new com.winnovature.dltfileprocessor.servlets.InitializePollers().init();
		
		new com.winnovature.downloadhandler.servlets.ServletInitializer().init();
		
		new com.winnovature.exclude.servlets.InitializeExcludeConsumer().init();
		
		new com.winnovature.fileuploads.servlets.InitializerServlet().init();
		
		new com.winnovature.groupsprocessor.servlets.InitializePoller().init();
		
		new com.winnovature.initialstate.servlets.InitializePoller().init();
		
		new com.winnovature.scheduleProcessor.servlets.InitializeScheduleCampaignPoller().init();
	
		new com.winnovature.splitstage.servlets.SplitStageServlet().init();
	}
	
	public static void startHandoverStage() {
		
		new com.winnovature.handoverstage.servlets.InitializeConsumersServlet().init();
	}
}

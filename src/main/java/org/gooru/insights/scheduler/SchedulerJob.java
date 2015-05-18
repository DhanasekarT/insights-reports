package org.gooru.insights.scheduler;

import org.gooru.insights.services.CSVFileWriterService;
import org.springframework.beans.factory.annotation.Autowired;

public class SchedulerJob {

	@Autowired
	private CSVFileWriterService csvWriterService;
	
	public CSVFileWriterService getCSVFileWriterService() {
		return csvWriterService;
	}
	
	public void removeExpiredFiles() {
		getCSVFileWriterService().removeExpiredFile();
	}
}

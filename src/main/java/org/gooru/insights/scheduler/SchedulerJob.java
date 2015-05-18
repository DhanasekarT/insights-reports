package org.gooru.insights.scheduler;

import org.gooru.insights.services.ItemService;
import org.springframework.beans.factory.annotation.Autowired;

public class SchedulerJob {

	@Autowired
	private ItemService itemService;
	
	public ItemService getItemService() {
		return itemService;
	}
	
	public void removeExpiredFiles() {
		getItemService().removeExpiredFile();
	}
}

package org.ednovo.gooru.table;

public enum ColumnFamily {

	APIKEY("app_api_key"),
	
	EVENTDETAIL("event_detail"),
	
	EVENTTIMELINE("event_timeline"),
	
	ACTIVITYSTREAM("activity_stream"),
	
	DIMEVENTS("dim_events_list"),
	
	DIMDATE("dim_date"),
	
	DIMTIME("dim_time"),
	
	DIMUSER("dim_user"),
	
	EXTRACTEDUSER("extracted_user"),
	
	EXTRACTEDCODE("extracted_code"),
	
	DIMCONTENTCLASSIFICATION("dim_content_classification"),
	
	DIMRESOURCE("dim_resource"),
	
	TEST("dim_resource_test"),
	
	STAGING("staging_event_detail"),
	
	EVENTFIELDS("event_fields"),
	
	CONFIGSETTINGS("job_config_settings"),
	
	REALTIMECONFIG("real_time_operation_config"),
	
	RECENTVIEWEDRESOURCES("recent_viewed_resources"),
	
	LIVEDASHBOARD("live_dashboard"),
	
	LIVEDASHBOARDTEST("live_dashboard_test"),
	
	MICROAGGREGATION("micro_aggregation"),
	
	ACITIVITYSTREAM("activity_stream"),
	
	REALTIMECOUNTER("real_time_counter"),
	
	REALTIMEAGGREGATOR("real_time_aggregator"),
	
	QUESTIONCOUNT("question_count"),
	
	COLLECTIONITEM("collection_item"),
	
	COLLECTION("collection"),
	
	CLASSPAGE("classpage"),
	
	LICENSE("license"),
	
	RESOURCETYPES("resource_type"),
	
	CATEGORY("category"),
	
	JOBTRACKER("job_tracker"),
	
	TABLE_DATATYPE("table_datatypes"),
	
	ASSESSMENTANSWER("assessment_answer"),
	
	RESOURCE("resource"),
	
	USERCOLLECTIONITEMASSOC("user_collection_item_assoc"),
	
	USER("user"),
	
	JOBCONFIGSETTING("job_config_settings"),
	
	JUNITTESTQUERIES("junit_test_queries");
		
	String name;

	
	private ColumnFamily(String name) {
		this.name = name;
	}


	public String getColumnFamily(){
		return name;
	}

}

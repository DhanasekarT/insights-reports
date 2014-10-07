package org.gooru.insights.services;

import java.util.Map;

import org.elasticsearch.client.Client;

import com.netflix.astyanax.Keyspace;

public interface BaseConnectionService {

	Keyspace connectInsights();
	
	Keyspace connectSearch();
	
	Client getClient();
	
	Map<String,Map<String,String>> getFields();
	
	Map<String, String> getFieldsDataType();
	
	Map<String, Map<String, String>> getFieldsJoinCache();
	
	Map<String, String> getIndexMap();
}

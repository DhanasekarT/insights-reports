package org.gooru.insights.services;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.client.Client;

import com.netflix.astyanax.Keyspace;

public interface BaseConnectionService {

	Keyspace connectInsights();
	
	Keyspace connectSearch();
	
	Client getDevClient();
	
	Client getProdClient();
	
	Map<String,Map<String,String>> getFields();
	
	Map<String, String> getFieldsDataType();
	
	Map<String, Map<String, String>> getFieldsJoinCache();
	
	Map<String, String> getIndexMap();
	
	Map<String,Map<String,Map<String, String>>> getDependentFieldsCache();
	
	Map<String, Map<String, String>>  getFieldsCustomDataType();
	
	boolean clearDataCache();
	
	void clearConnectionCache();
	
	public  Map<String,Object> getUserObject(String sessionToken ,Map<Integer,String> errorMap);
	
	Map<String,Object> getUserObjectData(String sessionToken ,Map<Integer,String> errorMap);
	
	Map<String, String> getFieldArrayHandler();
	
	String getArrayHandler();
	
	Set<String> getLogicalOperations();
	
	Set<String> getEsOperations();
	
	Map<String,String> getDefaultFields();
}

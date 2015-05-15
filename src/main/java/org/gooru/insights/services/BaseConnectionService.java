package org.gooru.insights.services;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.elasticsearch.client.Client;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnList;

public interface BaseConnectionService {

	Keyspace connectInsights();
	
	Client getDevClient();
	
	Client getProdClient();
	
	Map<String,Map<String,String>> getFields();
	
	Map<String, String> getFieldsDataType();
	
	Map<String, Map<String, String>> getFieldsJoinCache();
	
	Map<String, String> getIndexMap();
	
	Map<String,Map<String,Map<String, String>>> getDependentFieldsCache();
	
	Map<String, Map<String, String>>  getFieldsCustomDataType();
	
	void clearDataCache();
	
	void clearConnectionCache();
	
	Map<String,Object> getUserObject(String sessionToken ,Map<Integer,String> errorMap);
	
	Map<String,Object> getUserObjectData(String traceId,String sessionToken);
	
	Map<String, String> getFieldArrayHandler();
	
	String getArrayHandler();
	
	Set<String> getLogicalOperations();
	
	Set<String> getEsOperations();
	
	Set<String> getFormulaOperations();
	
	Set<String> getDataTypes();
	
	Map<String,String> getDefaultFields();
	
	Properties getFileProperties();
	
	Map<String, Map<String, String>> getApiFields();

	String getRealRepoPath();

	String getAppRepoPath();
	
	String getDefaultReplyToEmail();
	
	String getDefaultToEmail();

	ColumnList<String> getColumnListFromCache(String rowKey);
}

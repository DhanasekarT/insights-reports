package org.gooru.insights.services;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.json.JSONArray;

public interface ItemService {

	JSONArray generateQuery(String data,Map<String,Object> dataMap,Map<String,Object> userMap,Map<Integer,String> errorMap);
	
	JSONArray getPartyReport(HttpServletRequest request,String reportType,Map<String,Object> dataMap,Map<String,Object> userMap,Map<Integer,String> errorMap);
	
	JSONArray processApi(String data,Map<String,Object> dataMap,Map<Integer,String> errorMap);
	
	boolean clearDataCache();
	
	void clearConnectionCache();
	
	public  Map<String,Object> getUserObject(String sessionToken ,Map<Integer,String> errorMap);
	
	Boolean clearQuery(String id);
	
	JSONArray getQuery(String prefix,String id,Map<String,Object> dataMap);
	
	JSONArray getCacheData(String prefix,String id);
	
	boolean insertKey(String data);
	
	Map<String, Object> getUserObjectData(String sessionToken, Map<Integer, String> errorMap);
	
	Map<Integer,String> manageReports(String action,String reportName,String data,Map<Integer,String> errorMap);
	
	void getExportReportArray(HttpServletRequest request,String reportType,Map<String,Object> dataMap,Map<String,Object> userMap,Map<Integer,String> errorMap,String emailId,String fileName);
	
	void calculateScore(HttpServletRequest request,String reportType,Map<String,Object> dataMap,Map<String, Object> userMap,Map<Integer,String> errorMap,String eventId, int oldScore);
}

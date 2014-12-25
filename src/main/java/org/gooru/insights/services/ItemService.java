package org.gooru.insights.services;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

public interface ItemService {

	JSONArray getEventDetail(String data,Map<String,Object> dataMap,Map<String,Object> userMap,Map<Integer,String> errorMap);
	
	JSONArray getPartyReport(String data,String reportType,Map<String,Object> dataMap,Map<String,Object> userMap,Map<Integer,String> errorMap);
	
	JSONArray processApi(String data,Map<String,Object> dataMap,Map<Integer,String> errorMap);
	
	boolean clearDataCache();
	
	void clearConnectionCache();
	
	public  Map<String,Object> getUserObject(String sessionToken ,Map<Integer,String> errorMap);
	
	Boolean clearQuery(String id);
	
	JSONArray getQuery(String prefix,String id,Map<String,Object> dataMap);
	
	JSONArray getCacheData(String prefix,String id);
	
	boolean insertKey(String data);
	
	Map<String, Object> getUserObjectData(String sessionToken, Map<Integer, String> errorMap);
}

package org.gooru.insights.services;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.gooru.insights.constants.ResponseParamDTO;
import org.gooru.insights.models.RequestParamsDTO;
import org.json.JSONArray;
import org.json.JSONException;

public interface ItemService {

	ResponseParamDTO<Map<String,Object>> generateQuery(String data,Map<String,Object> userMap)throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getPartyReport(HttpServletRequest request,String reportType,Map<String,Object> userMap)throws Exception;
	
	ResponseParamDTO<Map<String, Object>> processApi(String data,Map<String,Object> dataMap)throws Exception;
	
	ResponseParamDTO<Map<String,Object>> clearDataCache();
	
	ResponseParamDTO<Map<String,Object>> clearConnectionCache();
	
	Map<String,Object> getUserObject(String sessionToken ,Map<Integer,String> errorMap);
	
	ResponseParamDTO<Map<String, String>> clearQuery(String id);
	
	ResponseParamDTO<Map<String,Object>> getQuery(String id,Map<String,Object> dataMap);
	
	ResponseParamDTO<Map<String,Object>> getCacheData(String id,Map<String,Object> userMap);
	
	ResponseParamDTO<Map<String,String>> insertKey(String data);
	
	Map<String, Object> getUserObjectData(String sessionToken);
	
	ResponseParamDTO<Map<Integer,String>> manageReports(String action,String reportName,String data);
	
	ResponseParamDTO<Map<String,Object>> serverStatus();

}

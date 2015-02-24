package org.gooru.insights.services;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.gooru.insights.models.ResponseParamDTO;

public interface ItemService {

	ResponseParamDTO<Map<String,Object>> generateQuery(String data, String sessionToken, Map<String, Object> userData)throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getPartyReport(HttpServletRequest request,String reportType, String sessionToken)throws Exception;
	
	ResponseParamDTO<Map<String, Object>> processApi(String data, String sessionToken)throws Exception;
	
	ResponseParamDTO<Map<String,Object>> clearDataCache();
	
	ResponseParamDTO<Map<String,Object>> clearConnectionCache();
	
	Map<String,Object> getUserObject(String sessionToken ,Map<Integer,String> errorMap);
	
	ResponseParamDTO<Map<String, String>> clearQuery(String id);
	
	ResponseParamDTO<Map<String,Object>> getQuery(String id,String sessionToken);
	
	ResponseParamDTO<Map<String,Object>> getCacheData(String id,String sessionToken);
	
	ResponseParamDTO<Map<String,String>> insertKey(String data);
	
	Map<String, Object> getUserObjectData(String sessionToken);
	
	ResponseParamDTO<Map<Integer,String>> manageReports(String action,String reportName,String data);
	
	ResponseParamDTO<Map<String,Object>> serverStatus();

}

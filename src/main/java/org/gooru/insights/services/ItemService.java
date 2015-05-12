package org.gooru.insights.services;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.models.ResponseParamDTO;

public interface ItemService {

	ResponseParamDTO<Map<String,Object>> generateQuery(String traceId,String data, String sessionToken, Map<String, Object> userData)throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getPartyReport(String traceId,HttpServletRequest request,String reportType, String sessionToken)throws Exception;
	
	ResponseParamDTO<Map<String, Object>> processApi(String traceId,String data, String sessionToken)throws Exception;
	
	ResponseParamDTO<Map<String, String>> clearQuery(String traceId,String id);
	
	ResponseParamDTO<Map<String,Object>> getQuery(String traceId,String id,String sessionToken);
	
	ResponseParamDTO<Map<String,Object>> getCacheData(String traceId,String id,String sessionToken);
	
	ResponseParamDTO<Map<String,String>> insertKey(String traceId,String data);
	
	ResponseParamDTO<Map<Integer,String>> manageReports(String traceId,String action,String reportName,String data);
	
	ResponseParamDTO<Map<String,Object>> serverStatus();

	ResponseParamDTO<Map<String,Object>> clearDataCache();

	ResponseParamDTO<Map<String,Object>> clearConnectionCache();

	ResponseParamDTO<Map<String, Object>> exportReport(HttpServletResponse response, String traceId, String data, String sessionToken, Map<String, Object> userMap);
}

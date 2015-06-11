package org.gooru.insights.services;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.gooru.insights.models.RequestParamsCoreDTO;
import org.gooru.insights.models.RequestParamsDTO;
import org.json.JSONArray;

public interface BaseAPIService {

	RequestParamsDTO buildRequestParameters(String data);
	
	Map<String,Object> getRequestFieldNameValueInMap(HttpServletRequest request,String prefix);
	
	boolean checkNull(Collection<?> request);
	
	boolean checkNull(Object request);
	
	boolean checkNull(String request);
	
	String[] convertStringtoArray(String data);
	
	String convertArraytoString(String[] data);
	
	Set<String> convertStringtoSet(String inputDatas);
	
	String[] getIndices(String names);
	
	boolean checkNull(Map<?,?> request);
	
	boolean checkNull(Integer parameter);
	
	Object[] convertSettoArray(Set<?> set);
	
	String convertCollectiontoString(Collection<String> datas);
	
	List<Map<String, Object>> innerJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child);
	
	List<Map<String,Object>> leftJoin(List<Map<String,Object>> parent,List<Map<String,Object>> child,Set<String> keys);

	List<Map<String, Object>> leftJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String parentKey, String childKey);
	
	List<Map<String, Object>> sortBy(List<Map<String, Object>> requestData, String sortBy, String sortOrder);
	
	JSONArray convertListtoJsonArray(List<Map<String,Object>> result);
	
	JSONArray formatKeyValueJson(List<Map<String,Object>> dataMap,String key) throws org.json.JSONException;

	RequestParamsCoreDTO buildRequestParamsCoreDTO(String data);

	Map<String, Boolean> checkPoint(RequestParamsDTO requestParamsDTO);
	
	<T> T deserialize(String json, Class<T> clazz);
	
	String convertSettoString(Set<String> inputDatas);
}

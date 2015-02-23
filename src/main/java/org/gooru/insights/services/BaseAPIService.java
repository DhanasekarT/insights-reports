package org.gooru.insights.services;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.gooru.insights.constants.ResponseParamDTO;
import org.gooru.insights.models.RequestParamsCoreDTO;
import org.gooru.insights.models.RequestParamsDTO;
import org.json.JSONArray;
import org.json.JSONObject;

public interface BaseAPIService {

	RequestParamsDTO buildRequestParameters(String data);
	
	Map<String,Object> getRequestFieldNameValueInMap(HttpServletRequest request,String prefix);
	
	boolean checkNull(Collection<?> request);
	
	boolean checkNull(Object request);
	
	boolean checkNull(String request);
	
	String[] convertStringtoArray(String data);
	
	String convertArraytoString(String[] data);
	
	Set<String> convertStringtoSet(String inputDatas);
	
	Map<String,Boolean> validateData(RequestParamsDTO requestParamsDTO);
	
	RequestParamsDTO validateUserRole(RequestParamsDTO requestParamsDTO,Map<String,Object> userMap);
	
	String[] getIndices(String names);
	
	boolean checkNull(Map<?,?> request);
	
	boolean checkNull(Integer parameter);
	
	Object[] convertSettoArray(Set<?> set);
	
	String convertCollectiontoString(Collection<String> datas);
	
	JSONArray InnerJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String commonKey);
	
	JSONArray InnerJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child);
	
	List<Map<String, Object>> innerJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child);
	
	List<Map<String, Object>> sortBy(List<Map<String, Object>> requestData, String sortBy, String sortOrder);
	
	JSONArray convertListtoJsonArray(List<Map<String,Object>> result);
	
	JSONArray formatKeyValueJson(List<Map<String,Object>> dataMap,String key) throws org.json.JSONException;

	String convertTimeMstoISO(Object milliseconds);
	
	RequestParamsCoreDTO buildRequestParamsCoreDTO(String data);

	String putRedisCache(String query,Map<String,Object> userMap,JSONObject jsonObject);
	
	boolean clearQuery(String id);
	
	String getQuery(String prefix,String id);
	
	boolean clearQuerys(String[] id);
	
	boolean hasKey(String id);
	
	String getKey(String id);
	
	Set<String> getKeys();
	
	String getRedisRawValue(String key);
	
	boolean insertKey(String data);

	Map<String, Boolean> checkPoint(RequestParamsDTO requestParamsDTO);
	
	void saveQuery(RequestParamsDTO requestParamsDTO, ResponseParamDTO<Map<String,Object>> responseParamDTO, String data, Map<String, Object> userMap);

	<T> T deserialize(String json, Class<T> clazz);
}

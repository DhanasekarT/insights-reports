package org.gooru.insights.services;

import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.gooru.insights.models.RequestParamsDTO;
import org.json.JSONArray;
import org.json.JSONException;

public interface BaseESService {

	JSONArray formDataJSONArray(Map<Integer,Map<String,Object>> requestMap);
	
	List<Map<String, Object>> itemSearch(RequestParamsDTO requestParamsDTO,
			String[] indices,Map<String,Boolean> validatedData,Map<String,Object> dataMap,Map<Integer,String> errorRecord);

	List<Map<String, Object>> leftJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String parentKey, String childKey);

	JSONArray buildAggregateJSON(List<Map<String,Object>> resultList)throws JSONException;
	
	List<Map<String,Object>> formatAggregateKeyValueJson(List<Map<String,Object>> dataMap,String key) throws org.json.JSONException;
}

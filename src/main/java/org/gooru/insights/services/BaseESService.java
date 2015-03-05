package org.gooru.insights.services;

import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.gooru.insights.models.RequestParamsDTO;
import org.json.JSONArray;
import org.json.JSONException;

public interface BaseESService {

	List<Map<String, Object>> generateQuery(RequestParamsDTO requestParamsDTO,
			String[] indices,Map<String,Boolean> validatedData,Map<String,Object> messageData,Map<Integer,String> errorData);
	
	public void singeColumnUpdate(String sourceIndex, String indexName, String typeName, String id, String column, Object value);
}

package org.gooru.insights.services;

import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.gooru.insights.models.RequestParamsDTO;
import org.json.JSONArray;

public interface BaseESService {

	JSONArray searchData(RequestParamsDTO requestParamsDTO,String[] indices,String[] types,Map<String,Boolean> validatedData,Map<Integer,String> errorRecord);
	
	JSONArray formDataJSONArray(Map<Integer,Map<String,Object>> requestMap);
}

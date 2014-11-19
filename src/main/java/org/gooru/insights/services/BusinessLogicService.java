package org.gooru.insights.services;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsPaginationDTO;
import org.json.JSONArray;
import org.json.JSONException;

public interface BusinessLogicService {

	boolean aggregate(String index,RequestParamsDTO requestParamsDTO,SearchRequestBuilder searchRequestBuilder,Map<String,String> metricsName,Map<String,Boolean> validatedData);

	boolean granularityAggregate(String index,RequestParamsDTO requestParamsDTO,SearchRequestBuilder searchRequestBuilder,Map<String,String> metricsName,Map<String,Boolean> validatedData);

	Map<Integer,Map<String,Object>> processAggregateJSON(String groupBy,String resultData,Map<String,String> metrics,boolean hasFilter);

	List<Map<String,Object>> buildAggregateJSON(String[] groupBy,String resultData,Map<String,String> metrics,boolean hasFilter);

	BoolFilterBuilder includeFilter(String index,List<RequestParamsFilterDetailDTO> requestParamsFiltersDetailDTO);
	
	List<Map<String,Object>> buildJSON(String[] groupBy,String resultData,Map<String,String> metrics,boolean hasFilter);

	BoolFilterBuilder customFilter(String index,Map<String,Set<Object>> filterMap,Set<String> userFilter);

	List<Map<String,Object>> leftJoin(List<Map<String,Object>> parent,List<Map<String,Object>> child,Set<String> keys);

	List<Map<String, Object>> leftJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String parentKey, String childKey);

	Map<String,Set<Object>> fetchFilters(String index,List<Map<String,Object>> dataList);
	
	JSONArray convertJSONArray(List<Map<String,Object>> data);
	
	List<Map<String,Object>> getData(String fields,String jsonObject);
	
	List<Map<String,Object>> formDataList(Map<Integer,Map<String,Object>> requestMap);
	
	JSONArray formDataJSONArray(Map<Integer,Map<String,Object>> requestMap);
	
	List<Map<String,Object>> getSource(String result);
	
	List<Map<String,Object>> formJoinKey(Map<String,Set<Object>> filtersMap);

	List<Map<String,Object>> formatAggregateKeyValueJson(List<Map<String,Object>> dataMap,String key) throws org.json.JSONException;
		
	JSONArray buildAggregateJSON(List<Map<String,Object>> resultList) throws JSONException;
		
	List<Map<String,Object>> customPaginate(RequestParamsPaginationDTO requestParamsPaginationDTO,List<Map<String,Object>> data,Map<String,Boolean> validatedData,Map<String,Object> returnMap);
	
	List<Map<String,Object>> aggregatePaginate(RequestParamsPaginationDTO requestParamsPaginationDTO,List<Map<String,Object>> data,Map<String,Boolean> validatedData,Map<String,Object> returnMap);
		
	List<Map<String,Object>> aggregateSortBy(RequestParamsPaginationDTO requestParamsPaginationDTO,List<Map<String,Object>> data,Map<String,Boolean> validatedData);

public List<Map<String,Object>> getRecords(String[] indices,String data,Map<Integer,String> errorRecord,String dataKey);
	
	public List<Map<String,Object>> getMultiGetRecords(String[] indices,Map<String,Map<String,String>> comparekey,String data,Map<Integer,String> errorRecord,String dataKey);
}

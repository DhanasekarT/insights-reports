package org.gooru.insights.services;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;

public interface UpdatedService {

	boolean aggregate(String index,RequestParamsDTO requestParamsDTO,SearchRequestBuilder searchRequestBuilder,Map<String,String> metricsName,Map<String,Boolean> validatedData);

	boolean granularityAggregate(String index,RequestParamsDTO requestParamsDTO,SearchRequestBuilder searchRequestBuilder,Map<String,String> metricsName,Map<String,Boolean> validatedData);

	Map<Integer,Map<String,Object>> processAggregateJSON(String groupBy,String resultData,Map<String,String> metrics,boolean hasFilter);

	List<Map<String,Object>> buildAggregateJSON(String[] groupBy,String resultData,Map<String,String> metrics,boolean hasFilter);

	BoolFilterBuilder includeFilter(String index,List<RequestParamsFilterDetailDTO> requestParamsFiltersDetailDTO);
	
	List<Map<String,Object>> buildJSON(String[] groupBy,String resultData,Map<String,String> metrics,boolean hasFilter);

	BoolFilterBuilder customFilter(String index,Map<String,Set<Object>> filterMap,Set<String> userFilter);
}

package org.gooru.insights.services;

import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;

public interface UpdatedService {

	boolean aggregate(RequestParamsDTO requestParamsDTO,SearchRequestBuilder searchRequestBuilder,Map<String,String> metricsName,Map<String,Boolean> validatedData);

	boolean granularityAggregate(RequestParamsDTO requestParamsDTO,SearchRequestBuilder searchRequestBuilder,Map<String,String> metricsName,Map<String,Boolean> validatedData);

	Map<Integer,Map<String,Object>> processAggregateJSON(String groupBy,String resultData,Map<String,String> metrics,boolean hasFilter);

	List<Map<String,Object>> buildAggregateJSON(String[] groupBy,String resultData,Map<String,String> metrics,boolean hasFilter);

	BoolFilterBuilder includeFilter(List<RequestParamsFilterDetailDTO> requestParamsFiltersDetailDTO);
	
	List<Map<String,Object>> buildHistogramAggregateJSON(String[] groupBy,String resultData,Map<String,String> metrics,boolean hasFilter);
}

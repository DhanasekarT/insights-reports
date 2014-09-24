package org.gooru.insights.services;

import java.util.Map;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.gooru.insights.models.RequestParamsDTO;

public interface UpdatedService {

	boolean aggregate(RequestParamsDTO requestParamsDTO,SearchRequestBuilder searchRequestBuilder,Map<String,String> metricsName);

	boolean granularityAggregate(RequestParamsDTO requestParamsDTO,SearchRequestBuilder searchRequestBuilder,Map<String,String> metricsName);

	Map<Integer,Map<String,Object>> processAggregateJSON(String groupBy,String resultData,Map<String,String> metrics,boolean hasFilter);
}

package org.gooru.insights.services;

import java.util.Map;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.gooru.insights.models.RequestParamsDTO;

public interface UpdatedService {

	boolean aggregate(RequestParamsDTO requestParamsDTO,SearchRequestBuilder searchRequestBuilder);

	boolean granularityAggregate(RequestParamsDTO requestParamsDTO,SearchRequestBuilder searchRequestBuilder);
}

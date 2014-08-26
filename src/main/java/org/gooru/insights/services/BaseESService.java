package org.gooru.insights.services;

import java.util.Map;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.gooru.insights.models.RequestParamsDTO;

public interface BaseESService {

	String searchData(RequestParamsDTO requestParamsDTO,String[] indices,String[] types,String field,QueryBuilder query,FilterBuilder filters,Integer offset,Integer limit,Map<String,String> sort,Map<String,Boolean> validatedData);
}

package org.gooru.insights.services;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.gooru.insights.models.RequestParamsDTO;

public interface BaseESService {

	String searchData(String[] indices,String[] types,String field,QueryBuilder query,FilterBuilder filters);
}

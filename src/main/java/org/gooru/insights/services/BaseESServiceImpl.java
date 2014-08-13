package org.gooru.insights.services;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.gooru.insights.models.RequestParamsDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class BaseESServiceImpl implements BaseESService{

	@Autowired
	BaseConnectionService baseConnectionService;

	@Autowired
	BaseAPIService baseAPIService;

	public Map<String,Object> record(String index,String type,String id){
		GetResponse response = getClient().prepareGet(index, type, id).execute().actionGet();
		return response.getSource();
	}
	
	public long recordCount(String[] indices,String[] types,QueryBuilder query,String id){
		CountRequestBuilder response = getClient().prepareCount(indices);
		if(query != null){
		response.setQuery(query);
		}
		if(types != null && types.length >= 0){
		response.setTypes(types);
		}
		return response.execute().actionGet().getCount();
	}
	
	public String searchData(String[] indices,String[] types,String field,QueryBuilder query,FilterBuilder filters){
		SearchRequestBuilder response = getClient().prepareSearch(indices).setSearchType(SearchType.DFS_QUERY_AND_FETCH);
		if(query != null){
		response.setQuery(query);
		}
		if(filters != null){
			response.setPostFilter(filters);
		}
		if(baseAPIService.checkNull(field)){
		response.addField(field);
		}
		return response.setFrom(0).setSize(20).execute().actionGet().toString();
	}
	
	public Client getClient(){
		return baseConnectionService.getClient();
	}
}

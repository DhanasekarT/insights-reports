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
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.gooru.insights.models.RequestParamsDTO;
import org.json.JSONException;
import org.json.JSONObject;
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
	
	public String searchData(String[] indices,String[] types,String fields,QueryBuilder query,FilterBuilder filters,Integer offset,Integer limit,Map<String,String> sort){
		SearchRequestBuilder searchRequestBuilder = getClient().prepareSearch(indices).setSearchType(SearchType.DFS_QUERY_AND_FETCH);
		if(query != null){
			searchRequestBuilder.setQuery(query);
		}
		if(filters != null){
			searchRequestBuilder.setPostFilter(filters);
		}
		if(baseAPIService.checkNull(sort)){
			for(Map.Entry<String, String> map : sort.entrySet()){
			searchRequestBuilder.addSort(map.getKey(), (map.getValue().equalsIgnoreCase("ASC") ? SortOrder.ASC : SortOrder.DESC));
			}
		}
		if(baseAPIService.checkNull(fields)){
			for(String field : fields.split(",")){
				searchRequestBuilder.addField(field);
			}
		}
		//pagination not working,may scroll API will help
		searchRequestBuilder = searchRequestBuilder.setFrom(offset);
		searchRequestBuilder = searchRequestBuilder.setSize(limit);
		String value  = searchRequestBuilder.execute().actionGet().toString();
		return value;
		
	}
	
	public Client getClient(){
		return baseConnectionService.getClient();
	}
}

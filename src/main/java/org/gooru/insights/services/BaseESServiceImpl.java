package org.gooru.insights.services;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.criteria.Order;

import org.antlr.misc.Interval;
import org.antlr.misc.IntervalSet;
import org.apache.lucene.index.Terms;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.aggregations.bucket.histogram.*;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.joda.time.format.DateTimeFormatterBuilder;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.facet.FacetBuilder;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacetBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.APIConstants.hasdata;
import org.gooru.insights.constants.ESConstants;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsFilterFieldsDTO;
import org.gooru.insights.models.RequestParamsPaginationDTO;
import org.gooru.insights.models.RequestParamsSortDTO;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.gdata.client.blogger.BlogPostQuery.OrderBy;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.yammer.metrics.core.Histogram;

@Service
public class BaseESServiceImpl implements BaseESService,APIConstants,ESConstants {

	@Autowired
	BaseConnectionService baseConnectionService;

	@Autowired
	BaseAPIService baseAPIService;
	
	@Autowired
	UpdatedService updatedService;

	RequestParamsSortDTO requestParamsSortDTO;

	RequestParamsFilterDetailDTO requestParamsFiltersDetailDTO;

	public Map<String, Object> record(String index, String type, String id) {
		GetResponse response = getClient().prepareGet(index, type, id)
				.execute().actionGet();
		return response.getSource();
	}
	public long recordCount(String[] indices, String[] types,
			QueryBuilder query, String id) {
		CountRequestBuilder response = getClient().prepareCount(indices);
		if (query != null) {
			response.setQuery(query);
		}
		if (types != null && types.length >= 0) {
			response.setTypes(types);
		}
		return response.execute().actionGet().getCount();
	}

	public JSONArray searchData(RequestParamsDTO requestParamsDTO,
			String[] indices, String[] types,
			Map<String,Boolean> validatedData,Map<Integer,String> errorRecord) {
		
		Map<String,String> metricsName = new HashMap<String,String>();
		boolean hasAggregate = false;
		String result ="[{}]";
		String fields = esFields(requestParamsDTO.getFields());
		String dataKey=esSources.SOURCE.esSource();

		SearchRequestBuilder searchRequestBuilder = getClient().prepareSearch(
				indices).setSearchType(SearchType.DFS_QUERY_AND_FETCH);


		if(validatedData.get(hasdata.HAS_FEILDS.check()))
			dataKey=esSources.FIELDS.esSource();
		
		if (validatedData.get(hasdata.HAS_FEILDS.check())) {
			for (String field : fields.split(",")) {
				searchRequestBuilder.addField(field);
			}
		}
		
		if (validatedData.get(hasdata.HAS_GRANULARITY.check())) {
			updatedService.granularityAggregate(requestParamsDTO, searchRequestBuilder,metricsName,validatedData);
			hasAggregate = true;
			} 
		
		if (!validatedData.get(hasdata.HAS_GRANULARITY.check())) {
			updatedService.aggregate(requestParamsDTO, searchRequestBuilder,metricsName,validatedData);
			hasAggregate = true;
		}
		
		if(!hasAggregate){
				// Add filter in Query
				if(validatedData.get(hasdata.HAS_FILTER.check()))
				searchRequestBuilder.setPostFilter(updatedService.includeFilter(requestParamsDTO.getFilter()));
		}
		
		if(validatedData.get(hasdata.HAS_SORTBY.check()))
		sortData(requestParamsDTO.getPagination().getOrder(),searchRequestBuilder,validatedData);
		
		 searchRequestBuilder.setPreference("_primaries");

		 if(validatedData.get(hasdata.HAS_PAGINATION.check()))
		paginate(searchRequestBuilder, requestParamsDTO.getPagination(), validatedData);
		
		System.out.println("query \n"+searchRequestBuilder);
		
		try{
		result =  searchRequestBuilder.execute().actionGet().toString();
		}catch(Exception e){
			e.printStackTrace();
			errorRecord.put(500, "please contact the developer team for knowing about the error details.");
		}
		
		if(hasAggregate){
		try {
			String groupBy[] = requestParamsDTO.getGroupBy().split(",");
			
			return baseAPIService.formatKeyValueJson(updatedService.buildAggregateJSON(groupBy, result, metricsName, validatedData.get(hasdata.HAS_FILTER.check())),groupBy[groupBy.length-1]);
			
		} catch (JSONException e) {
			e.printStackTrace();
		}
		}
		
		return getRecords(result,errorRecord,dataKey);

	}
	
	public void sortData(List<RequestParamsSortDTO> requestParamsSortDTO,SearchRequestBuilder searchRequestBuilder,Map<String,Boolean> validatedData){
			for(RequestParamsSortDTO sortData : requestParamsSortDTO){
				if(validatedData.get(hasdata.HAS_SORTBY.check()))
				searchRequestBuilder.addSort(esFields(sortData.getSortBy()), (baseAPIService.checkNull(sortData.getSortOrder()) && sortData.getSortOrder().equalsIgnoreCase("DESC")) ? SortOrder.DESC : SortOrder.ASC);
		}
	}

	public void paginate(SearchRequestBuilder searchRequestBuilder,RequestParamsPaginationDTO requestParamsPaginationDTO,Map<String,Boolean> validatedData) {
		searchRequestBuilder.setFrom(validatedData.get(hasdata.HAS_Offset.check()) ? requestParamsPaginationDTO.getOffset().intValue() : 0);
		searchRequestBuilder.setSize(validatedData.get(hasdata.HAS_LIMIT.check()) ? requestParamsPaginationDTO.getLimit().intValue() : 10);
	}

	
	public List<Map<String,Object>> subSearch(RequestParamsDTO requestParamsDTOs,String[] indices,String fields,Map<String,Set<Object>> filtersData){
		SearchRequestBuilder searchRequestBuilder = getClient().prepareSearch(
				indices).setSearchType(SearchType.DFS_QUERY_AND_FETCH);
		
		if(baseAPIService.checkNull(fields)){
			for(String field : fields.split(",")){
		searchRequestBuilder.addField(field);
			}
		}
		BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
		
		for(Map.Entry<String,Set<Object>> entry : filtersData.entrySet()){
			boolFilter.must(FilterBuilders.inFilter(entry.getKey(),baseAPIService.convertSettoArray(entry.getValue())));
		}
		updatedService.includeFilter(requestParamsDTOs.getFilter());
		searchRequestBuilder.setPostFilter(boolFilter);
	System.out.println(" sub query \n"+searchRequestBuilder);
	
		String resultSet = searchRequestBuilder.execute().actionGet().toString();
		return getData(fields, resultSet);
	}
	
	public List<Map<String,Object>> getData(String fields,String jsonObject){
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		try{
			JSONObject json = new JSONObject(jsonObject);
			json = new JSONObject(json.get("hits").toString());
		JSONArray hitsArray = new JSONArray(json.get("hits").toString());
		
		for(int i=0;i<hitsArray.length();i++){
			Map<String,Object> resultMap = new HashMap<String,Object>();
		JSONObject getSourceJson = new JSONObject(hitsArray.get(i).toString());
		getSourceJson = new JSONObject(getSourceJson.get("_source").toString());
		if(baseAPIService.checkNull(fields)){
		for(String field : fields.split(",")){
			if(getSourceJson.has(field)){
			resultMap.put(field, getSourceJson.get(field));
			}
		}
		}else{
			Iterator<String> keys = getSourceJson.keys();
			while(keys.hasNext()){
				String key=keys.next();
				resultMap.put(key, getSourceJson.get(key));
			}
		}
		resultList.add(resultMap);
		}
		}catch(Exception e){
			System.out.println(" get Data method failed");
			e.printStackTrace();
		}
		return resultList;
	}
	
	public List<Map<String,Object>> formDataList(Map<Integer,Map<String,Object>> requestMap){
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		for(Map.Entry<Integer,Map<String,Object>> entry : requestMap.entrySet()){
			resultList.add(entry.getValue());
		}
		return resultList;
	}
	
	public JSONArray formDataJSONArray(Map<Integer,Map<String,Object>> requestMap){
		JSONArray jsonArray = new JSONArray();
		for(Map.Entry<Integer,Map<String,Object>> entry : requestMap.entrySet()){
			jsonArray.put(entry.getValue());
		}
		return jsonArray;
	}
	
	public List<Map<String,Object>> getSource(String result){
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		try {
			Gson gson = new Gson();
			Type mapType = new TypeToken<Map<String,Object>>(){}.getType();
			JSONObject mainJson = new JSONObject(result);
			mainJson = new JSONObject(mainJson.get("hits").toString());
			JSONArray jsonArray = new JSONArray(mainJson.get("hits").toString());
			for(int i=0;i<jsonArray.length();i++){
				 mainJson = new JSONObject(jsonArray.get(i).toString());
				 Map<String,Object> dataMap = new HashMap<String,Object>();	 
				 dataMap = gson.fromJson(mainJson.getString("_source"),mapType);
				 resultList.add(dataMap);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return resultList;
	}
	public List<Map<String,Object>> formJoinKey(Map<String,Set<Object>> filtersMap){
		List<Map<String,Object>> formedList = new ArrayList<Map<String,Object>>();
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		for(Map.Entry<String,Set<Object>> entry : filtersMap.entrySet()){
			for(Object value : entry.getValue()){
				Map<String,Object> formedMap = new HashMap<String,Object>();
			formedMap.put(entry.getKey(), value);
			formedList.add(formedMap);
			}
		}
		if(baseAPIService.checkNull(formedList)){
			if(baseAPIService.checkNull(resultList)){
				List<Map<String,Object>> tempList = new ArrayList<Map<String,Object>>();
				for(Map<String,Object> resultMap : resultList){
					for(Map<String,Object> formedMap : formedList){
						tempList.add(formedMap);
						tempList.add(resultMap);
					}	
				}
				resultList = tempList;
			}else{
				resultList = formedList;
			}
		}
		return resultList;
	}
	public Client getClient() {
		return baseConnectionService.getClient();
	}
	
	
	public JSONArray getRecords(String data,Map<Integer,String> errorRecord,String dataKey){
		JSONObject json;
		JSONArray jsonArray = new JSONArray();
		JSONArray resultJsonArray = new JSONArray();
		try {
			json = new JSONObject(data);
			json = new JSONObject(json.get("hits").toString());
			jsonArray = new JSONArray(json.get("hits").toString());
			if(!dataKey.equalsIgnoreCase("fields")){
			for(int i =0;i< jsonArray.length();i++){
				json = new JSONObject(jsonArray.get(i).toString());
				JSONObject fieldJson = new JSONObject(json.get(dataKey).toString());
				json = new JSONObject();
				
				Iterator<String> keys = fieldJson.keys();
				while(keys.hasNext()){
					String key =keys.next(); 
					json.put(esFields(key), fieldJson.get(key));
				}
				resultJsonArray.put(json);
			}
			}else{
				for(int i =0;i< jsonArray.length();i++){
					JSONObject resultJson = new JSONObject();
				json = new JSONObject(jsonArray.get(i).toString());
				json = new JSONObject(json.get(dataKey).toString());
				 Iterator<String> keys =json.keys();
				 while(keys.hasNext()){
					 String key = keys.next();
					 JSONArray fieldJsonArray = new JSONArray(json.get(key).toString());
					if(fieldJsonArray.length() == 1){	 
					 resultJson.put(apiFields(key),fieldJsonArray.get(0));
					}else{
						resultJson.put(apiFields(key),fieldJsonArray);
					}
				 }
				 resultJsonArray.put(resultJson);
			}
			}
			return resultJsonArray;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return resultJsonArray;
	}
	
	public String esFields(String fields){
		Map<String,String> mappingfields = baseConnectionService.getFields();
		StringBuffer esFields = new StringBuffer();
		for(String field : fields.split(",")){
			if(esFields.length() > 0){
				esFields.append(",");
			}
			if(mappingfields.containsKey(field)){
				esFields.append(mappingfields.get(field));
			}else{
				esFields.append(field);
			}
		}
		return esFields.toString();
	}
	
	public String apiFields(String fields){
		Map<String,String> mappingfields = baseConnectionService.getFields();
		Set<String> keys = mappingfields.keySet();
		Map<String,String> apiFields =new HashMap<String, String>();
		for(String key : keys){
			apiFields.put(mappingfields.get(key), key);
		}
		StringBuffer esFields = new StringBuffer();
		for(String field : fields.split(",")){
			if(esFields.length() > 0){
				esFields.append(",");
			}
			if(apiFields.containsKey(field)){
				esFields.append(apiFields.get(field));
			}else{
				esFields.append(field);
			}
		}
		return esFields.toString();
	}
}

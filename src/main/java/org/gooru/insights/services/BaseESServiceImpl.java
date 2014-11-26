package org.gooru.insights.services;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.ESConstants;
import org.gooru.insights.constants.ESConstants.esConfigs;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsPaginationDTO;
import org.gooru.insights.models.RequestParamsSortDTO;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.mortbay.servlet.jetty.IncludableGzipFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

@Service
public class BaseESServiceImpl implements BaseESService,APIConstants,ESConstants {

	@Autowired
	BaseConnectionService baseConnectionService;

	@Autowired
	BaseAPIService baseAPIService;
	
	@Autowired
	BusinessLogicService businessLogicService;

	RequestParamsSortDTO requestParamsSortDTO;

	RequestParamsFilterDetailDTO requestParamsFiltersDetailDTO;

	public List<Map<String,Object>> itemSearch(RequestParamsDTO requestParamsDTO,
			String[] indices,
			Map<String,Boolean> validatedData,Map<String,Object> dataMap,Map<Integer,String> errorRecord) {
		
		List<Map<String,Object>> dataList = new ArrayList<Map<String,Object>>();
		Map<String,Set<Object>> filterMap = new HashMap<String,Set<Object>>();

		dataList = searchData(requestParamsDTO,new String[]{ indices[0]},new String[]{ indexTypes.get(indices[0])},validatedData,dataMap,errorRecord,filterMap);
		
		if(dataList.isEmpty())
		return new ArrayList<Map<String,Object>>();			
		
		filterMap = businessLogicService.fetchFilters(indices[0], dataList);
		
		for(int i=1;i<indices.length;i++){
		
			Set<String> usedFilter = new HashSet<String>();
			Map<String,Set<Object>> innerFilterMap = new HashMap<String,Set<Object>>();

			List<Map<String,Object>> resultList = multiGet(requestParamsDTO,new String[]{ indices[i]}, new String[]{ indexTypes.get(indices[i])}, validatedData,filterMap,errorRecord,dataList.size(),usedFilter);
			
			innerFilterMap = businessLogicService.fetchFilters(indices[i], resultList);
			filterMap.putAll(innerFilterMap);
			
			System.out.println("filter Map: "+filterMap);
			System.out.println("index "+indices[i]+" result : "+resultList);
			System.out.println("user filter : "+usedFilter);
			
			dataList = businessLogicService.leftJoin(dataList, resultList,usedFilter);
		}
		
		System.out.println("combined "+ dataList);
		if(validatedData.get(hasdata.HAS_GROUPBY.check()) && validatedData.get(hasdata.HAS_GRANULARITY.check())){
		try {
			String groupBy[] = requestParamsDTO.getGroupBy().split(",");
			dataList = businessLogicService.formatAggregateKeyValueJson(dataList, groupBy[groupBy.length-1]);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		dataList = businessLogicService.aggregatePaginate(requestParamsDTO.getPagination(), dataList, validatedData, dataMap);		
		}
		
		if(!dataMap.containsKey("totalRows")){
			dataMap.put("totalRows", dataList.size());
		}
	return dataList;
	}
	
	public List<Map<String,Object>> getItem(RequestParamsDTO requestParamsDTO,
			String[] indices,
			Map<String,Boolean> validatedData,Map<String,Object> dataMap,Map<Integer,String> errorRecord) {
		
		List<Map<String,Object>> dataList = new ArrayList<Map<String,Object>>();
		Map<String,Set<Object>> filterMap = new HashMap<String,Set<Object>>();
		dataList = searchData(requestParamsDTO,new String[]{ indices[0]},new String[]{ indexTypes.get(indices[0])},validatedData,dataMap,errorRecord,filterMap);
		System.out.println(" result data : "+dataList);
		
		if(dataList.isEmpty())
		return new ArrayList<Map<String,Object>>();			
		
		filterMap = businessLogicService.fetchFilters(indices[0], dataList);
		System.out.println("filter Map: "+filterMap);
		
		for(int i=1;i<indices.length;i++){
			
			Set<String> usedFilter = new HashSet<String>();
			Map<String,Set<Object>> innerFilterMap = new HashMap<String,Set<Object>>();
			
			List<Map<String,Object>> resultList = multiGet(requestParamsDTO,new String[]{ indices[i]}, new String[]{ indexTypes.get(indices[i])}, validatedData,filterMap,errorRecord,dataList.size(),usedFilter);
			
			innerFilterMap = businessLogicService.fetchFilters(indices[i], resultList);
			filterMap.putAll(innerFilterMap);
			
			System.out.println("filter Map: "+filterMap);
			System.out.println("user filter : "+usedFilter);
			dataList = businessLogicService.leftJoin(dataList, resultList,usedFilter);
		}
		
	return dataList;
	}
	
	public List<Map<String,Object>> multiGet(RequestParamsDTO requestParamsDTO,
			String[] indices, String[] types,
			Map<String,Boolean> validatedData,Map<String,Set<Object>> filterMap,Map<Integer,String> errorRecord,Integer limit,Set<String> usedFilter) {
		
		SearchRequestBuilder searchRequestBuilder = getClient(requestParamsDTO.getSourceIndex()).prepareSearch(
				indices).setSearchType(SearchType.DFS_QUERY_AND_FETCH);

		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		String result ="[{}]";
		String dataKey=esSources.SOURCE.esSource();
		String fields = esFields(indices[0],requestParamsDTO.getFields());
		System.out.println("fields"+fields);
		
		if(fields.contains("code_id") || fields.contains("label")){
		fields = fields+",depth";	
		}
		
		if(validatedData.get(hasdata.HAS_FEILDS.check()))
			dataKey=esSources.FIELDS.esSource();
		
		if (validatedData.get(hasdata.HAS_FEILDS.check())) {
			for (String field : fields.split(",")) {
				searchRequestBuilder.addField(field);
			}
		}
		
		if(!filterMap.isEmpty()){
			BoolFilterBuilder filterData = businessLogicService.customFilter(indices[0],filterMap,usedFilter);
			if(filterData.hasClauses())
			searchRequestBuilder.setPostFilter(filterData);
		}


		searchRequestBuilder.setPreference("_primaries");

		searchRequestBuilder.setSize(limit);
		
		try{
			System.out.println("mutiget query "+searchRequestBuilder);
		result =  searchRequestBuilder.execute().actionGet().toString();
		}catch(Exception e){
			e.printStackTrace();
			errorRecord.put(500, "please contact the developer team for knowing about the error details.");
		}
		
		
		resultList = businessLogicService.getRecords(indices,result, errorRecord, dataKey);
		
		return resultList;
	}
	
	public static void main(String[] args){
		 Client client = null;
		 Settings settings = ImmutableSettings.settingsBuilder().put(esConfigs.ES_CLUSTER.esConfig(),"").put("client.transport.sniff", true).build();
         TransportClient transportClient = new TransportClient(settings);
         transportClient.addTransportAddress(new InetSocketTransportAddress("localhost",9300));
         client = transportClient;
         String search[] = new String[1];
         search[0] = "sort_test";
		SearchRequestBuilder searchRequestBuilder =client.prepareSearch(search).setSearchType(SearchType.DFS_QUERY_THEN_FETCH);	

		searchRequestBuilder.setFrom(1);
		searchRequestBuilder.setSize(1);
		TermsBuilder termBuilder  = AggregationBuilders.terms("name").field("name");
		termBuilder.subAggregation(AggregationBuilders.sum("score").field("score")).order(org.elasticsearch.search.aggregations.bucket.terms.Terms.Order.aggregation("score",false));
		searchRequestBuilder.addAggregation(termBuilder);
		System.out.println("query"+searchRequestBuilder);
		System.out.println(searchRequestBuilder.execute().actionGet());
		
	}
	
	public List<Map<String,Object>> searchData(RequestParamsDTO requestParamsDTO,
			String[] indices, String[] types,
			Map<String,Boolean> validatedData,Map<String,Object> dataMap,Map<Integer,String> errorRecord,Map<String,Set<Object>> filterMap) {
		
		Map<String,String> metricsName = new HashMap<String,String>();
		boolean hasAggregate = false;
		String result ="[{}]";
		String fields = esFields(indices[0],requestParamsDTO.getFields());
		String dataKey=esSources.SOURCE.esSource();

		System.out.print("indices :" + indices);
		SearchRequestBuilder searchRequestBuilder =  null;
		
		if (validatedData.get(hasdata.HAS_GROUPBY.check())) {
			searchRequestBuilder = getClient(requestParamsDTO.getSourceIndex()).prepareSearch(
				indices).setSearchType(SearchType.DFS_QUERY_AND_FETCH);
		}else{
			searchRequestBuilder = getClient(requestParamsDTO.getSourceIndex()).prepareSearch(
					indices).setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		}

		if(validatedData.get(hasdata.HAS_FEILDS.check()))
			dataKey=esSources.FIELDS.esSource();
		
		if (validatedData.get(hasdata.HAS_FEILDS.check())) {
			for (String field : fields.split(",")) {
				searchRequestBuilder.addField(field);
			}
		}
		
		if (validatedData.get(hasdata.HAS_GRANULARITY.check())) {
			int recordSize = 500;
			if(validatedData.get(hasdata.HAS_PAGINATION.check())){
				recordSize = requestParamsDTO.getPagination().getLimit();
			}
			searchRequestBuilder.setNoFields();
			businessLogicService.granularityAggregate(indices[0],requestParamsDTO, searchRequestBuilder,metricsName,validatedData,recordSize);
			hasAggregate = true;
			} 
		
		if (!validatedData.get(hasdata.HAS_GRANULARITY.check()) && validatedData.get(hasdata.HAS_GROUPBY.check())) {
			int recordSize = 500;
			if(validatedData.get(hasdata.HAS_PAGINATION.check())){
				recordSize = requestParamsDTO.getPagination().getLimit();
			}
			searchRequestBuilder.setNoFields();
			businessLogicService.aggregate(indices[0],requestParamsDTO, searchRequestBuilder,metricsName,validatedData,recordSize);
			hasAggregate = true;
		}
/*		if (validatedData.get(hasdata.HAS_GROUPBY.check())) {
			int recordSize = 500;
			if(validatedData.containsKey(hasdata.HAS_PAGINATION.check())){
				recordSize = requestParamsDTO.getPagination().getLimit();
			}
			businessLogicService.granularityAggregate(indices[0],requestParamsDTO, searchRequestBuilder,metricsName,validatedData,recordSize);
			hasAggregate = true;
			} 
*/		
		
		if(!hasAggregate){

				// Add filter in Query
				if(validatedData.get(hasdata.HAS_FILTER.check()))
				searchRequestBuilder.setPostFilter(businessLogicService.includeFilter(indices[0],requestParamsDTO.getFilter()));

				if(validatedData.get(hasdata.HAS_SORTBY.check()))
					sortData(indices,requestParamsDTO.getPagination().getOrder(),searchRequestBuilder,validatedData);
		}

		//Include only source file to avoid miss functionality of data during aggregation on ES version 1.2.2 
		 searchRequestBuilder.setPreference("_primaries");

		 //currently its not working in ES version 1.2.2,its shows record count is 1 * no of shades = total Records
		 System.out.println(" pagination status "+validatedData);
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
			List<Map<String,Object>> data = businessLogicService.buildJSON(groupBy, result, metricsName, validatedData.get(hasdata.HAS_FILTER.check()));
			data = businessLogicService.aggregateSortBy(requestParamsDTO.getPagination(), data, validatedData,dataMap);
			
			if(!validatedData.get(hasdata.HAS_GRANULARITY.check()))
				data = businessLogicService.customPaginate(requestParamsDTO.getPagination(), data, validatedData, dataMap);
			
			return data;
		} catch (Exception e) {
			e.printStackTrace();
		}
		}
		
		List<Map<String,Object>> data = businessLogicService.getRecords(indices,result,errorRecord,dataKey);
		
//		data = businessLogicService.customPaginate(requestParamsDTO.getPagination(), data, validatedData, dataMap);
		
		return data;

	}
	
	public List<Map<String,Object>> subSearch(RequestParamsDTO requestParamsDTOs,String[] indices,String fields,Map<String,Set<Object>> filtersData){
		SearchRequestBuilder searchRequestBuilder = getClient(requestParamsDTOs.getSourceIndex()).prepareSearch(
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
		businessLogicService.includeFilter(indices[0],requestParamsDTOs.getFilter());
		searchRequestBuilder.setPostFilter(boolFilter);
	System.out.println(" sub query \n"+searchRequestBuilder);
	
		String resultSet = searchRequestBuilder.execute().actionGet().toString();
		return businessLogicService.getData(fields, resultSet);
	}
	
	
	public Client getClient(String indexSource) {
		if(indexSource != null && indexSource.equalsIgnoreCase("dev")){
			return baseConnectionService.getDevClient();
		}else if(indexSource != null && indexSource.equalsIgnoreCase("prod")){
			return baseConnectionService.getProdClient();
		}else{			
			return baseConnectionService.getProdClient();
		}
	}

	public void sortData(String[] indices,List<RequestParamsSortDTO> requestParamsSortDTO,SearchRequestBuilder searchRequestBuilder,Map<String,Boolean> validatedData){
			for(RequestParamsSortDTO sortData : requestParamsSortDTO){
				if(validatedData.get(hasdata.HAS_SORTBY.check()))
				searchRequestBuilder.addSort(esFields(indices[0],sortData.getSortBy()), (baseAPIService.checkNull(sortData.getSortOrder()) && sortData.getSortOrder().equalsIgnoreCase("DESC")) ? SortOrder.DESC : SortOrder.ASC);
		}
	}

	public void paginate(SearchRequestBuilder searchRequestBuilder,RequestParamsPaginationDTO requestParamsPaginationDTO,Map<String,Boolean> validatedData) {
		searchRequestBuilder.setFrom(validatedData.get(hasdata.HAS_Offset.check()) ? requestParamsPaginationDTO.getOffset().intValue() == 0 ? 0 : requestParamsPaginationDTO.getOffset().intValue() -1  : 0);
		searchRequestBuilder.setSize(validatedData.get(hasdata.HAS_LIMIT.check()) ? requestParamsPaginationDTO.getLimit().intValue() == 0 ? 0 : requestParamsPaginationDTO.getLimit().intValue() : 10);
	}
	
	public Map<String, Object> record(String sourceIndex,String index, String type, String id) {
		GetResponse response = getClient(sourceIndex).prepareGet(index, type, id)
				.execute().actionGet();
		return response.getSource();
	}
	
	public long recordCount(String sourceIndex,String[] indices, String[] types,
			QueryBuilder query, String id) {
		CountRequestBuilder response = getClient(sourceIndex).prepareCount(indices);
		if (query != null) {
			response.setQuery(query);
		}
		if (types != null && types.length >= 0) {
			response.setTypes(types);
		}
		return response.execute().actionGet().getCount();
	}

	public String esFields(String index,String fields){
		Map<String,String> mappingfields = baseConnectionService.getFields().get(index);
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
	
}

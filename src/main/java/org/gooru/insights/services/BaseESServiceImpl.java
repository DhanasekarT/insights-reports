package org.gooru.insights.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.ESConstants;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsPaginationDTO;
import org.gooru.insights.models.RequestParamsSortDTO;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
	
	public List<Map<String,Object>> generateQuery(RequestParamsDTO requestParamsDTO,
			String[] indices,
			Map<String,Boolean> checkPoint,Map<String,Object> messageData,Map<Integer,String> errorData) {
		
		List<Map<String,Object>> dataList = new ArrayList<Map<String,Object>>();
		Map<String,Object> filterData = new HashMap<String,Object>();

		/*
		 * Core Get
		 */
		dataList = coreGet(requestParamsDTO,new String[]{ indices[0]},checkPoint,messageData,errorData,filterData);
		
		if(dataList.isEmpty())
		return new ArrayList<Map<String,Object>>();			
		
		/*
		 * Get all the acceptable filter data from the index
		 */
		filterData = businessLogicService.fetchFilters(indices[0], dataList);
		
		/*
		 * MultiGet loop
		 */
		for(int i=1;i<indices.length;i++){
		
			Set<String> usedFilter = new HashSet<String>();
			Map<String,Object> innerFilterMap = new HashMap<String,Object>();

			List<Map<String,Object>> resultList = multiGet(requestParamsDTO,new String[]{ indices[i]}, new String[]{}, checkPoint,filterData,errorData,dataList.size(),usedFilter);
			
			if(!errorData.isEmpty()){
				return dataList;			
			}
			
			innerFilterMap = businessLogicService.fetchFilters(indices[i], resultList);
			filterData.putAll(innerFilterMap);
			
			dataList = businessLogicService.leftJoin(dataList, resultList,usedFilter);
			groupConcat(dataList, resultList, usedFilter);
		}
		
		if(checkPoint.get(hasdata.HAS_GROUPBY.check()) && checkPoint.get(hasdata.HAS_GRANULARITY.check())){
		try {
			String groupBy[] = requestParamsDTO.getGroupBy().split(",");
			dataList = businessLogicService.formatAggregateKeyValueJson(dataList, groupBy[groupBy.length-1]);
			dataList = businessLogicService.aggregatePaginate(requestParamsDTO.getPagination(), dataList, checkPoint);		
		} catch (JSONException e) {
			e.printStackTrace();
		}
		}
		
	return dataList;
	}
	
	private void groupConcat(List<Map<String,Object>> dataList,List<Map<String,Object>> resultList,Set<String> usedFilter){
		Set<String> groupConcatFields = new HashSet<String>(); 
		for(String data : usedFilter){
		if(baseConnectionService.getArrayHandler().contains(data)){
			groupConcatFields.add(data);
		}
		}
		if(!groupConcatFields.isEmpty()){
			List<Map<String,Object>> tempList = new ArrayList<Map<String,Object>>();
			for(Map<String,Object> dataEntry : dataList){
				Map<String,Object> tempMap = new HashMap<String, Object>();
				for(String groupConcatField : groupConcatFields){
					String groupConcatFieldName = baseConnectionService.getFieldArrayHandler().get(groupConcatField);
					tempMap.putAll(dataEntry);
				try{
				Set<Object> courseIds = (Set<Object>) dataEntry.get(groupConcatField);
				StringBuffer stringBuffer = new StringBuffer();
				for(Object courseId : courseIds){
					for(Map<String,Object> resultMap : resultList){
						if(resultMap.containsKey(groupConcatField) && resultMap.containsKey(groupConcatFieldName) && resultMap.get(groupConcatField).toString().equalsIgnoreCase(courseId.toString())){
							if(stringBuffer.length() > 0){
								stringBuffer.append(PIPE);
							}
							stringBuffer.append(resultMap.get(groupConcatFieldName));
						}
					}
				}
				tempMap.put(groupConcatFieldName, stringBuffer.toString());
				}catch(Exception e){
				}
				}
				tempList.add(tempMap);
			}
			dataList = tempList;
		}
	}
	
	public List<Map<String,Object>> getItem(RequestParamsDTO requestParamsDTO,
			String[] indices,
			Map<String,Boolean> validatedData,Map<String,Object> dataMap,Map<Integer,String> errorRecord) {
		
		List<Map<String,Object>> dataList = new ArrayList<Map<String,Object>>();
		Map<String,Object> filterMap = new HashMap<String,Object>();
		dataList = coreGet(requestParamsDTO,new String[]{ indices[0]},validatedData,dataMap,errorRecord,filterMap);
		
		if(dataList.isEmpty())
		return new ArrayList<Map<String,Object>>();			
		
		filterMap = businessLogicService.fetchFilters(indices[0], dataList);

		for(int i=1;i<indices.length;i++){
			Set<String> usedFilter = new HashSet<String>();
			Map<String,Object> innerFilterMap = new HashMap<String,Object>();
			List<Map<String,Object>> resultList = multiGet(requestParamsDTO,new String[]{ indices[i]}, new String[]{}, validatedData,filterMap,errorRecord,dataList.size(),usedFilter);
			innerFilterMap = businessLogicService.fetchFilters(indices[i], resultList);
			filterMap.putAll(innerFilterMap);
			dataList = businessLogicService.leftJoin(dataList, resultList,usedFilter);
		}
		
	return dataList;
	}
	
	public List<Map<String,Object>> multiGet(RequestParamsDTO requestParamsDTO,
			String[] indices, String[] types,
			Map<String,Boolean> validatedData,Map<String,Object> filterMap,Map<Integer,String> errorRecord,Integer limit,Set<String> usedFilter) {
		
		SearchRequestBuilder searchRequestBuilder = getClient(requestParamsDTO.getSourceIndex()).prepareSearch(
				indices).setSearchType(SearchType.DFS_QUERY_THEN_FETCH);

		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		String result ="[{}]";
		String dataKey=esSources.SOURCE.esSource();

		if (validatedData.get(hasdata.HAS_FEILDS.check())) {
			Set<String> filterFields = new HashSet<String>();
			if(!requestParamsDTO.getFields().equalsIgnoreCase(WILD_CARD)){
			String fields = esFields(indices[0],requestParamsDTO.getFields());
		
			if(fields.contains("code_id") || fields.contains("label")){
				fields = fields+COMMA+"depth";	
				}

			filterFields = baseAPIService.convertStringtoSet(fields);
			}else{
					for(String field : baseConnectionService.getDefaultFields().get(indices[0]).split(COMMA)){
						searchRequestBuilder.addField(field);	
					}
		}
			for (String field : filterFields) {
				searchRequestBuilder.addField(field);
			}
			dataKey=esSources.FIELDS.esSource();
			}
		
		if(!filterMap.isEmpty()){
			BoolFilterBuilder filterData = businessLogicService.customFilter(indices[0],filterMap,usedFilter);
			if(filterData.hasClauses())
			searchRequestBuilder.setPostFilter(filterData);
		}
		searchRequestBuilder.setSize(limit);
		try{
		result =  searchRequestBuilder.execute().actionGet().toString();
		}catch(Exception e){
			e.printStackTrace();
			errorRecord.put(500, "please contact the developer team for knowing about the error details.");
		}
		
		
		resultList = businessLogicService.getRecords(indices,result, errorRecord, dataKey,null);
		
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
		/*System.out.println("query"+searchRequestBuilder);*/
	}
	
	/*
	 * @requestParamsDTO is the de-serialized API request
	 * @type is the list of specified index type
	 * @validatedData is the pre-validated data
	 * @dataMap is for messaging map to user
	 * @errorRecord is for returning the error message from service
	 * @filterMap is the filter for core API's and sub-sequent API's
	 */
	public List<Map<String,Object>> coreGet(RequestParamsDTO requestParamsDTO,
			String[] indices,
			Map<String,Boolean> validatedData,Map<String,Object> dataMap,Map<Integer,String> errorRecord,Map<String,Object> filterMap) {
		
		String result = EMPTY_JSON_ARRAY;
		boolean hasAggregate = false;
		Map<String,String> metricsName = new HashMap<String,String>();
		String dataKey = esSources.SOURCE.esSource();

		SearchRequestBuilder searchRequestBuilder = getClient(requestParamsDTO.getSourceIndex()).prepareSearch(
					indices).setSearchType(SearchType.DFS_QUERY_THEN_FETCH);

		searchRequestBuilder.setSize(1);

		if (validatedData.get(hasdata.HAS_GRANULARITY.check())) {
			
			searchRequestBuilder.setNoFields();
			businessLogicService.performGranularityAggregation(indices[0],requestParamsDTO, searchRequestBuilder,metricsName,validatedData);
			hasAggregate = true;

		}else if (validatedData.get(hasdata.HAS_GROUPBY.check())) {

			searchRequestBuilder.setNoFields();
			businessLogicService.performAggregation(indices[0],requestParamsDTO, searchRequestBuilder,metricsName);
			hasAggregate = true;
		
		}else  {
			Set<String> filterFields = new HashSet<String>();
			if(validatedData.get(hasdata.HAS_FEILDS.check())){
				if(!requestParamsDTO.getFields().equalsIgnoreCase(WILD_CARD)){
			dataKey=esSources.FIELDS.esSource();
			String fields = esFields(indices[0],requestParamsDTO.getFields());
			filterFields = baseAPIService.convertStringtoSet(fields);
			}else{
					for(String field : baseConnectionService.getDefaultFields().get(indices[0]).split(COMMA)){
						searchRequestBuilder.addField(field);	
					}
			}
			for (String field : filterFields) {
				searchRequestBuilder.addField(field);
			}
		}
		}
		
		if(!hasAggregate){

			if(validatedData.get(hasdata.HAS_FILTER.check()))
			searchRequestBuilder.setPostFilter(businessLogicService.includeFilter(indices[0],requestParamsDTO.getFilter()).cache(true));

			if(validatedData.get(hasdata.HAS_SORTBY.check()))
				includeSort(indices,requestParamsDTO.getPagination().getOrder(),searchRequestBuilder,validatedData);

			if(validatedData.get(hasdata.HAS_PAGINATION.check()))
				performPagination(searchRequestBuilder, requestParamsDTO.getPagination(), validatedData);
		
		}
		
		try{
		//System.out.println("query "+ searchRequestBuilder);		
		result =  searchRequestBuilder.execute().actionGet().toString();
		}catch(Exception e){
			e.printStackTrace();
			errorRecord.put(500, "please contact the developer team for knowing about the error details.");
		}
		
		if(hasAggregate){
			
			int limit = 10;
			if(validatedData.get(hasdata.HAS_PAGINATION.check())){
			
				if(validatedData.get(hasdata.HAS_LIMIT.check())){
				limit = requestParamsDTO.getPagination().getLimit();
				limit = requestParamsDTO.getPagination().getOffset() + requestParamsDTO.getPagination().getLimit();
				}
			}
				
			String groupBy[] = requestParamsDTO.getGroupBy().split(COMMA);
			List<Map<String,Object>> queryResult = businessLogicService.customizeJSON(groupBy, result, metricsName, validatedData.get(hasdata.HAS_FILTER.check()),dataMap,limit);
			
			if(!validatedData.get(hasdata.HAS_GRANULARITY.check())){
				queryResult = businessLogicService.customPagination(requestParamsDTO.getPagination(), queryResult, validatedData);
			}else{
				queryResult = businessLogicService.customSort(requestParamsDTO.getPagination(), queryResult, validatedData);
			}
			
			return queryResult;
		}else{
			return businessLogicService.getRecords(indices,result,errorRecord,dataKey,dataMap);
		}
	}
	
	public Client getClient(String indexSource) {
		if(indexSource != null && indexSource.equalsIgnoreCase(DEV)){
			return baseConnectionService.getDevClient();
		}else if(indexSource != null && indexSource.equalsIgnoreCase(PROD)){
			return baseConnectionService.getProdClient();
		}else{			
			return baseConnectionService.getProdClient();
		}
	}

	public void includeSort(String[] indices,List<RequestParamsSortDTO> requestParamsSortDTO,SearchRequestBuilder searchRequestBuilder,Map<String,Boolean> validatedData){
			for(RequestParamsSortDTO sortData : requestParamsSortDTO){
				if(validatedData.get(hasdata.HAS_SORTBY.check()))
				searchRequestBuilder.addSort(esFields(indices[0],sortData.getSortBy()), (baseAPIService.checkNull(sortData.getSortOrder()) && sortData.getSortOrder().equalsIgnoreCase("DESC")) ? SortOrder.DESC : SortOrder.ASC);
		}
	}

	public void performPagination(SearchRequestBuilder searchRequestBuilder,RequestParamsPaginationDTO requestParamsPaginationDTO,Map<String,Boolean> validatedData) {
		searchRequestBuilder.setFrom(validatedData.get(hasdata.HAS_Offset.check()) ? requestParamsPaginationDTO.getOffset().intValue() == 0 ? 0 : requestParamsPaginationDTO.getOffset().intValue() -1  : 0);
		searchRequestBuilder.setSize(validatedData.get(hasdata.HAS_LIMIT.check()) ? requestParamsPaginationDTO.getLimit().intValue() == 0 ? 0 : requestParamsPaginationDTO.getLimit().intValue() : 10);
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

	public void singeColumnUpdate(String sourceIndex, String indexName, String typeName, String id, String column, Object value) {
		try {
			getClient(sourceIndex).prepareUpdate(indexName,typeName,id)
			.addScriptParam("fieldValue", value)
			.setScript("ctx._source."+column+"=fieldValue")
			.execute().actionGet();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public String esFields(String index,String fields){
		Map<String,String> mappingfields = baseConnectionService.getFields().get(index);
		StringBuffer esFields = new StringBuffer();
		for(String field : fields.split(COMMA)){
			if(esFields.length() > 0){
				esFields.append(COMMA);
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

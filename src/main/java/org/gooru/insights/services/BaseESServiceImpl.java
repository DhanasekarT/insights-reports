package org.gooru.insights.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.APIConstants.Hasdatas;
import org.gooru.insights.constants.ESConstants;
import org.gooru.insights.constants.ESConstants.EsSources;
import org.gooru.insights.constants.ErrorConstants;
import org.gooru.insights.exception.handlers.ReportGenerationException;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsPaginationDTO;
import org.gooru.insights.models.RequestParamsSortDTO;
import org.gooru.insights.models.ResponseParamDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class BaseESServiceImpl implements BaseESService {

	Logger logger = LoggerFactory.getLogger(BaseESServiceImpl.class);
	
	@Autowired
	BaseConnectionService baseConnectionService;

	@Autowired
	BaseAPIService baseAPIService;
	
	@Autowired
	BusinessLogicService businessLogicService;

	RequestParamsSortDTO requestParamsSortDTO;

	RequestParamsFilterDetailDTO requestParamsFiltersDetailDTO;
	
	//valid
	public ResponseParamDTO<Map<String,Object>> generateQuery(RequestParamsDTO requestParamsDTO,
			String[] indices,
			Map<String,Boolean> checkPoint) throws Exception{
		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		

		/**
		 * Do Core Get
		 */
		Map<String,Object> filters = new HashMap<String,Object>();
		List<Map<String,Object>> dataList = coreGet(requestParamsDTO,responseParamDTO,indices[0],checkPoint,filters);
		
		if(dataList.isEmpty())
		return responseParamDTO;			
		
		/**
		 * Get all the acceptable filter data from the index
		 */
		filters = businessLogicService.fetchFilters(indices[0], dataList);
		
		/**
		 * Do MultiGet loop
		 */
		for(int i=1;i<indices.length;i++){
		
			Set<String> usedFilter = new HashSet<String>();
			Map<String,Object> innerFilterMap = new HashMap<String,Object>();

			List<Map<String,Object>> resultList = multiGet(requestParamsDTO,indices[i], new String[]{}, checkPoint,filters,dataList.size(),usedFilter);
			
			innerFilterMap = businessLogicService.fetchFilters(indices[i], resultList);
			filters.putAll(innerFilterMap);
			
			dataList = businessLogicService.leftJoin(dataList, resultList,usedFilter);
			groupConcat(dataList, resultList, usedFilter);
		}
		
		if(checkPoint.get(Hasdatas.HAS_GROUPBY.check()) && checkPoint.get(Hasdatas.HAS_GRANULARITY.check())){
			String groupBy[] = requestParamsDTO.getGroupBy().split(",");
			dataList = businessLogicService.formatAggregateKeyValueJson(dataList, groupBy[groupBy.length-1]);
			dataList = businessLogicService.aggregatePaginate(requestParamsDTO.getPagination(), dataList, checkPoint);		
		}
		responseParamDTO.setContent(dataList);
		return responseParamDTO;
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
								stringBuffer.append(APIConstants.PIPE);
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
	
	public ResponseParamDTO<Map<String,Object>> getItem(RequestParamsDTO requestParamsDTO,
			String[] indices,
			Map<String,Boolean> validatedData,Map<String,Object> dataMap,Map<Integer,String> errorRecord) throws Exception {
		
		List<Map<String,Object>> dataList = new ArrayList<Map<String,Object>>();
		Map<String,Object> filterMap = new HashMap<String,Object>();
		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		dataList = coreGet(requestParamsDTO,responseParamDTO,indices[0],validatedData,filterMap);
		
		if(dataList.isEmpty())
		return responseParamDTO;			
		
		filterMap = businessLogicService.fetchFilters(indices[0], dataList);

		for(int i=1;i<indices.length;i++){
			Set<String> usedFilter = new HashSet<String>();
			Map<String,Object> innerFilterMap = new HashMap<String,Object>();
			List<Map<String,Object>> resultList = multiGet(requestParamsDTO,indices[i], new String[]{}, validatedData,filterMap,dataList.size(),usedFilter);
			innerFilterMap = businessLogicService.fetchFilters(indices[i], resultList);
			filterMap.putAll(innerFilterMap);
			dataList = businessLogicService.leftJoin(dataList, resultList,usedFilter);
		}
		responseParamDTO.setContent(dataList);
	return responseParamDTO;
	}
	
	/**
	 * This will do a multi Get operation to perform integration of data.
	 * @param RequestParamsDTO This is request object
	 * @param Indices This is the indices on where the request to be processed
	 * @param Types currently type based support is not added
	 * @param ValidatedData validated map for the given request
	 * @param filterMap includes the filter data
	 * @param errorRecord 
	 * @param limit
	 * @param usedFilter
	 * @return
	 */
	private List<Map<String,Object>> multiGet(RequestParamsDTO requestParamsDTO,
			String indices, String[] types,
			Map<String,Boolean> validatedData,Map<String,Object> filterMap,int limit,Set<String> usedFilter) throws Exception{
		
		SearchRequestBuilder searchRequestBuilder = getClient(requestParamsDTO.getSourceIndex()).prepareSearch(
				indices).setSearchType(SearchType.DFS_QUERY_THEN_FETCH);

		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		String result ="[{}]";
		String dataKey=ESConstants.EsSources.SOURCE.esSource();

		if (validatedData.get(Hasdatas.HAS_FEILDS.check())) {
			Set<String> filterFields = new HashSet<String>();
			if(!requestParamsDTO.getFields().equalsIgnoreCase(APIConstants.WILD_CARD)){
			String fields = esFields(indices,requestParamsDTO.getFields());
		
			if(fields.contains("code_id") || fields.contains("label")){
				fields = fields+APIConstants.COMMA+"depth";	
				}

			filterFields = baseAPIService.convertStringtoSet(fields);
			}else{
					for(String field : baseConnectionService.getDefaultFields().get(indices).split(APIConstants.COMMA)){
						searchRequestBuilder.addField(field);	
					}
		}
			for (String field : filterFields) {
				searchRequestBuilder.addField(field);
			}
			dataKey=EsSources.FIELDS.esSource();
			}
		
		if(!filterMap.isEmpty()){
			BoolFilterBuilder filterData = businessLogicService.customFilter(indices,filterMap,usedFilter);
			if(filterData.hasClauses())
			searchRequestBuilder.setPostFilter(filterData);
		}
		searchRequestBuilder.setSize(limit);
		try{
		result =  searchRequestBuilder.execute().actionGet().toString();
		}catch(Exception e){
			throw new ReportGenerationException(ErrorConstants.QUERY_ERROR+searchRequestBuilder.toString());
		}
		
		resultList = businessLogicService.getRecords(indices,null,result, dataKey);
		
		return resultList;
	}
	
	/**
	 * This function will perform aggregation and relevent process 
	 * @param requestParamsDTO is the de-serialized API request
	 * @param indices is the list of specified index type
	 * @param validatedData is the pre-validated data
	 * @param filters is the filter for core API's and sub-sequent API's
	 * @return List of aggregated data
	 * @throws Exception 
	 */
	//valid
	public List<Map<String,Object>> coreGet(RequestParamsDTO requestParamsDTO,ResponseParamDTO<Map<String,Object>> responseParamDTO,
			String indices,
			Map<String,Boolean> validatedData,Map<String,Object> filters) throws Exception {
		
		String result = APIConstants.EMPTY_JSON_ARRAY;
		boolean hasAggregate = false;
		Map<String,String> metricsName = new HashMap<String,String>();
		String dataKey = EsSources.SOURCE.esSource();

		SearchRequestBuilder searchRequestBuilder = getClient(requestParamsDTO.getSourceIndex()).prepareSearch(
					indices).setSearchType(SearchType.DFS_QUERY_THEN_FETCH);

		searchRequestBuilder.setSize(1);

		if (validatedData.get(Hasdatas.HAS_GRANULARITY.check())) {
			
			searchRequestBuilder.setNoFields();
			businessLogicService.buildGranularityBuckets(indices,requestParamsDTO, searchRequestBuilder,metricsName,validatedData);
			hasAggregate = true;

		}else if (validatedData.get(Hasdatas.HAS_GROUPBY.check())) {

			searchRequestBuilder.setNoFields();
			businessLogicService.buildBuckets(indices,requestParamsDTO, searchRequestBuilder,metricsName);
			hasAggregate = true;
		
		}else  {
			Set<String> filterFields = new HashSet<String>();
			if(validatedData.get(Hasdatas.HAS_FEILDS.check())){
				if(!requestParamsDTO.getFields().equalsIgnoreCase(APIConstants.WILD_CARD)){
			dataKey=EsSources.FIELDS.esSource();
			String fields = esFields(indices,requestParamsDTO.getFields());
			filterFields = baseAPIService.convertStringtoSet(fields);
			}else{
					for(String field : baseConnectionService.getDefaultFields().get(indices).split(APIConstants.COMMA)){
						searchRequestBuilder.addField(field);	
					}
			}
			for (String field : filterFields) {
				searchRequestBuilder.addField(field);
			}
		}
		}
		
		if(!hasAggregate){

			if(validatedData.get(Hasdatas.HAS_FILTER.check()))
			searchRequestBuilder.setPostFilter(businessLogicService.includeBucketFilter(indices,requestParamsDTO.getFilter()).cache(true));

			if(validatedData.get(Hasdatas.HAS_SORTBY.check()))
				includeSort(indices,requestParamsDTO.getPagination().getOrder(),searchRequestBuilder,validatedData);

			if(validatedData.get(Hasdatas.HAS_PAGINATION.check()))
				performPagination(searchRequestBuilder, requestParamsDTO.getPagination(), validatedData);
		
		}
		
		try{
			logger.info(APIConstants.QUERY+searchRequestBuilder);
		result =  searchRequestBuilder.execute().actionGet().toString();
		}catch(Exception e){
			throw new ReportGenerationException(ErrorConstants.INVALID_ERROR.replace(ErrorConstants.REPLACER, APIConstants.QUERY));
		}
		
		if(hasAggregate){
			
			int limit = 10;
			if(validatedData.get(Hasdatas.HAS_PAGINATION.check())){
			
				if(validatedData.get(Hasdatas.HAS_LIMIT.check())){
				limit = requestParamsDTO.getPagination().getLimit();
				limit = requestParamsDTO.getPagination().getOffset() + requestParamsDTO.getPagination().getLimit();
				}
			}
				
			String groupBy[] = requestParamsDTO.getGroupBy().split(APIConstants.COMMA);
			List<Map<String,Object>> queryResult = businessLogicService.customizeJSON(groupBy, result, metricsName, validatedData.get(Hasdatas.HAS_FILTER.check()),responseParamDTO,limit);
			
			if(!validatedData.get(Hasdatas.HAS_GRANULARITY.check())){
				queryResult = businessLogicService.customPagination(requestParamsDTO.getPagination(), queryResult, validatedData);
			}else{
				queryResult = businessLogicService.customSort(requestParamsDTO.getPagination(), queryResult, validatedData);
			}
			
			return queryResult;
		}else{
			return businessLogicService.getRecords(indices,responseParamDTO,result,dataKey);
		}
	}
	
	public Client getClient(String indexSource) {
		if(indexSource != null && indexSource.equalsIgnoreCase(APIConstants.DEV)){
			return baseConnectionService.getDevClient();
		}else if(indexSource != null && indexSource.equalsIgnoreCase(APIConstants.PROD)){
			return baseConnectionService.getProdClient();
		}else{			
			return baseConnectionService.getProdClient();
		}
	}

	private void includeSort(String indices,List<RequestParamsSortDTO> requestParamsSortDTO,SearchRequestBuilder searchRequestBuilder,Map<String,Boolean> validatedData){
			for(RequestParamsSortDTO sortData : requestParamsSortDTO){
				if(validatedData.get(Hasdatas.HAS_SORTBY.check()))
				searchRequestBuilder.addSort(esFields(indices,sortData.getSortBy()), (baseAPIService.checkNull(sortData.getSortOrder()) && sortData.getSortOrder().equalsIgnoreCase("DESC")) ? SortOrder.DESC : SortOrder.ASC);
		}
	}

	private void performPagination(SearchRequestBuilder searchRequestBuilder,RequestParamsPaginationDTO requestParamsPaginationDTO,Map<String,Boolean> validatedData) {
		searchRequestBuilder.setFrom(validatedData.get(Hasdatas.HAS_Offset.check()) ? requestParamsPaginationDTO.getOffset().intValue() == 0 ? 0 : requestParamsPaginationDTO.getOffset().intValue() -1  : 0);
		searchRequestBuilder.setSize(validatedData.get(Hasdatas.HAS_LIMIT.check()) ? requestParamsPaginationDTO.getLimit().intValue() == 0 ? 0 : requestParamsPaginationDTO.getLimit().intValue() : 10);
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

	/**
	 * This will convert the apiField name to Els field name
	 * @param index The index fields are belongs to
	 * @param fields Request fields from user
	 * @return converted comma separated Els fields
	 */
	public String esFields(String index,String fields){
		Map<String,String> mappingfields = baseConnectionService.getFields().get(index);
		StringBuffer esFields = new StringBuffer();
		for(String field : fields.split(APIConstants.COMMA)){
			if(esFields.length() > 0){
				esFields.append(APIConstants.COMMA);
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

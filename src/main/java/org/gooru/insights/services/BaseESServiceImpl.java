package org.gooru.insights.services;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.NotFilterBuilder;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Interval;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order;
import org.elasticsearch.search.aggregations.bucket.range.RangeBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.gooru.insights.builders.utils.InsightsLogger;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.APIConstants.Hasdatas;
import org.gooru.insights.constants.ESConstants;
import org.gooru.insights.constants.ESConstants.EsSources;
import org.gooru.insights.constants.ErrorConstants;
import org.gooru.insights.exception.handlers.ReportGenerationException;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsFilterFieldsDTO;
import org.gooru.insights.models.RequestParamsPaginationDTO;
import org.gooru.insights.models.RequestParamsRangeDTO;
import org.gooru.insights.models.RequestParamsSortDTO;
import org.gooru.insights.models.ResponseParamDTO;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;

@Service
public class BaseESServiceImpl implements BaseESService {

	@Autowired
	private BaseConnectionService baseConnectionService;

	@Autowired
	private BaseAPIService baseAPIService;
	
	@Autowired
	private ESDataProcessor businessLogicService;
	
	public ResponseParamDTO<Map<String,Object>> generateQuery(String traceId,RequestParamsDTO requestParamsDTO,
			String[] indices,
			Map<String,Boolean> checkPoint) throws Exception{

		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		/**
		 * Do Core Get
		 */
		Map<String,Object> filters = new HashMap<String,Object>();
		List<Map<String,Object>> dataList = new ArrayList<Map<String,Object>>();
		
		if(requestParamsDTO.getGroupByDataSource() == null || requestParamsDTO.getGroupByDataSource().equalsIgnoreCase(indices[0])){
		dataList = coreGet(traceId,requestParamsDTO,responseParamDTO,indices[0],checkPoint,filters, new HashSet<String>());
		}else {
			dataList = multiGet(traceId,requestParamsDTO,indices[0], new String[]{}, checkPoint,filters,0,new HashSet<String>());
		}
			
		if(dataList.isEmpty()){
		return responseParamDTO;
		}
		/**
		 * Get all the acceptable filter data from the index
		 */
		filters = getBusinessLogicService().fetchFilters(indices[0], dataList);
		checkPoint.put(Hasdatas.HAS_MULTIGET.check(), true);
		/**
		 * Do MultiGet loop
		 */
		for(int i=1;i<indices.length;i++){
		boolean rightJoin = false;
			Set<String> usedFilter = new HashSet<String>();
			Map<String,Object> innerFilterMap = new HashMap<String,Object>();
			List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();

			if(indices[i].equalsIgnoreCase(requestParamsDTO.getGroupByDataSource())){
				resultList = coreGet(traceId,requestParamsDTO,responseParamDTO,indices[i],checkPoint,filters,usedFilter);
				rightJoin = true;
			}else{
			resultList = multiGet(traceId,requestParamsDTO,indices[i], new String[]{}, checkPoint,filters,dataList.size(),usedFilter);
			}
			innerFilterMap = getBusinessLogicService().fetchFilters(indices[i], resultList);
			filters.putAll(innerFilterMap);
			if(rightJoin){
				dataList = getBaseAPIService().leftJoin(resultList,dataList,usedFilter);
			}else{
			dataList = getBaseAPIService().leftJoin(dataList, resultList,usedFilter);
			}
			groupConcat(dataList, resultList, usedFilter);
		}
		
		if(checkPoint.get(Hasdatas.HAS_GROUPBY.check()) && (checkPoint.get(Hasdatas.HAS_GRANULARITY.check()) || checkPoint.get(Hasdatas.HAS_RANGE.check()))){
			String groupBy[] = requestParamsDTO.getGroupBy().split(APIConstants.COMMA);
			dataList = getBusinessLogicService().formatAggregateKeyValueJson(dataList, groupBy[groupBy.length-1]);
			dataList = getBusinessLogicService().aggregatePaginate(requestParamsDTO.getPagination(), dataList, checkPoint);		
		}
		responseParamDTO.setContent(dataList);
		return responseParamDTO;
	}
	
	private void groupConcat(List<Map<String,Object>> dataList,List<Map<String,Object>> resultList,Set<String> usedFilter){
		Set<String> groupConcatFields = new HashSet<String>(); 
		for(String data : usedFilter){
		if(getBaseConnectionService().getArrayHandler().contains(data)){
			groupConcatFields.add(data);
		}
		}
		if(!groupConcatFields.isEmpty()){
			List<Map<String,Object>> tempList = new ArrayList<Map<String,Object>>();
			for(Map<String,Object> dataEntry : dataList){
				Map<String,Object> tempMap = new HashMap<String, Object>();
				for(String groupConcatField : groupConcatFields){
					String groupConcatFieldName = getBaseConnectionService().getFieldArrayHandler().get(groupConcatField);
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
	
	public ResponseParamDTO<Map<String,Object>> getItem(String traceId,RequestParamsDTO requestParamsDTO,
			String[] indices,
			Map<String,Boolean> validatedData,Map<String,Object> dataMap,Map<Integer,String> errorRecord) throws Exception {
		
		List<Map<String,Object>> dataList = new ArrayList<Map<String,Object>>();
		Map<String,Object> filterMap = new HashMap<String,Object>();
		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		dataList = coreGet(traceId,requestParamsDTO,responseParamDTO,indices[0],validatedData,filterMap, new HashSet<String>());
		
		if(dataList.isEmpty())
		return responseParamDTO;			
		
		filterMap = getBusinessLogicService().fetchFilters(indices[0], dataList);

		for(int i=1;i<indices.length;i++){
			Set<String> usedFilter = new HashSet<String>();
			Map<String,Object> innerFilterMap = new HashMap<String,Object>();
			List<Map<String,Object>> resultList = multiGet(traceId,requestParamsDTO,indices[i], new String[]{}, validatedData,filterMap,dataList.size(),usedFilter);
			innerFilterMap = getBusinessLogicService().fetchFilters(indices[i], resultList);
			filterMap.putAll(innerFilterMap);
			dataList = getBaseAPIService().leftJoin(dataList, resultList,usedFilter);
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
	private List<Map<String,Object>> multiGet(String traceId,RequestParamsDTO requestParamsDTO,
			String indices, String[] types,
			Map<String,Boolean> validatedData,Map<String,Object> filterMap,int limit,Set<String> usedFilter) throws Exception{
		
		SearchRequestBuilder searchRequestBuilder = getClient(requestParamsDTO.getSourceIndex()).prepareSearch(
				indices).setSearchType(SearchType.DFS_QUERY_THEN_FETCH);

		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
		String result =APIConstants.EMPTY_JSON_ARRAY;
		String dataKey=ESConstants.EsSources.SOURCE.esSource();

		if (validatedData.get(Hasdatas.HAS_FEILDS.check())) {
			Set<String> filterFields = new HashSet<String>();
			if(!requestParamsDTO.getFields().equalsIgnoreCase(APIConstants.WILD_CARD)){
			String fields = getBusinessLogicService().esFields(indices,requestParamsDTO.getFields());
		
			/**
			 * Need to change taxonomy logic
			 */
			if(fields.contains(APIConstants._CODEID) || fields.contains(APIConstants.LABEL)){
				fields =BaseAPIServiceImpl.buildString(new Object[]{fields,APIConstants.COMMA,APIConstants.DEPTH});	
				}

			filterFields = getBaseAPIService().convertStringtoSet(fields);
			}else{
					for(String field : getBaseConnectionService().getDefaultFields().get(indices).split(APIConstants.COMMA)){
						searchRequestBuilder.addField(field);	
					}
		}
			for (String field : filterFields) {
				searchRequestBuilder.addField(field);
			}
			dataKey=EsSources.FIELDS.esSource();
			}
		
		if(validatedData.get(Hasdatas.HAS_DATASOURCE_FILTER.check())){
			boolFilter = includeBucketFilter(indices,requestParamsDTO.getFilter(),validatedData);
		}
			boolFilter = customFilter(indices,filterMap,usedFilter,boolFilter);
			if(boolFilter.hasClauses())
			searchRequestBuilder.setPostFilter(boolFilter);
			if(validatedData.get(Hasdatas.HAS_MULTIGET.check())){
				searchRequestBuilder.setSize(limit);
			}else{
				searchRequestBuilder.setSize(Integer.valueOf(prepareCount(requestParamsDTO,indices,validatedData).toString()));
			}
		try{
		result =  searchRequestBuilder.execute().actionGet().toString();
		}catch(Exception e){
			throw new ReportGenerationException(APIConstants.QUERY,e);
		}
		
		resultList = getBusinessLogicService().getRecords(traceId,indices,null,result, dataKey);
		
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
	public List<Map<String,Object>> coreGet(String traceId,RequestParamsDTO requestParamsDTO,ResponseParamDTO<Map<String,Object>> responseParamDTO,
			String indices,
			Map<String,Boolean> validatedData,Map<String,Object> filterData,Set<String> userFilter) throws Exception {
		
		String result = APIConstants.EMPTY_JSON_ARRAY;
		boolean hasAggregate = false;
		Map<String,String> metricsName = new HashMap<String,String>();
		String dataKey = EsSources.SOURCE.esSource();

		SearchRequestBuilder searchRequestBuilder = getClient(requestParamsDTO.getSourceIndex()).prepareSearch(
					indices).setSearchType(SearchType.DFS_QUERY_THEN_FETCH);

		searchRequestBuilder.setSize(1);

		if (validatedData.get(Hasdatas.HAS_GRANULARITY.check())) {
			
			searchRequestBuilder.setNoFields();
			buildGranularityBuckets(indices,requestParamsDTO, searchRequestBuilder,metricsName,validatedData, filterData, userFilter);
			hasAggregate = true;

		} else if(validatedData.get(Hasdatas.HAS_RANGE.check()) && validatedData.get(Hasdatas.HAS_GROUPBY.check())) {
			searchRequestBuilder.setNoFields();
			buildRangeBuckets(indices,requestParamsDTO,searchRequestBuilder,metricsName,validatedData, filterData, userFilter);
			hasAggregate = true;
		} else if (validatedData.get(Hasdatas.HAS_GROUPBY.check())) {

			searchRequestBuilder.setNoFields();
			buildBuckets(traceId,indices,requestParamsDTO, searchRequestBuilder,metricsName,validatedData,filterData, userFilter);
			hasAggregate = true;
		
		}	else if (validatedData.get(Hasdatas.HAS_AGGREGATE.check())) {
			searchRequestBuilder.setNoFields();
			buildAggregation(traceId,indices,requestParamsDTO, searchRequestBuilder,metricsName,validatedData,filterData, userFilter);
			hasAggregate = true;
		
		}  else {
			Set<String> filterFields = new HashSet<String>();
			if (validatedData.get(Hasdatas.HAS_FEILDS.check())) {
				if (!requestParamsDTO.getFields().equalsIgnoreCase(APIConstants.WILD_CARD)) {
					dataKey = EsSources.FIELDS.esSource();
					String fields = getBusinessLogicService().esFields(indices, requestParamsDTO.getFields());
					filterFields = getBaseAPIService().convertStringtoSet(fields);
				} else {
					for (String field : getBaseConnectionService().getDefaultFields().get(indices).split(APIConstants.COMMA)) {
						filterFields.add(field);
					}
				}
				for (String field : filterFields) {
					searchRequestBuilder.addField(field);
				}
			}
		}
		
		if(!hasAggregate){

			if(validatedData.get(Hasdatas.HAS_FILTER.check()))
			searchRequestBuilder.setPostFilter(includeBucketFilter(indices,requestParamsDTO.getFilter(),validatedData).cache(true));

			if(validatedData.get(Hasdatas.HAS_SORTBY.check()))
				includeSort(indices,requestParamsDTO.getPagination().getOrder(),searchRequestBuilder,validatedData);

			if(validatedData.get(Hasdatas.HAS_PAGINATION.check()))
				performPagination(searchRequestBuilder, requestParamsDTO.getPagination(), validatedData);
		}
		try{
			InsightsLogger.info(traceId, BaseAPIServiceImpl.buildString(new Object[]{APIConstants.QUERY, searchRequestBuilder}));
		result =  searchRequestBuilder.execute().actionGet().toString();
		}catch(Exception e){
			throw new ReportGenerationException(APIConstants.QUERY, e);
		}
		if(hasAggregate){
			int limit = 10;
			if(validatedData.get(Hasdatas.HAS_PAGINATION.check())){
			
				if(validatedData.get(Hasdatas.HAS_LIMIT.check())){
				limit = requestParamsDTO.getPagination().getLimit();
				limit = requestParamsDTO.getPagination().getOffset() + requestParamsDTO.getPagination().getLimit();
				}
			}
			List<Map<String,Object>> queryResult = getBusinessLogicService().customizeJSON(traceId,requestParamsDTO.getGroupBy(), result, metricsName, validatedData,responseParamDTO,limit);
			
			if(!validatedData.get(Hasdatas.HAS_GRANULARITY.check())){
				queryResult = getBusinessLogicService().customPagination(requestParamsDTO.getPagination(), queryResult, validatedData);
			}else{
				queryResult = getBusinessLogicService().customSort(requestParamsDTO.getPagination(), queryResult, validatedData);
			}
			
			return queryResult;
		}else{
			return getBusinessLogicService().getRecords(traceId,indices,responseParamDTO,result,dataKey);
		}
	}
	
	public Client getClient(String indexSource) {
		if(indexSource != null && indexSource.equalsIgnoreCase(APIConstants.DEV)){
			return getBaseConnectionService().getDevClient();
		}else if(indexSource != null && indexSource.equalsIgnoreCase(APIConstants.PROD)){
			return getBaseConnectionService().getProdClient();
		}else{			
			return getBaseConnectionService().getProdClient();
		}
	}

	private void includeSort(String indices,List<RequestParamsSortDTO> requestParamsSortDTO,SearchRequestBuilder searchRequestBuilder,Map<String,Boolean> validatedData){
			for(RequestParamsSortDTO sortData : requestParamsSortDTO){
				if(validatedData.get(Hasdatas.HAS_SORTBY.check()))
				searchRequestBuilder.addSort(getBusinessLogicService().esFields(indices,sortData.getSortBy()), (getBaseAPIService().checkNull(sortData.getSortOrder()) && sortData.getSortOrder().equalsIgnoreCase("DESC")) ? SortOrder.DESC : SortOrder.ASC);
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
	 * This function will build the aggregate bucket
	 * @param index This is the source index name
	 * @param RequestParamDTO is the client request
	 * @param searchRequestBuilder is the search query request
	 * @param metricsName is the name of metric functions
	 * @throws unable to build the bucket
	 */
	private void buildBuckets(String traceId,String index, RequestParamsDTO requestParamsDTO, SearchRequestBuilder searchRequestBuilder, Map<String, String> metricsName, Map<String, Boolean> validatedData,Map<String,Object> filterData, Set<String> userFilter) {

		try {
			TermsBuilder termBuilder = null;
			String[] groupBy = requestParamsDTO.getGroupBy().split(APIConstants.COMMA);
			FilterAggregationBuilder filterAggregationBuilder = null;
			
			if (validatedData.get(Hasdatas.HAS_FILTER.check())) {
				filterAggregationBuilder = includeFilterAggregate(index, requestParamsDTO.getFilter(), validatedData, filterData, userFilter);
			}
			
			for (int i = groupBy.length - 1; i >= 0; i--) {
				String fieldName = getBusinessLogicService().esFields(index, groupBy[i]);
				TermsBuilder tempBuilder = null;
				if (termBuilder != null) {
					tempBuilder = AggregationBuilders.terms(groupBy[i]).field(fieldName);
					tempBuilder.subAggregation(termBuilder);
					termBuilder = tempBuilder;
				} else {
					termBuilder = AggregationBuilders.terms(groupBy[i]).field(fieldName);
				}
				if (i == groupBy.length - 1) {
					includeMetricsAggregation(index, requestParamsDTO, filterAggregationBuilder, searchRequestBuilder, termBuilder, null, null, metricsName);
					includeOrder(requestParamsDTO, validatedData, groupBy[i], termBuilder, null, metricsName);
					termBuilder.size(0);
				}
			}
				
			if (filterAggregationBuilder != null) {
				termBuilder.size(0);
				filterAggregationBuilder.subAggregation(termBuilder);
				searchRequestBuilder.addAggregation(filterAggregationBuilder);
			} else {
				termBuilder.size(0);
				searchRequestBuilder.addAggregation(termBuilder);
			}
		} catch (Exception e) {
			InsightsLogger.error(traceId, ErrorConstants.BUCKET_ERROR.replace(ErrorConstants.REPLACER,ErrorConstants.AGGREGATION_BUCKET ),e);
		}
	}

	private void buildRangeBuckets(String index, RequestParamsDTO requestParamsDTO, SearchRequestBuilder searchRequestBuilder,Map<String,String> metricsName,Map<String, Boolean> validatedData,Map<String,Object> filterData, Set<String> userFilter) {
		try {
			
			String fieldName = getBusinessLogicService().esFields(index, requestParamsDTO.getGroupBy());
			FilterAggregationBuilder filterAggregationBuilder = null;
			if (validatedData.get(Hasdatas.HAS_FILTER.check())) {
				filterAggregationBuilder = includeFilterAggregate(index, requestParamsDTO.getFilter(),validatedData, filterData, userFilter);
			}
			RangeBuilder rangeAggregationBuilder = new RangeBuilder(requestParamsDTO.getGroupBy()).field(fieldName);
				for(RequestParamsRangeDTO ranges : requestParamsDTO.getRanges()) {
					if(getBaseAPIService().checkNull(ranges.getFrom()) && getBaseAPIService().checkNull(ranges.getTo())) {
						rangeAggregationBuilder.addRange(ranges.getFrom(), ranges.getTo()+1);
					} else if(getBaseAPIService().checkNull(ranges.getFrom())) {
						rangeAggregationBuilder.addUnboundedFrom(ranges.getFrom());
					} else if(getBaseAPIService().checkNull(ranges.getTo())) {
						rangeAggregationBuilder.addUnboundedTo(ranges.getTo()+1);
					}
				}
				includeMetricsAggregation(index, requestParamsDTO, filterAggregationBuilder, searchRequestBuilder, null, rangeAggregationBuilder, null, metricsName);
				if (filterAggregationBuilder != null) {
					filterAggregationBuilder.subAggregation(rangeAggregationBuilder);
					searchRequestBuilder.addAggregation(filterAggregationBuilder);
				} else {
				    searchRequestBuilder.addAggregation(rangeAggregationBuilder);
				}
		} catch (Exception e) {
			throw new ReportGenerationException(ErrorConstants.BUCKET_ERROR.replace(ErrorConstants.REPLACER,ErrorConstants.RANGE_BUCKET )+e);
		}
	}
	
	/**
	 * This function will build a granularity bucket
	 * @param index This is the source index name
	 * @param RequestParamDTO is the client request
	 * @param searchRequestBuilder is the search query request
	 * @param metricsName is the name of metric functions
	 * @param validatedData is the pre-validated data
	 * @throws unable to build the bucket
	 */
	private void buildGranularityBuckets(String index, RequestParamsDTO requestParamsDTO, SearchRequestBuilder searchRequestBuilder, Map<String, String> metricsName,
			Map<String, Boolean> validatedData,Map<String,Object> filterData, Set<String> userFilter) {
		try {

			TermsBuilder termBuilder = null;
			DateHistogramBuilder dateHistogram = null;
			String[] groupBy = requestParamsDTO.getGroupBy().split(APIConstants.COMMA);
			boolean isFirstDateHistogram = false;
			FilterAggregationBuilder filterAggregationBuilder = null;

			if (validatedData.get(Hasdatas.HAS_FILTER.check())) {
				filterAggregationBuilder = includeFilterAggregate(index, requestParamsDTO.getFilter(), validatedData, filterData, userFilter);
			}

			/**
			 * building the bucket
			 */
			for (int i = groupBy.length -1; i >=0; i--) {

				TermsBuilder tempBuilder = null;
				String groupByName = getBusinessLogicService().esFields(index, groupBy[i]);
				if (getBaseConnectionService().getFieldsDataType().containsKey(groupBy[i])
						&& getBaseConnectionService().getFieldsDataType().get(groupBy[i]).equalsIgnoreCase(APIConstants.LogicalConstants.DATE.value())) {
					isFirstDateHistogram = true;
					dateHistogram = generateDateHistogram(requestParamsDTO.getGranularity(), groupBy[i], groupByName);
					if (termBuilder != null) {
						dateHistogram.subAggregation(termBuilder);
						termBuilder = null;
					}
				} else {
					if (termBuilder != null) {
						tempBuilder = AggregationBuilders.terms(groupBy[i]).field(groupByName);
						if (dateHistogram != null) {
							if (termBuilder != null) {
								dateHistogram.subAggregation(termBuilder);
							}
						} else {
							tempBuilder.subAggregation(termBuilder);
						}
						termBuilder = tempBuilder;
					} else {
						termBuilder = AggregationBuilders.terms(groupBy[i]).field(groupByName);
					}
					if (dateHistogram != null) {
						termBuilder.subAggregation(dateHistogram);
						dateHistogram = null;
					}
					isFirstDateHistogram = false;
				}
				if (i == groupBy.length - 1 && !isFirstDateHistogram) {
					if (termBuilder != null) {
						includeMetricsAggregation(index, requestParamsDTO, filterAggregationBuilder, searchRequestBuilder, termBuilder, null, null, metricsName);
						includeOrder(requestParamsDTO, validatedData, groupBy[i], termBuilder, null, metricsName);
						termBuilder.size(0);
					}
				}
				if (i == groupBy.length - 1 && isFirstDateHistogram) {
					if (dateHistogram != null) {
						includeMetricsAggregation(index, requestParamsDTO, filterAggregationBuilder, searchRequestBuilder, null, null, dateHistogram, metricsName);
						includeOrder(requestParamsDTO, validatedData, groupBy[i], null, dateHistogram, metricsName);
					}
				}
			}
			
			/**
			 * include the bucket in filter if it has filter
			 */
			if (filterAggregationBuilder != null) {
				if (isFirstDateHistogram) {
					filterAggregationBuilder.subAggregation(dateHistogram);
				} else {
					termBuilder.size(0);
					filterAggregationBuilder.subAggregation(termBuilder);
				}
				searchRequestBuilder.addAggregation(filterAggregationBuilder);
			} else {
				termBuilder.size(0);
				searchRequestBuilder.addAggregation(termBuilder);
			}
		} catch (Exception e) {
			throw new ReportGenerationException(ErrorConstants.BUCKET_ERROR.replace(ErrorConstants.REPLACER,ErrorConstants.GRANULARITY_BUCKET )+e);
		}
	}

	private void buildAggregation(String traceId, String index, RequestParamsDTO requestParamsDTO, SearchRequestBuilder searchRequestBuilder, Map<String, String> metricsName,
			Map<String, Boolean> validatedData, Map<String, Object> filterData, Set<String> userFilter) {

		try {
			FilterAggregationBuilder filterAggregationBuilder = null;
			if (validatedData.get(Hasdatas.HAS_FILTER.check())) {
				filterAggregationBuilder = includeFilterAggregate(index, requestParamsDTO.getFilter(), validatedData, filterData, userFilter);
			}
			includeMetricsAggregation(index, requestParamsDTO, filterAggregationBuilder, searchRequestBuilder, null, null, null, metricsName);
			if (filterAggregationBuilder != null) {
				searchRequestBuilder.addAggregation(filterAggregationBuilder);
			}
		} catch (Exception e) {
			InsightsLogger.error(traceId, ErrorConstants.BUCKET_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.AGGREGATION), e);
		}
	}

	private FilterAggregationBuilder includeFilterAggregate(String index, List<RequestParamsFilterDetailDTO> requestParamsFiltersDetailDTO,Map<String,Boolean> validatedData,Map<String,Object> filterData, Set<String> userFilter) {
		FilterAggregationBuilder filterBuilder = new FilterAggregationBuilder(APIConstants.EsFilterFields.FILTERS.field());
		if (requestParamsFiltersDetailDTO != null) {
			BoolFilterBuilder boolFilter = includeBucketFilter(index, requestParamsFiltersDetailDTO,validatedData);
			if(getBaseAPIService().checkNull(filterData)){
				boolFilter = customFilter(index,filterData,userFilter,boolFilter);
			}
			filterBuilder.filter(boolFilter);
		}
		return filterBuilder;
	}
	
	private BoolFilterBuilder includeBucketFilter(String index, List<RequestParamsFilterDetailDTO> requestParamsFiltersDetailDTO, Map<String,Boolean> validatedData) {

		BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
		if (requestParamsFiltersDetailDTO != null) {
			for (RequestParamsFilterDetailDTO fieldData : requestParamsFiltersDetailDTO) {
				if (fieldData != null) {
					List<RequestParamsFilterFieldsDTO> requestParamsFilterFieldsDTOs = fieldData.getFields();
					AndFilterBuilder andFilter = null;
					OrFilterBuilder orFilter = null;
					NotFilterBuilder notFilter = null;
					for (RequestParamsFilterFieldsDTO fieldsDetails : requestParamsFilterFieldsDTOs) {
						
						if(fieldsDetails.getDataSource() != null && !index.equalsIgnoreCase(fieldsDetails.getDataSource())){
							continue;
						}							
						
						if(validatedData.get(Hasdatas.HAS_MULTIGET.check()) && fieldsDetails.getDataSource() == null){
							continue;
						}
						String fieldName = getBusinessLogicService().esFields(index, fieldsDetails.getFieldName());
						FilterBuilder filter = rangeBucketFilter(fieldsDetails, fieldName);
						if (fieldsDetails.getType().equalsIgnoreCase(APIConstants.EsFilterFields.SELECTOR.field())) {
							if (APIConstants.EsFilterFields.EQ.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
								filter = FilterBuilders.termFilter(fieldName, checkDataType(fieldsDetails.getValue(), fieldsDetails.getValueType(), fieldsDetails.getFormat()));
							} else if (APIConstants.EsFilterFields.LK.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
								filter = FilterBuilders.prefixFilter(fieldName, checkDataType(fieldsDetails.getValue(), fieldsDetails.getValueType(), fieldsDetails.getFormat()).toString());
							} else if (APIConstants.EsFilterFields.EX.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
								filter = FilterBuilders.existsFilter(checkDataType(fieldsDetails.getValue(), fieldsDetails.getValueType(), fieldsDetails.getFormat()).toString());
							} else if (APIConstants.EsFilterFields.IN.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
								filter = FilterBuilders.inFilter(fieldName, fieldsDetails.getValue().split(APIConstants.COMMA));
							}
						}
						if (APIConstants.EsFilterFields.AND.field().equalsIgnoreCase(fieldData.getLogicalOperatorPrefix())) {
							if (andFilter == null) {
								andFilter = FilterBuilders.andFilter(filter);
							} else {
								andFilter.add(filter);
							}
						} else if (APIConstants.EsFilterFields.OR.field().equalsIgnoreCase(fieldData.getLogicalOperatorPrefix())) {
							if (orFilter == null) {
								orFilter = FilterBuilders.orFilter(filter);
							} else {
								orFilter.add(filter);
							}
						} else if (APIConstants.EsFilterFields.NOT.field().equalsIgnoreCase(fieldData.getLogicalOperatorPrefix())) {
							if (notFilter == null) {
								notFilter = FilterBuilders.notFilter(filter);
							}
						}
					}
					if (andFilter != null) {
						boolFilter.must(andFilter);
					}
					if (orFilter != null) {
						boolFilter.must(orFilter);
					}
					if (notFilter != null) {
						boolFilter.must(notFilter);
					}
				}
			}
		}
		return boolFilter;
	}
	
	private FilterBuilder rangeBucketFilter(RequestParamsFilterFieldsDTO fieldsDetails, String fieldName) {

		FilterBuilder filter = null;
		if (APIConstants.EsFilterFields.RG.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
			filter = FilterBuilders.rangeFilter(fieldName).from(checkDataType(fieldsDetails.getFrom(), fieldsDetails.getValueType(), fieldsDetails.getFormat()))
					.to(checkDataType(fieldsDetails.getTo(), fieldsDetails.getValueType(), fieldsDetails.getFormat()));
		} else if (APIConstants.EsFilterFields.NRG.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
			filter = FilterBuilders.notFilter(FilterBuilders.rangeFilter(fieldName).from(checkDataType(fieldsDetails.getFrom(), fieldsDetails.getValueType(), fieldsDetails.getFormat()))
					.to(checkDataType(fieldsDetails.getTo(), fieldsDetails.getValueType(), fieldsDetails.getFormat())));
		} else if (APIConstants.EsFilterFields.LE.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
			filter = FilterBuilders.rangeFilter(fieldName).lte(checkDataType(fieldsDetails.getValue(), fieldsDetails.getValueType(), fieldsDetails.getFormat()));
		} else if (APIConstants.EsFilterFields.GE.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
			filter = FilterBuilders.rangeFilter(fieldName).gte(checkDataType(fieldsDetails.getValue(), fieldsDetails.getValueType(), fieldsDetails.getFormat()));
		} else if (APIConstants.EsFilterFields.LT.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
			filter = FilterBuilders.rangeFilter(fieldName).lt(checkDataType(fieldsDetails.getValue(), fieldsDetails.getValueType(), fieldsDetails.getFormat()));
		} else if (APIConstants.EsFilterFields.GT.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
			filter = FilterBuilders.rangeFilter(fieldName).gt(checkDataType(fieldsDetails.getValue(), fieldsDetails.getValueType(), fieldsDetails.getFormat()));
		}
		return filter;
	}

	/**
	 * This will sort the bucket
	 * @param termsBuilder This is an term bucket to be sorted
	 * @param histogramBuilder This is an date bucket to be sorted
	 * @param requestParamsDTO This object is an request object
	 * @param orderData will holds odering data
	 * @param metricsName will check for metrics to be sorted
	 */
	private void sortAggregatedValue(TermsBuilder termsBuilder, DateHistogramBuilder histogramBuilder, RequestParamsDTO requestParamsDTO, RequestParamsSortDTO orderData, Map<String, String> metricsName) {
		if (termsBuilder != null) {
			if (metricsName.containsKey(orderData.getSortBy())) {
				if (APIConstants.DESC.equalsIgnoreCase(orderData.getSortOrder())) {
					termsBuilder.order(org.elasticsearch.search.aggregations.bucket.terms.Terms.Order.aggregation(metricsName.get(orderData.getSortBy()), false));
				} else {
					termsBuilder.order(org.elasticsearch.search.aggregations.bucket.terms.Terms.Order.aggregation(metricsName.get(orderData.getSortBy()), true));
				}
			}
		}
		if (histogramBuilder != null) {
			if (metricsName.containsKey(orderData.getSortBy())) {
				if (APIConstants.DESC.equalsIgnoreCase(orderData.getSortOrder())) {
					histogramBuilder.order(Order.KEY_DESC);
				} else {
					histogramBuilder.order(Order.KEY_ASC);
				}
			}
		}
	}
	
	/**
	 * 
	 * @param index
	 * @param requestParamsDTO
	 * @param termBuilder
	 * @param metricsName
	 * @deprecated Reason : This method is handled in the method includeMetricsAggregation()
	 */
	private void bucketAggregation(String index, RequestParamsDTO requestParamsDTO, TermsBuilder termBuilder, Map<String, String> metricsName) {

		if (!requestParamsDTO.getAggregations().isEmpty()) {
			try {
				Gson gson = new Gson();
				String requestJsonArray = gson.toJson(requestParamsDTO.getAggregations());
				JSONArray jsonArray = new JSONArray(requestJsonArray);

				for (int i = 0; i < jsonArray.length(); i++) {

					JSONObject jsonObject;
					jsonObject = new JSONObject(jsonArray.get(i).toString());
					String requestValue = jsonObject.get(APIConstants.FormulaFields.REQUEST_VALUES.getField()).toString();
					String fieldName = getBusinessLogicService().esFields(index, jsonObject.getString(requestValue));
					includeBucketAggregation(termBuilder, jsonObject, jsonObject.getString(APIConstants.FormulaFields.FORMULA.getField()), APIConstants.FormulaFields.FIELD.getField()+i, fieldName);
					metricsName.put(jsonObject.getString(APIConstants.FormulaFields.NAME.getField()) != null ? jsonObject.getString(APIConstants.FormulaFields.NAME.getField()) : fieldName,
							APIConstants.FormulaFields.FIELD.getField()+i);
				}
			} catch (Exception e) {
				throw new ReportGenerationException(ErrorConstants.AGGREGATION_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.AGGREGATION_BUCKET)+e);
			}
		}
	}

	/**
	 * 
	 * @param index
	 * @param requestParamsDTO
	 * @param rangebuilder
	 * @param metricsName
	 * @deprecated Reason : This method is handled in the method includeMetricsAggregation()
	 */
	private void rangeBucketAggregation(String index, RequestParamsDTO requestParamsDTO, RangeBuilder rangebuilder, Map<String, String> metricsName) {

		if (!requestParamsDTO.getAggregations().isEmpty()) {
			try {
				Gson gson = new Gson();
				String requestJsonArray = gson.toJson(requestParamsDTO.getAggregations());
				JSONArray jsonArray = new JSONArray(requestJsonArray);

				for (int i = 0; i < jsonArray.length(); i++) {

					JSONObject jsonObject;
					jsonObject = new JSONObject(jsonArray.get(i).toString());
					String requestValue = jsonObject.get(APIConstants.FormulaFields.REQUEST_VALUES.getField()).toString();
					String fieldName = getBusinessLogicService().esFields(index, jsonObject.getString(requestValue));
					includeRangeBucketAggregation(rangebuilder, jsonObject, jsonObject.getString(APIConstants.FormulaFields.FORMULA.getField()), requestValue, fieldName);
					metricsName.put(jsonObject.getString(APIConstants.FormulaFields.NAME.getField()) != null ? jsonObject.getString(APIConstants.FormulaFields.NAME.getField()) : fieldName,
							requestValue);
				}
			} catch (Exception e) {
				throw new ReportGenerationException(ErrorConstants.AGGREGATION_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.RANGE_BUCKET)+e);
			}
		}
	}
	
	/**
	 * 
	 * @param index
	 * @param requestParamsDTO
	 * @param dateHistogramBuilder
	 * @param metricsName
	 * @deprecated Reason : This method is handled in the method includeMetricsAggregation() 
	 *
	 */
	private void granularityBucketAggregation(String index, RequestParamsDTO requestParamsDTO, DateHistogramBuilder dateHistogramBuilder, Map<String, String> metricsName) {

		if (!requestParamsDTO.getAggregations().isEmpty()) {
			try {
				Gson gson = new Gson();
				String requestJsonArray = gson.toJson(requestParamsDTO.getAggregations());
				JSONArray jsonArray = new JSONArray(requestJsonArray);

				for (int i = 0; i < jsonArray.length(); i++) {
					JSONObject jsonObject;
					jsonObject = new JSONObject(jsonArray.get(i).toString());

					String requestValues = jsonObject.get(APIConstants.FormulaFields.REQUEST_VALUES.getField()).toString();
					String fieldName = getBusinessLogicService().esFields(index, jsonObject.get(requestValues).toString());
					includeGranularityAggregation(dateHistogramBuilder, jsonObject, jsonObject.getString(APIConstants.FormulaFields.FORMULA.getField()), APIConstants.FormulaFields.FIELD.getField()+i, fieldName);
					metricsName.put(jsonObject.getString(APIConstants.FormulaFields.NAME.getField()) != null ? jsonObject.getString(APIConstants.FormulaFields.NAME.getField()) : fieldName,
							APIConstants.FormulaFields.FIELD.getField()+i);
				}
			} catch (Exception e) {
				throw new ReportGenerationException(ErrorConstants.AGGREGATION_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.GRANULARITY_BUCKET)+e);
			}
		}
	}
	
	/**
	 * Here Metrics aggregation can be added to dateHistogramBuilder, termBuilder,rangeAggregationBuilder,filterAggregationBuilder and searchRequestBuilder 
	 * @param index
	 * @param requestParamsDTO
	 * @param filterAggregationBuilder
	 * @param searchRequestBuilder
	 * @param termBuilder
	 * @param rangeAggregationBuilder
	 * @param dateHistogramBuilder
	 * @param metricsName
	 */
	private void includeMetricsAggregation(String index, RequestParamsDTO requestParamsDTO, FilterAggregationBuilder filterAggregationBuilder,SearchRequestBuilder searchRequestBuilder, TermsBuilder termBuilder, RangeBuilder rangeAggregationBuilder, DateHistogramBuilder dateHistogramBuilder, Map<String, String> metricsName) {

		if (!requestParamsDTO.getAggregations().isEmpty()) {
			try {
				List<Map<String, String>> aggregationMapAsList = requestParamsDTO.getAggregations();
				int fieldIndex = 0;
				MetricsAggregationBuilder<?> metricsAggregationBuilder = null;
				AggregationBuilder<?> aggregationBuilder = null;
				if(dateHistogramBuilder != null) {
					aggregationBuilder = dateHistogramBuilder;
				} else if(rangeAggregationBuilder != null) {
					aggregationBuilder = rangeAggregationBuilder;
				} else if(termBuilder != null) {
					aggregationBuilder = termBuilder;
				} else if (filterAggregationBuilder != null){ 
					aggregationBuilder = filterAggregationBuilder;
				}
				
				for(Map<String, String> aggregationMap : aggregationMapAsList) {
					String requestValue = aggregationMap.get(APIConstants.FormulaFields.REQUEST_VALUES.getField()).toString();
					String fieldName = getBusinessLogicService().esFields(index, aggregationMap.get(requestValue));
					metricsAggregationBuilder = buildMetrics(metricsAggregationBuilder, aggregationMap, aggregationMap.get(APIConstants.FormulaFields.FORMULA.getField()).toString(), APIConstants.FormulaFields.FIELD.getField() + fieldIndex, fieldName);
					if(aggregationBuilder != null) {
						aggregationBuilder.subAggregation(metricsAggregationBuilder);
					} else {
						searchRequestBuilder.addAggregation(metricsAggregationBuilder);
					}
					metricsName.put(aggregationMap.get(APIConstants.FormulaFields.NAME.getField()) != null ? aggregationMap.get(APIConstants.FormulaFields.NAME.getField()) : fieldName,
							APIConstants.FormulaFields.FIELD.getField() + fieldIndex);
					fieldIndex++;
				}
				
			} catch (Exception e) {
				throw new ReportGenerationException(ErrorConstants.AGGREGATION_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.AGGREGATION_BUCKET), e);
			}
		}
	}
	
	/**
	 * 
	 * @param mainFilter
	 * @param jsonObject
	 * @param aggregateType
	 * @param aggregateName
	 * @param fieldName
	 * @deprecated Reason : This method is handled in the method buildMetrics
	 */
	private void includeBucketAggregation(TermsBuilder mainFilter,JSONObject jsonObject,String aggregateType,String aggregateName,String fieldName){

		try {
			if(APIConstants.AggregateFields.SUM.getField().equalsIgnoreCase(aggregateType)){
				mainFilter.subAggregation(AggregationBuilders.sum(aggregateName).field(fieldName));
			}else if(APIConstants.AggregateFields.AVG.getField().equalsIgnoreCase(aggregateType)){
				mainFilter.subAggregation(AggregationBuilders.avg(aggregateName).field(fieldName));
			}else if(APIConstants.AggregateFields.MAX.getField().equalsIgnoreCase(aggregateType)){
				mainFilter.subAggregation(AggregationBuilders.max(aggregateName).field(fieldName));
			}else if(APIConstants.AggregateFields.MIN.getField().equalsIgnoreCase(aggregateType)){
				mainFilter.subAggregation(AggregationBuilders.min(aggregateName).field(fieldName));
			}else if(APIConstants.AggregateFields.COUNT.getField().equalsIgnoreCase(aggregateType)){
				mainFilter.subAggregation(AggregationBuilders.count(aggregateName).field(fieldName));
			}else if(APIConstants.AggregateFields.DISTINCT.getField().equalsIgnoreCase(aggregateType)){
				mainFilter.subAggregation(AggregationBuilders.cardinality(aggregateName).field(fieldName));
			}else if(APIConstants.AggregateFields.PERCENTILES.getField().equalsIgnoreCase(aggregateType)){
				PercentilesBuilder percentilesBuilder = AggregationBuilders.percentiles(aggregateName).field(fieldName);
				if(jsonObject.has(APIConstants.AggregateFields.PERCENTS.getField()) && !jsonObject.isNull(APIConstants.AggregateFields.PERCENTS.getField())) {
					String[] percentsArray = jsonObject.get(APIConstants.AggregateFields.PERCENTS.getField()).toString().split(APIConstants.COMMA);
					double[] percents = new double[percentsArray.length];
					for(int index = 0; index < percentsArray.length; index ++) {
						percents[index] = Double.parseDouble(percentsArray[index]);
					}
					percentilesBuilder.percentiles(percents);
				}
				mainFilter.subAggregation(percentilesBuilder);
			}
		} catch (Exception e) {
			throw new ReportGenerationException(ErrorConstants.AGGREGATOR_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.AGGREGATION_BUCKET), e);
		} 
	}
	
	/**
	 * This method gives the metricsAggregationbuilder of the given aggregateType
	 * @param metricsAggregationbuilder
	 * @param aggregationMap
	 * @param aggregateType
	 * @param aggregateName
	 * @param fieldName
	 * @return
	 */
	private MetricsAggregationBuilder<?> buildMetrics(MetricsAggregationBuilder<?> metricsAggregationbuilder, Map<String, String> aggregationMap,String aggregateType,String aggregateName,String fieldName){
		try {
			if (APIConstants.AggregateFields.SUM.getField().equalsIgnoreCase(aggregateType)) {
				metricsAggregationbuilder = AggregationBuilders.sum(aggregateName).field(fieldName);
			} else if (APIConstants.AggregateFields.AVG.getField().equalsIgnoreCase(aggregateType)) {
				metricsAggregationbuilder = AggregationBuilders.avg(aggregateName).field(fieldName);
			} else if (APIConstants.AggregateFields.MAX.getField().equalsIgnoreCase(aggregateType)) {
				metricsAggregationbuilder = AggregationBuilders.max(aggregateName).field(fieldName);
			} else if (APIConstants.AggregateFields.MIN.getField().equalsIgnoreCase(aggregateType)) {
				metricsAggregationbuilder = AggregationBuilders.min(aggregateName).field(fieldName);
			} else if (APIConstants.AggregateFields.COUNT.getField().equalsIgnoreCase(aggregateType)) {
				metricsAggregationbuilder = AggregationBuilders.count(aggregateName).field(fieldName);
			} else if (APIConstants.AggregateFields.DISTINCT.getField().equalsIgnoreCase(aggregateType)) {
				metricsAggregationbuilder = AggregationBuilders.cardinality(aggregateName).field(fieldName);
			} else if (APIConstants.AggregateFields.PERCENTILES.getField().equalsIgnoreCase(aggregateType)) {
				PercentilesBuilder percentilesBuilder = AggregationBuilders.percentiles(aggregateName).field(fieldName);
				if (aggregationMap.containsKey(APIConstants.AggregateFields.PERCENTS.getField()) && StringUtils.isNotBlank(aggregationMap.get(APIConstants.AggregateFields.PERCENTS.getField()))) {
					String[] percentsArray = aggregationMap.get(APIConstants.AggregateFields.PERCENTS.getField()).toString().split(APIConstants.COMMA);
					double[] percents = new double[percentsArray.length];
					for (int index = 0; index < percentsArray.length; index++) {
						percents[index] = Double.parseDouble(percentsArray[index]);
					}
					percentilesBuilder.percentiles(percents);
				}
				metricsAggregationbuilder = percentilesBuilder;

			}
			return metricsAggregationbuilder;
		} catch (Exception e) {
			throw new ReportGenerationException(ErrorConstants.AGGREGATOR_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.METRICS), e);
		} 
	}
	
	/**
	 * 
	 * @param mainFilter
	 * @param jsonObject
	 * @param aggregateType
	 * @param aggregateName
	 * @param fieldName
	 * @deprecated Reason : This method is handled in the method buildMetrics
	 */
	private void includeRangeBucketAggregation(RangeBuilder mainFilter,JSONObject jsonObject,String aggregateType,String aggregateName,String fieldName){

		try {
			if(APIConstants.AggregateFields.SUM.getField().equalsIgnoreCase(aggregateType)){
				mainFilter.subAggregation(AggregationBuilders.sum(aggregateName).field(fieldName));
			}else if(APIConstants.AggregateFields.AVG.getField().equalsIgnoreCase(aggregateType)){
				mainFilter.subAggregation(AggregationBuilders.avg(aggregateName).field(fieldName));
			}else if(APIConstants.AggregateFields.MAX.getField().equalsIgnoreCase(aggregateType)){
				mainFilter.subAggregation(AggregationBuilders.max(aggregateName).field(fieldName));
			}else if(APIConstants.AggregateFields.MIN.getField().equalsIgnoreCase(aggregateType)){
				mainFilter.subAggregation(AggregationBuilders.min(aggregateName).field(fieldName));
			}else if(APIConstants.AggregateFields.COUNT.getField().equalsIgnoreCase(aggregateType)){
				mainFilter.subAggregation(AggregationBuilders.count(aggregateName).field(fieldName));
			}else if(APIConstants.AggregateFields.DISTINCT.getField().equalsIgnoreCase(aggregateType)){
				mainFilter.subAggregation(AggregationBuilders.cardinality(aggregateName).field(fieldName));
			}else if(APIConstants.AggregateFields.PERCENTILES.getField().equalsIgnoreCase(aggregateType)){
				PercentilesBuilder percentilesBuilder = AggregationBuilders.percentiles(aggregateName).field(fieldName);
				if(jsonObject.has(APIConstants.AggregateFields.PERCENTS.getField()) && !jsonObject.isNull(APIConstants.AggregateFields.PERCENTS.getField())) {
					String[] percentsArray = jsonObject.get(APIConstants.AggregateFields.PERCENTS.getField()).toString().split(APIConstants.COMMA);
					double[] percents = new double[percentsArray.length];
					for(int index = 0; index < percentsArray.length; index ++) {
						percents[index] = Double.parseDouble(percentsArray[index]);
					}
					percentilesBuilder.percentiles(percents);
				}
				mainFilter.subAggregation(percentilesBuilder);
			}
		} catch (Exception e) {
			throw new ReportGenerationException(ErrorConstants.AGGREGATOR_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.RANGE_BUCKET), e);
		} 
	}
	
	/**
	 * 
	 * @param dateHistogramBuilder
	 * @param jsonObject
	 * @param aggregateType
	 * @param aggregateName
	 * @param fieldName
	 * @deprecated Reason : This method is handled in the method buildMetrics
	 */
	private void includeGranularityAggregation(DateHistogramBuilder dateHistogramBuilder,JSONObject jsonObject,String aggregateType,String aggregateName,String fieldName){
		try {
			if(APIConstants.AggregateFields.SUM.getField().equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder.subAggregation(AggregationBuilders.sum(aggregateName).field(fieldName));
			}else if(APIConstants.AggregateFields.AVG.getField().equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder.subAggregation(AggregationBuilders.avg(aggregateName).field(fieldName));
			}else if(APIConstants.AggregateFields.MAX.getField().equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder.subAggregation(AggregationBuilders.max(aggregateName).field(fieldName));
			}else if(APIConstants.AggregateFields.MIN.getField().equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder.subAggregation(AggregationBuilders.min(aggregateName).field(fieldName));
			}else if(APIConstants.AggregateFields.COUNT.getField().equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder.subAggregation(AggregationBuilders.count(aggregateName).field(fieldName));
			}else if(APIConstants.AggregateFields.DISTINCT.getField().equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder.subAggregation(AggregationBuilders.cardinality(aggregateName).field(fieldName));
			}else if(APIConstants.AggregateFields.PERCENTILES.getField().equalsIgnoreCase(aggregateType)){
				PercentilesBuilder percentilesBuilder = AggregationBuilders.percentiles(aggregateName).field(fieldName);
				if(jsonObject.has(APIConstants.AggregateFields.PERCENTS.getField()) && !jsonObject.isNull(APIConstants.AggregateFields.PERCENTS.getField())) {
					String[] percentsArray = jsonObject.get(APIConstants.AggregateFields.PERCENTS.getField()).toString().split(APIConstants.COMMA);
					double[] percents = new double[percentsArray.length];
					for(int index = 0; index < percentsArray.length; index ++) {
						percents[index] = Double.parseDouble(percentsArray[index]);
					}
					percentilesBuilder.percentiles(percents);
				}
				dateHistogramBuilder.subAggregation(percentilesBuilder);
			}
		} catch (Exception e) {
			throw new ReportGenerationException(ErrorConstants.AGGREGATOR_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.GRANULARITY_BUCKET), e);
		} 
	}

	private BoolFilterBuilder customFilter(String index, Map<String, Object> filterData, Set<String> userFilter,BoolFilterBuilder boolFilter) {

		Set<String> keys = filterData.keySet();
		Map<String, String> supportFilters = getBaseConnectionService().getFieldsJoinCache().get(index);
		Set<String> supportKeys = supportFilters.keySet();
		String supportKey = APIConstants.EMPTY;
		for (String key : supportKeys) {
			if (getBaseAPIService().checkNull(supportKey)) {
				supportKey += APIConstants.COMMA;
			}
			supportKey = key;
		}
		for (String key : keys) {
			if (supportKey.contains(key)) {
				userFilter.add(key);
				Set<Object> data = (Set<Object>) filterData.get(key);
				if (!data.isEmpty()) {
					boolFilter.must(FilterBuilders.inFilter(getBusinessLogicService().esFields(index, key), getBaseAPIService().convertSettoArray(data)));
				}
			}
		}
		return boolFilter;
	}
	
	private DateHistogramBuilder generateDateHistogram(String granularity, String fieldName, String field) {

		String format = APIConstants.DateFormats.DEFAULT.format();
		if (getBaseAPIService().checkNull(granularity)) {
			granularity = granularity.toUpperCase();
			org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Interval interval = DateHistogram.Interval.DAY;
			if (APIConstants.DateFormats.YEAR.name().equalsIgnoreCase(granularity)) {
				interval = DateHistogram.Interval.YEAR;
				format = APIConstants.DateFormats.YEAR.format();
			} else if (APIConstants.DateFormats.DAY.name().equalsIgnoreCase(granularity)) {
				interval = DateHistogram.Interval.DAY;
				format = APIConstants.DateFormats.DAY.format();
			} else if (APIConstants.DateFormats.MONTH.name().equalsIgnoreCase(granularity)) {
				interval = DateHistogram.Interval.MONTH;
				format = APIConstants.DateFormats.MONTH.format();
			} else if (APIConstants.DateFormats.HOUR.name().equalsIgnoreCase(granularity)) {
				interval = DateHistogram.Interval.HOUR;
				format = APIConstants.DateFormats.HOUR.format();
			} else if (APIConstants.DateFormats.MINUTE.name().equalsIgnoreCase(granularity)) {
				interval = DateHistogram.Interval.MINUTE;
				format = APIConstants.DateFormats.MINUTE.format();
			} else if (APIConstants.DateFormats.SECOND.name().equalsIgnoreCase(granularity)) {
				interval = DateHistogram.Interval.SECOND;
			} else if (APIConstants.DateFormats.QUARTER.name().equalsIgnoreCase(granularity)) {
				interval = DateHistogram.Interval.QUARTER;
				format = APIConstants.DateFormats.QUARTER.format();
			} else if (APIConstants.DateFormats.WEEK.name().equalsIgnoreCase(granularity)) {
				format = APIConstants.DateFormats.WEEK.format();
				interval = DateHistogram.Interval.WEEK;
			} else {
				Map<String,Object> customDateHistogram = customDateHistogram(granularity);
				if(customDateHistogram != null){
					format = customDateHistogram.get("format").toString();
					interval = (Interval) customDateHistogram.get("interval");
				}
			}
			DateHistogramBuilder dateHistogram = AggregationBuilders.dateHistogram(fieldName).field(field).interval(interval).format(format);
			return dateHistogram;
		}
		return null;
	}

	private Map<String,Object> customDateHistogram(String granularity){
		Map<String,Object> customDateHistogram = new HashMap<String, Object>();
		if (granularity.matches(APIConstants.DateHistory.D_CHECKER.replace())) {
			int days = new Integer(granularity.replaceFirst(APIConstants.DateHistory.D_REPLACER.replace(), APIConstants.EMPTY));
			customDateHistogram.put("format", APIConstants.DateFormats.D.format());
			customDateHistogram.put("interval", DateHistogram.Interval.days(days));
		} else if (granularity.matches(APIConstants.DateHistory.W_CHECKER.replace())) {
			int weeks = new Integer(granularity.replaceFirst(APIConstants.DateHistory.W_REPLACER.replace(), APIConstants.EMPTY));
			customDateHistogram.put("format", APIConstants.DateFormats.W.name());
			customDateHistogram.put("interval", DateHistogram.Interval.weeks(weeks));
		} else if (granularity.matches(APIConstants.DateHistory.H_CHECKER.replace())) {
			int hours = new Integer(granularity.replaceFirst(APIConstants.DateHistory.H_REPLACER.replace(), APIConstants.EMPTY));
			customDateHistogram.put("format", APIConstants.DateFormats.H.name());
			customDateHistogram.put("interval", DateHistogram.Interval.hours(hours));
		} else if (granularity.matches(APIConstants.DateHistory.K_CHECKER.replace())) {
			int minutes = new Integer(granularity.replaceFirst(APIConstants.DateHistory.K_REPLACER.replace(), APIConstants.EMPTY));
			customDateHistogram.put("format", APIConstants.DateFormats.K.format());
			customDateHistogram.put("interval", DateHistogram.Interval.minutes(minutes));
		} else if (granularity.matches(APIConstants.DateHistory.S_CHECKER.replace())) {
			int seconds = new Integer(granularity.replaceFirst(APIConstants.DateHistory.S_REPLACER.replace(), APIConstants.EMPTY));
			customDateHistogram.put("interval", DateHistogram.Interval.seconds(seconds));
		}else{
			return null;
		}
		return customDateHistogram;
	}
		
	private void includeOrder(RequestParamsDTO requestParamsDTO, Map<String, Boolean> validatedData, String fieldName, TermsBuilder termsBuilder, DateHistogramBuilder dateHistogramBuilder,
			Map<String, String> metricsName) {

		if (validatedData.get(APIConstants.Hasdatas.HAS_SORTBY.check())) {
			RequestParamsPaginationDTO pagination = requestParamsDTO.getPagination();
			List<RequestParamsSortDTO> orderDatas = pagination.getOrder();
			for (RequestParamsSortDTO orderData : orderDatas) {
				if (termsBuilder != null) {
					if (fieldName.equalsIgnoreCase(orderData.getSortBy())) {
						if (APIConstants.DESC.equalsIgnoreCase(orderData.getSortOrder())) {

							termsBuilder.order(org.elasticsearch.search.aggregations.bucket.terms.Terms.Order.term(false));
						} else {
							termsBuilder.order(org.elasticsearch.search.aggregations.bucket.terms.Terms.Order.term(true));
						}
					}
					sortAggregatedValue(termsBuilder, null, requestParamsDTO, orderData, metricsName);
				} else if (dateHistogramBuilder != null) {
					if (fieldName.equalsIgnoreCase(orderData.getSortBy())) {
						if (APIConstants.DESC.equalsIgnoreCase(orderData.getSortOrder())) {

							dateHistogramBuilder.order(Order.KEY_DESC);
						} else {
							dateHistogramBuilder.order(Order.KEY_ASC);
						}
					}
					sortAggregatedValue(null, dateHistogramBuilder, requestParamsDTO, orderData, metricsName);
				}
			}
		}
	}
	
	private Long prepareCount(RequestParamsDTO requestParamsDTO,String indices,Map<String,Boolean> validatedData){
		CountRequestBuilder countRequestBuilder = getBaseConnectionService().getProdClient().prepareCount(indices);

		BoolFilterBuilder boolFilter = includeBucketFilter(indices,requestParamsDTO.getFilter(),validatedData);
		ConstantScoreQueryBuilder filterQuery = 	QueryBuilders.constantScoreQuery(boolFilter);
		if(boolFilter.hasClauses()){
				countRequestBuilder.setQuery(filterQuery);
			}
		try{
		return countRequestBuilder.execute().actionGet().getCount();
		}catch(Exception e){
			throw new ReportGenerationException(ErrorConstants.QUERY_ERROR+countRequestBuilder.toString());
		}
	}

	private Object checkDataType(String value, String valueType, String dateformat) {

		SimpleDateFormat format = new SimpleDateFormat(APIConstants.DEFAULT_FORMAT);
		if (getBaseAPIService().checkNull(dateformat)) {
			try {
				format = new SimpleDateFormat(dateformat);
			} catch (Exception e) {
				throw new ReportGenerationException(ErrorConstants.INVALID_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.FORMAT));
			}
		}
		if (APIConstants.DataTypes.STRING.dataType().equalsIgnoreCase(valueType)) {
			return value;
		} else if (APIConstants.DataTypes.LONG.dataType().equalsIgnoreCase(valueType)) {
			return Long.valueOf(value);
		} else if (APIConstants.DataTypes.INTEGER.dataType().equalsIgnoreCase(valueType)) {
			return Integer.valueOf(value);
		} else if (APIConstants.DataTypes.DOUBLE.dataType().equalsIgnoreCase(valueType)) {
			return Double.valueOf(value);
		} else if (APIConstants.DataTypes.SHORT.dataType().equalsIgnoreCase(valueType)) {
			return Short.valueOf(value);
		} else if (APIConstants.DataTypes.DATE.dataType().equalsIgnoreCase(valueType)) {
			try {
				return format.parse(value).getTime();
			} catch (ParseException e) {
				throw new ReportGenerationException(ErrorConstants.INVALID_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.DATA_TYPE));
			}
		}
		return Integer.valueOf(value);
	}
	

	public BaseConnectionService getBaseConnectionService() {
		return baseConnectionService;
	}

	public BaseAPIService getBaseAPIService() {
		return baseAPIService;
	}

	public ESDataProcessor getBusinessLogicService() {
		return businessLogicService;
	}
}

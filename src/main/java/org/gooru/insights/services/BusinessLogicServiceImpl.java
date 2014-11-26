package org.gooru.insights.services;

import java.lang.reflect.Type;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.activation.DataHandler;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.MatchAllFilterBuilder;
import org.elasticsearch.index.query.NestedFilterBuilder;
import org.elasticsearch.index.query.NestedFilterParser;
import org.elasticsearch.index.query.NotFilterBuilder;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.gooru.insights.constants.APIConstants.hasdata;
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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

@Service
public class BusinessLogicServiceImpl implements BusinessLogicService{

	@Autowired
	BaseConnectionService baseConnectionService;
	
	@Autowired
	BaseAPIService baseAPIService;
	
	
	public boolean aggregate(String index,RequestParamsDTO requestParamsDTO,SearchRequestBuilder searchRequestBuilder,Map<String,String> metricsName,Map<String,Boolean> validatedData,Integer recordSize) {
		try{
			TermsBuilder termBuilder = null;
			String[] groupBy = requestParamsDTO.getGroupBy().split(",");
			for(int i=groupBy.length-1; i >= 0;i--){
				TermsBuilder tempBuilder = null;
				if(termBuilder != null){
						tempBuilder = AggregationBuilders.terms("field"+i).field(esFields(index,groupBy[i]));
						tempBuilder.subAggregation(termBuilder);
						termBuilder = tempBuilder;
				}else{
					termBuilder = AggregationBuilders.terms("field"+i).field(esFields(index,groupBy[i]));
				}
				termBuilder.size(500);
				if( i == groupBy.length-1){
					System.out.println("expected");
					includeAggregation(index,requestParamsDTO, termBuilder,metricsName);
				}
			}
			if(baseAPIService.checkNull(requestParamsDTO.getFilter())){
				FilterAggregationBuilder filterBuilder = null;
			if(filterBuilder == null){
				filterBuilder = includeFilterAggregate(index,requestParamsDTO.getFilter());
			}
			if(termBuilder != null){
//				termBuilder.size(recordSize);
				filterBuilder.subAggregation(termBuilder);
			}
			searchRequestBuilder.addAggregation(filterBuilder);
			}else{
//				termBuilder.size(recordSize);
				searchRequestBuilder.addAggregation(termBuilder);
			}
			return true;
	}catch(Exception e){
		e.printStackTrace();
		return false;
	}
	}
	
	public boolean granularityAggregate(String index,RequestParamsDTO requestParamsDTO,SearchRequestBuilder searchRequestBuilder,Map<String,String> metricsName,Map<String,Boolean> validatedData,Integer recordSize) {
		try{
			TermsBuilder termBuilder = null;
			DateHistogramBuilder dateHistogram = null;
			String[] groupBy = requestParamsDTO.getGroupBy().split(",");
			boolean isFirstDateHistogram = false;
			for(int i=groupBy.length-1; i >= 0;i--){
				TermsBuilder tempBuilder = null;
				String groupByName = esFields(index,groupBy[i]);
				//date field checker	
				if(baseConnectionService.getFieldsDataType().containsKey(groupBy[i]) && baseConnectionService.getFieldsDataType().get(groupBy[i]).equalsIgnoreCase("date")){
					dateHistogram = dateHistogram(requestParamsDTO.getGranularity(),"field"+i,groupByName);
					isFirstDateHistogram =true;
					if(termBuilder != null){
						dateHistogram.subAggregation(termBuilder);
						termBuilder = null;
						}
					}else{
						
						if(termBuilder != null){
						tempBuilder = AggregationBuilders.terms("field"+i).field(groupByName);
						if(dateHistogram != null){
							if(termBuilder != null){
								dateHistogram.subAggregation(termBuilder);
							}
							
						}else{
						tempBuilder.subAggregation(termBuilder);
						}
						termBuilder = tempBuilder;
						}else{
							termBuilder = AggregationBuilders.terms("field"+i).field(groupByName);
						}
						if(dateHistogram != null){
							termBuilder.subAggregation(dateHistogram);
							dateHistogram = null;
						}
						termBuilder.size(500);
						isFirstDateHistogram =false;
					}
				
				if( i == groupBy.length-1 && !isFirstDateHistogram){
					if(termBuilder != null ){
					includeAggregation(index,requestParamsDTO, termBuilder,metricsName);
					includeOrder(requestParamsDTO, validatedData, groupBy[i], termBuilder,null,metricsName);
					}
					}
				
				if( i == groupBy.length-1 && isFirstDateHistogram){
					if(dateHistogram != null ){
					includeAggregation(index,requestParamsDTO, dateHistogram,metricsName);
					includeOrder(requestParamsDTO, validatedData, groupBy[i], null, dateHistogram,metricsName);
					}
					}
			}
			
			if(baseAPIService.checkNull(requestParamsDTO.getFilter())){
				FilterAggregationBuilder filterBuilder = null;
			if(filterBuilder == null){
				filterBuilder = includeFilterAggregate(index,requestParamsDTO.getFilter());
			}

			if(isFirstDateHistogram){
				filterBuilder.subAggregation(dateHistogram);
			}else{
				termBuilder.size(recordSize);
				filterBuilder.subAggregation(termBuilder);	
			}
			searchRequestBuilder.addAggregation(filterBuilder);
			}else{
				termBuilder.size(recordSize);
				searchRequestBuilder.addAggregation(termBuilder);
			}
			
			return true;
	}catch(Exception e){
		
		e.printStackTrace();
		return false;
	}
		
	}
	
	
	public void sortAggregatedValue(TermsBuilder termsBuilder,DateHistogramBuilder histogramBuilder,RequestParamsDTO requestParamsDTO,RequestParamsSortDTO orderData,Map<String,String> metricsName){
		if(termsBuilder != null){	
		if(metricsName.containsKey(orderData.getSortBy())){
				if(orderData.getSortOrder().equalsIgnoreCase("DESC")){
				termsBuilder.order(org.elasticsearch.search.aggregations.bucket.terms.Terms.Order.aggregation(metricsName.get(orderData.getSortBy()),false));
				}else{
					termsBuilder.order(org.elasticsearch.search.aggregations.bucket.terms.Terms.Order.aggregation(metricsName.get(orderData.getSortBy()),true));
				}
			}
		}
		if(histogramBuilder != null){
			if(metricsName.containsKey(orderData.getSortBy())){
				if(orderData.getSortOrder().equalsIgnoreCase("DESC")){
					histogramBuilder.order(Order.KEY_DESC);
				}else{
					histogramBuilder.order(Order.KEY_ASC);
				}
			}	
		}
	}
	
	public void includeAggregation(String index,RequestParamsDTO requestParamsDTO,TermsBuilder termBuilder,Map<String,String> metricsName){
	if (!requestParamsDTO.getAggregations().isEmpty()) {
		try{
		Gson gson = new Gson();
		String requestJsonArray = gson
				.toJson(requestParamsDTO.getAggregations());
		JSONArray jsonArray = new JSONArray(
				requestJsonArray);
		for (int i = 0; i < jsonArray.length(); i++) {
			JSONObject jsonObject;
			jsonObject = new JSONObject(jsonArray.get(i)
					.toString());
			if (!jsonObject.has("formula")
					&& !jsonObject.has("requestValues")) {
				continue;
			}
				if (baseAPIService.checkNull(jsonObject
						.get("formula"))) {
						String requestValues = jsonObject
								.get("requestValues")
								.toString();
						String metricField[] =requestValues.split(","); 
						for (int j=0;j<metricField.length;j++) {
							if (!jsonObject
									.has(metricField[j])) {
								continue;
							}
							String fieldName = esFields(index,jsonObject.get(metricField[j]).toString());
						performAggregation(termBuilder,jsonObject,jsonObject.getString("formula"), "metrics"+i,fieldName);
						metricsName.put(jsonObject.getString("name") != null ? jsonObject.getString("name") : fieldName, "metrics"+i);

						}
				}
		}
	}catch(Exception e){
		e.printStackTrace();
	}
	}
	}
	
	public void includeAggregation(String index,RequestParamsDTO requestParamsDTO,DateHistogramBuilder  dateHistogramBuilder,Map<String,String> metricsName){
		if (!requestParamsDTO.getAggregations().isEmpty()) {
			try{
			Gson gson = new Gson();
			String requestJsonArray = gson
					.toJson(requestParamsDTO.getAggregations());
			JSONArray jsonArray = new JSONArray(
					requestJsonArray);
			for (int i = 0; i < jsonArray.length(); i++) {
				JSONObject jsonObject;
				jsonObject = new JSONObject(jsonArray.get(i)
						.toString());
				if (!jsonObject.has("formula")
						&& !jsonObject.has("requestValues")) {
					continue;
				}
					if (baseAPIService.checkNull(jsonObject
							.get("formula"))) {
							String requestValues = jsonObject
									.get("requestValues")
									.toString();
							String aggregateName[] = requestValues
									.split(",");
							for (int j=0;j<aggregateName.length;j++) {
								if (!jsonObject
										.has(aggregateName[j])) {
									continue;
								}
								String fieldName = esFields(index,jsonObject.get(aggregateName[j]).toString());
							performAggregation(dateHistogramBuilder,jsonObject,jsonObject.getString("formula"), "metrics"+i,fieldName);
							metricsName.put(jsonObject.getString("name") != null ? jsonObject.getString("name") : fieldName, "metrics"+i);

							}
					}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		}
		}
	
	public void performAggregation(TermsBuilder mainFilter,JSONObject jsonObject,String aggregateType,String aggregateName,String fieldName){
		try {
			System.out.println("included aggregate");
			if("SUM".equalsIgnoreCase(aggregateType)){
			mainFilter
			.subAggregation(AggregationBuilders
					.sum(aggregateName)
					.field(fieldName));
			}else if("AVG".equalsIgnoreCase(aggregateType)){
				mainFilter
				.subAggregation(AggregationBuilders.avg(aggregateName).field(fieldName));
			}else if("MAX".equalsIgnoreCase(aggregateType)){
				mainFilter
				.subAggregation(AggregationBuilders.max(aggregateName).field(fieldName));
			}else if("MIN".equalsIgnoreCase(aggregateType)){
				mainFilter
				.subAggregation(AggregationBuilders.min(aggregateName).field(fieldName));
				
			}else if("COUNT".equalsIgnoreCase(aggregateType)){
				mainFilter
				.subAggregation(AggregationBuilders.count(aggregateName).field(fieldName));
			}else if("DISTINCT".equalsIgnoreCase(aggregateType)){
				mainFilter
				.subAggregation(AggregationBuilders.cardinality(aggregateName).field(fieldName));
			}
	
		} catch (Exception e) {
			e.printStackTrace();
		} 
		}
	
	public void performAggregation(DateHistogramBuilder dateHistogramBuilder,JSONObject jsonObject,String aggregateType,String aggregateName,String fieldName){
		try {
			if("SUM".equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder
			.subAggregation(AggregationBuilders
					.sum(aggregateName)
					.field(fieldName));
			}else if("AVG".equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder
				.subAggregation(AggregationBuilders.avg(aggregateName).field(fieldName));
			}else if("MAX".equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder
				.subAggregation(AggregationBuilders.max(aggregateName).field(fieldName));
			}else if("MIN".equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder
				.subAggregation(AggregationBuilders.min(aggregateName).field(fieldName));
				
			}else if("COUNT".equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder
				.subAggregation(AggregationBuilders.count(aggregateName).field(fieldName));
			}else if("DISTINCT".equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder
				.subAggregation(AggregationBuilders.cardinality(aggregateName).field(fieldName));
			}
	
		} catch (Exception e) {
			e.printStackTrace();
		} 
		}
	
	//search Filter
		public FilterAggregationBuilder addFilters(
				String index,List<RequestParamsFilterDetailDTO> requestParamsFiltersDetailDTO) {
			MatchAllFilterBuilder subFilter = FilterBuilders.matchAllFilter();
			FilterAggregationBuilder filterBuilder = new FilterAggregationBuilder("filters");
			if (requestParamsFiltersDetailDTO != null) {
				for (RequestParamsFilterDetailDTO fieldData : requestParamsFiltersDetailDTO) {
				
					if (fieldData != null) {
						List<RequestParamsFilterFieldsDTO> requestParamsFilterFieldsDTOs = fieldData
								.getFields();
						BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
						for (RequestParamsFilterFieldsDTO fieldsDetails : requestParamsFilterFieldsDTOs) {
							FilterBuilder filter = null;
							String fieldName = esFields(index,fieldsDetails.getFieldName());
							if (fieldsDetails.getType()
									.equalsIgnoreCase("selector")) {
								if (fieldsDetails.getOperator().equalsIgnoreCase(
										"rg")) {
									boolFilter.must(FilterBuilders
											.rangeFilter(fieldName)
											.from(checkDataType(
													fieldsDetails.getFrom(),
													fieldsDetails.getValueType(),fieldsDetails.getFormat()))
											.to(checkDataType(
													fieldsDetails.getTo(),
													fieldsDetails.getValueType(),fieldsDetails.getFormat())));
								} else if (fieldsDetails.getOperator()
										.equalsIgnoreCase("nrg")) {
									boolFilter.must(FilterBuilders
											.rangeFilter(fieldName)
											.from(checkDataType(
													fieldsDetails.getFrom(),
													fieldsDetails.getValueType(),fieldsDetails.getFormat()))
											.to(checkDataType(
													fieldsDetails.getTo(),
													fieldsDetails.getValueType(),fieldsDetails.getFormat())));
								} else if (fieldsDetails.getOperator()
										.equalsIgnoreCase("eq")) {
									boolFilter.must(FilterBuilders.termFilter(
											fieldName,
											checkDataType(fieldsDetails.getValue(),
													fieldsDetails.getValueType(),fieldsDetails.getFormat())));
								} else if (fieldsDetails.getOperator()
										.equalsIgnoreCase("lk")) {
									boolFilter.must(FilterBuilders.prefixFilter(
											fieldName,
											checkDataType(fieldsDetails.getValue(),
													fieldsDetails.getValueType(),fieldsDetails.getFormat())
													.toString()));
								} else if (fieldsDetails.getOperator()
										.equalsIgnoreCase("ex")) {
									boolFilter.must(FilterBuilders
											.existsFilter(checkDataType(
													fieldsDetails.getValue(),
													fieldsDetails.getValueType(),fieldsDetails.getFormat())
													.toString()));
								}   else if (fieldsDetails.getOperator()
										.equalsIgnoreCase("in")) {
									boolFilter.must(FilterBuilders.inFilter(fieldName,
											fieldsDetails.getValue().split(",")));
								} else if (fieldsDetails.getOperator()
										.equalsIgnoreCase("le")) {
									boolFilter.must(FilterBuilders.rangeFilter(
											fieldName).lte(
											checkDataType(fieldsDetails.getValue(),
													fieldsDetails.getValueType(),fieldsDetails.getFormat())));
								} else if (fieldsDetails.getOperator()
										.equalsIgnoreCase("ge")) {
									boolFilter.must(FilterBuilders.rangeFilter(
											fieldName).gte(
											checkDataType(fieldsDetails.getValue(),
													fieldsDetails.getValueType(),fieldsDetails.getFormat())));
								} else if (fieldsDetails.getOperator()
										.equalsIgnoreCase("lt")) {
									boolFilter.must(FilterBuilders.rangeFilter(
											fieldName).lt(
											checkDataType(fieldsDetails.getValue(),
													fieldsDetails.getValueType(),fieldsDetails.getFormat())));
								} else if (fieldsDetails.getOperator()
										.equalsIgnoreCase("gt")) {
									boolFilter.must(FilterBuilders.rangeFilter(
											fieldName).gt(
											checkDataType(fieldsDetails.getValue(),
													fieldsDetails.getValueType(),fieldsDetails.getFormat())));
								}
					}
						}
							if (fieldData.getLogicalOperatorPrefix().equalsIgnoreCase(
									"AND")) {
								filterBuilder.filter(FilterBuilders.andFilter(boolFilter));
//								subFilter.must(FilterBuilders.andFilter(boolFilter));
//								filterBuilder.filter(FilterBuilders.andFilter(boolFilter));
							} else if (fieldData.getLogicalOperatorPrefix()
									.equalsIgnoreCase("OR")) {
								filterBuilder.filter(FilterBuilders.orFilter(boolFilter));
//								filterBuilder.filter(FilterBuilders.orFilter(boolFilter));
							} else if (fieldData.getLogicalOperatorPrefix()
									.equalsIgnoreCase("NOT")) {
								filterBuilder.filter(FilterBuilders.notFilter(boolFilter));
//								filterBuilder.filter(FilterBuilders.notFilter(boolFilter));
							
						}
						
					}
				}
				filterBuilder.filter(subFilter);
			}
			
			return filterBuilder;
		}
		
		public BoolFilterBuilder customFilter(String index,Map<String,Set<Object>> filterMap,Set<String> userFilter){
		
			BoolFilterBuilder boolFilter =FilterBuilders.boolFilter();
			
			Set<String> keys = filterMap.keySet();
			Map<String,String> supportFilters = baseConnectionService.getFieldsJoinCache().get(index);
			Set<String> supportKeys = supportFilters.keySet();
			String supportKey = "";
			for(String key : supportKeys){
				if(baseAPIService.checkNull(supportKey)){
					supportKey+=",";	
				}
				supportKey = key;
			}
			for(String key : keys){
			
				if(supportKey.contains(key)){
					userFilter.add(key);
				Set<Object> data = filterMap.get(key);	
			if(!data.isEmpty())
				boolFilter.must(FilterBuilders.inFilter(esFields(index,key), data));
			}
			}
			return boolFilter;
		}

		public BoolFilterBuilder includeFilter(String index,
				List<RequestParamsFilterDetailDTO> requestParamsFiltersDetailDTO) {
			BoolQueryBuilder queryBuilder = QueryBuilders
                    .boolQuery();
			BoolFilterBuilder boolFilter =FilterBuilders.boolFilter();
			if (requestParamsFiltersDetailDTO != null) {
				for (RequestParamsFilterDetailDTO fieldData : requestParamsFiltersDetailDTO) {
					if (fieldData != null) {
						List<RequestParamsFilterFieldsDTO> requestParamsFilterFieldsDTOs = fieldData
								.getFields();
						AndFilterBuilder andFilter = null;
						OrFilterBuilder orFilter = null;
						NotFilterBuilder notFilter =null;
			for (RequestParamsFilterFieldsDTO fieldsDetails : requestParamsFilterFieldsDTOs) {
				FilterBuilder filter = null;
				String fieldName = esFields(index,fieldsDetails.getFieldName());
				if (fieldsDetails.getType()
						.equalsIgnoreCase("selector")) {
					if (fieldsDetails.getOperator().equalsIgnoreCase(
							"rg")) {
							filter = FilterBuilders
								.rangeFilter(fieldName)
								.from(checkDataType(
										fieldsDetails.getFrom(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()))
								.to(checkDataType(
										fieldsDetails.getTo(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					} else if (fieldsDetails.getOperator()
							.equalsIgnoreCase("nrg")) {
						filter =  FilterBuilders
								.rangeFilter(fieldName)
								.from(checkDataType(
										fieldsDetails.getFrom(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()))
								.to(checkDataType(
										fieldsDetails.getTo(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					} else if (fieldsDetails.getOperator()
							.equalsIgnoreCase("eq")) {
						filter = FilterBuilders.termFilter(
								fieldName,
								checkDataType(fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					} else if (fieldsDetails.getOperator()
							.equalsIgnoreCase("lk")) {
						filter =  FilterBuilders.prefixFilter(
								fieldName,
								checkDataType(fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat())
										.toString());
					} else if (fieldsDetails.getOperator()
							.equalsIgnoreCase("ex")) {
						filter = FilterBuilders
								.existsFilter(checkDataType(
										fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat())
										.toString());
					}   else if (fieldsDetails.getOperator()
							.equalsIgnoreCase("in")) {
						filter = FilterBuilders.inFilter(fieldName,
								fieldsDetails.getValue().split(","));
					} else if (fieldsDetails.getOperator()
							.equalsIgnoreCase("le")) {
						filter = FilterBuilders.rangeFilter(
								fieldName).lte(
								checkDataType(fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					} else if (fieldsDetails.getOperator()
							.equalsIgnoreCase("ge")) {
						filter = FilterBuilders.rangeFilter(
								fieldName).gte(
								checkDataType(fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					} else if (fieldsDetails.getOperator()
							.equalsIgnoreCase("lt")) {
						filter = FilterBuilders.rangeFilter(
								fieldName).lt(
								checkDataType(fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					} else if (fieldsDetails.getOperator()
							.equalsIgnoreCase("gt")) {
						filter = FilterBuilders.rangeFilter(
								fieldName).gt(
								checkDataType(fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					}
					}

			
			if (fieldData.getLogicalOperatorPrefix().equalsIgnoreCase(
					"AND")) {
				if(andFilter == null){
					andFilter = FilterBuilders.andFilter(filter);
				}else{
					andFilter.add(filter);
				}
			}else if (fieldData.getLogicalOperatorPrefix().equalsIgnoreCase(
					"OR")) {
				if(orFilter == null){
					orFilter = FilterBuilders.orFilter(filter);
				}else{
					orFilter.add(filter);
				}
			}else if (fieldData.getLogicalOperatorPrefix().equalsIgnoreCase(
					"NOT")) {
				if(notFilter == null){
					notFilter = FilterBuilders.notFilter(filter);
				}
			}
			}
			if(andFilter != null){
				boolFilter.must(andFilter);
			}
			if(orFilter != null){
				boolFilter.must(orFilter);
			}
			if(notFilter != null){
				boolFilter.must(notFilter);
			}
					}
				}
			}
			return boolFilter;
		}
		
		public FilterAggregationBuilder includeFilterAggregate(
				String index,List<RequestParamsFilterDetailDTO> requestParamsFiltersDetailDTO) {
			FilterAggregationBuilder filterBuilder = new FilterAggregationBuilder("filters");
			if (requestParamsFiltersDetailDTO != null) {
				BoolFilterBuilder boolFilter =FilterBuilders.boolFilter();
				for (RequestParamsFilterDetailDTO fieldData : requestParamsFiltersDetailDTO) {
					if (fieldData != null) {
						List<RequestParamsFilterFieldsDTO> requestParamsFilterFieldsDTOs = fieldData
								.getFields();
						AndFilterBuilder andFilter = null;
						OrFilterBuilder orFilter = null;
						NotFilterBuilder notFilter =null;
			for (RequestParamsFilterFieldsDTO fieldsDetails : requestParamsFilterFieldsDTOs) {
				FilterBuilder filter = null;
				String fieldName = esFields(index,fieldsDetails.getFieldName());
				if (fieldsDetails.getType()
						.equalsIgnoreCase("selector")) {
					if (fieldsDetails.getOperator().equalsIgnoreCase(
							"rg")) {
							filter = FilterBuilders
								.rangeFilter(fieldName)
								.from(checkDataType(
										fieldsDetails.getFrom(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()))
								.to(checkDataType(
										fieldsDetails.getTo(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					} else if (fieldsDetails.getOperator()
							.equalsIgnoreCase("nrg")) {
						filter =  FilterBuilders
								.rangeFilter(fieldName)
								.from(checkDataType(
										fieldsDetails.getFrom(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()))
								.to(checkDataType(
										fieldsDetails.getTo(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					} else if (fieldsDetails.getOperator()
							.equalsIgnoreCase("eq")) {
						filter = FilterBuilders.termFilter(
								fieldName,
								checkDataType(fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					} else if (fieldsDetails.getOperator()
							.equalsIgnoreCase("lk")) {
						filter =  FilterBuilders.prefixFilter(
								fieldName,
								checkDataType(fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat())
										.toString());
					} else if (fieldsDetails.getOperator()
							.equalsIgnoreCase("ex")) {
						filter = FilterBuilders
								.existsFilter(checkDataType(
										fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat())
										.toString());
					}   else if (fieldsDetails.getOperator()
							.equalsIgnoreCase("in")) {
						filter = FilterBuilders.inFilter(fieldName,
								fieldsDetails.getValue().split(","));
					} else if (fieldsDetails.getOperator()
							.equalsIgnoreCase("le")) {
						filter = FilterBuilders.rangeFilter(
								fieldName).lte(
								checkDataType(fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					} else if (fieldsDetails.getOperator()
							.equalsIgnoreCase("ge")) {
						filter = FilterBuilders.rangeFilter(
								fieldName).gte(
								checkDataType(fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					} else if (fieldsDetails.getOperator()
							.equalsIgnoreCase("lt")) {
						filter = FilterBuilders.rangeFilter(
								fieldName).lt(
								checkDataType(fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					} else if (fieldsDetails.getOperator()
							.equalsIgnoreCase("gt")) {
						filter = FilterBuilders.rangeFilter(
								fieldName).gt(
								checkDataType(fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					}
					}

			
			if (fieldData.getLogicalOperatorPrefix().equalsIgnoreCase(
					"AND")) {
				if(andFilter == null){
					andFilter = FilterBuilders.andFilter(filter);
				}else{
					andFilter.add(filter);
				}
			}else if (fieldData.getLogicalOperatorPrefix().equalsIgnoreCase(
					"OR")) {
				if(orFilter == null){
					orFilter = FilterBuilders.orFilter(filter);
				}else{
					orFilter.add(filter);
				}
			}else if (fieldData.getLogicalOperatorPrefix().equalsIgnoreCase(
					"NOT")) {
				if(notFilter == null){
					notFilter = FilterBuilders.notFilter(filter);
				}
			}
			}
			if(andFilter != null){
				boolFilter.must(andFilter);
			}
			if(orFilter != null){
				boolFilter.must(orFilter);
			}
			if(notFilter != null){
				boolFilter.must(notFilter);
			}
					}
				}
				filterBuilder.filter(boolFilter);
			}
			return filterBuilder;
		}

		public Object checkDataType(String value, String valueType,String dateformat) {
			
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
			
			if(baseAPIService.checkNull(dateformat)){
				try{
				format = new SimpleDateFormat(dateformat);
				}catch(Exception e){
					
				}
			}
			if (valueType.equalsIgnoreCase("String")) {
				return value;
			} else if (valueType.equalsIgnoreCase("Long")) {
				return Long.valueOf(value);
			} else if (valueType.equalsIgnoreCase("Integer")) {
				return Integer.valueOf(value);
			} else if (valueType.equalsIgnoreCase("Double")) {
				return Double.valueOf(value);
			} else if (valueType.equalsIgnoreCase("Short")) {
				return Short.valueOf(value);
			}else if (valueType.equalsIgnoreCase("Date")) {
				try {
					return format.parse(value).getTime();
				} catch (ParseException e) {
					e.printStackTrace();
					return value.toString();
				}
			}
			return Integer.valueOf(value);
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
		public DateHistogramBuilder dateHistogram(String granularity,String fieldName,String field){
			
			String format ="yyyy-MM-dd hh:kk:ss";
			if(baseAPIService.checkNull(granularity)){
				org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Interval interval = DateHistogram.Interval.DAY;
				if(granularity.equalsIgnoreCase("year")){
					interval = DateHistogram.Interval.YEAR;
					format ="yyyy";
				}else if(granularity.equalsIgnoreCase("day")){
					interval = DateHistogram.Interval.DAY;
					format ="yyyy-MM-dd";
				}else if(granularity.equalsIgnoreCase("month")){
					interval = DateHistogram.Interval.MONTH;
					format ="yyyy-MM";
				}else if(granularity.equalsIgnoreCase("hour")){
					interval = DateHistogram.Interval.HOUR;
					format ="yyyy-MM-dd hh";
				}else if(granularity.equalsIgnoreCase("minute")){
					interval = DateHistogram.Interval.MINUTE;
					format ="yyyy-MM-dd hh:kk";
				}else if(granularity.equalsIgnoreCase("second")){
					interval = DateHistogram.Interval.SECOND;
				}else if(granularity.equalsIgnoreCase("quarter")){
					interval = DateHistogram.Interval.QUARTER;
					format ="yyyy-MM-dd";
				}else if(granularity.equalsIgnoreCase("week")){
					format ="yyyy-MM-dd";
					interval = DateHistogram.Interval.WEEK;
				}else if(granularity.endsWith("d")){
					int days = new Integer(granularity.replace("d",""));
					format ="yyyy-MM-dd";
					interval = DateHistogram.Interval.days(days);
				}else if(granularity.endsWith("w")){
					int weeks = new Integer(granularity.replace("w",""));
					format ="yyyy-MM-dd";
					interval = DateHistogram.Interval.weeks(weeks);
				}else if(granularity.endsWith("h")){
					int hours = new Integer(granularity.replace("h",""));
					format ="yyyy-MM-dd hh";
					interval = DateHistogram.Interval.hours(hours);
				}else if(granularity.endsWith("k")){
					int minutes = new Integer(granularity.replace("k",""));
					format ="yyyy-MM-dd hh:kk";
					interval = DateHistogram.Interval.minutes(minutes);
				}else if(granularity.endsWith("s")){
					int seconds = new Integer(granularity.replace("s",""));
					interval = DateHistogram.Interval.seconds(seconds);
				}
				
				DateHistogramBuilder dateHistogram = AggregationBuilders.dateHistogram(fieldName).field(field).interval(interval).format(format);
				return dateHistogram;
		}
			return null;
	}
		
		public List<Map<String,Object>> buildAggregateJSON(String[] groupBy,String resultData,Map<String,String> metrics,boolean hasFilter){

			List<Map<String,Object>> data = new ArrayList<Map<String,Object>>();
			try {
				int counter=0;
				JSONObject json = new JSONObject(resultData);
				json = new JSONObject(json.get("aggregations").toString());
				if(hasFilter){
					json = new JSONObject(json.get("filters").toString());
				}
				Map<Object,Map<String,Object>> intermediateMap = new HashMap<Object,Map<String,Object>>(); 
				while(counter < groupBy.length){
					if(json.length() > 0){
					JSONObject requestJSON = new JSONObject(json.get("field"+counter).toString());
				JSONArray jsonArray = new JSONArray(requestJSON.get("buckets").toString());
				JSONArray subJsonArray = new JSONArray();
				boolean hasSubAggregate = false;
				boolean hasRecord = false;
				for(int i=0;i<jsonArray.length();i++){
					hasRecord = true;
					JSONObject newJson = new JSONObject(jsonArray.get(i).toString());
					Object key=newJson.get("key");
						if(counter+1 == (groupBy.length)){
						Map<String,Object> resultMap = new LinkedHashMap<String,Object>();
						for(Map.Entry<String,String> entry : metrics.entrySet()){
							if(newJson.has(entry.getValue())){
								resultMap.put(entry.getKey(), new JSONObject(newJson.get(entry.getValue()).toString()).get("value"));
								resultMap.put(groupBy[counter], newJson.get("key"));
							}
							}
						if(baseAPIService.checkNull(intermediateMap.get(key))){
						resultMap.putAll(intermediateMap.get(key));
						}
						data.add(resultMap);
					}else{
						JSONArray tempArray = new JSONArray();
						newJson = new JSONObject(newJson.get("field"+(counter+1)).toString());
						tempArray = new JSONArray(newJson.get("buckets").toString());
						for(int j=0;j<tempArray.length();j++){
							JSONObject subJson = new JSONObject(tempArray.get(j).toString());
								Map<String,Object> tempMap = new HashMap<String, Object>();
								if(intermediateMap.containsKey(key)){
									tempMap.putAll(intermediateMap.get(key));
									tempMap.put(groupBy[counter], key);
									intermediateMap.put(subJson.get("key"),tempMap);
								}else{
									tempMap.put(groupBy[counter], key);
									intermediateMap.put(subJson.get("key"), tempMap);
								}
							subJsonArray.put(tempArray.get(j));
						}
						hasSubAggregate = true;
					}
				}
				if(hasSubAggregate){
					json = new JSONObject();
					requestJSON.put("buckets", subJsonArray);
					json.put("field"+(counter+1), requestJSON);
				}
				
				if(!hasRecord){
					json = new JSONObject();	
				}
					}
				counter++;
				}
			} catch (JSONException e) {
				System.out.println("some logical problem in filter aggregate json ");
				e.printStackTrace();
			}
			System.out.println("data "+data);
			
			return data;
		}

		public List<Map<String,Object>> buildJSON(String[] groupBy,String resultData,Map<String,String> metrics,boolean hasFilter){

			List<Map<String,Object>> dataList = new ArrayList<Map<String,Object>>();
		       try {
		    		int counter=0;
					JSONObject json = new JSONObject(resultData);
					json = new JSONObject(json.get("aggregations").toString());
					if(hasFilter){
						json = new JSONObject(json.get("filters").toString());
					}
		           while(counter < groupBy.length){
		        	   if(json.length() > 0){
		               JSONObject requestJSON = new JSONObject(json.get("field"+counter).toString());
		           JSONArray jsonArray = new JSONArray(requestJSON.get("buckets").toString());
		           JSONArray subJsonArray = new JSONArray();
		           Set<Object> keys = new HashSet<Object>();
		           boolean hasSubAggregate = false;
		           for(int i=0;i<jsonArray.length();i++){
		               JSONObject newJson = new JSONObject(jsonArray.get(i).toString());
		               Object key=newJson.get("key");
		               keys.add(key);
		               if(counter+1 == (groupBy.length)){
		            	   Map<String,Object> resultMap = new LinkedHashMap<String,Object>();
		                   for(Map.Entry<String,String> entry : metrics.entrySet()){
		                       if(newJson.has(entry.getValue())){
		                           resultMap.put(entry.getKey(), new JSONObject(newJson.get(entry.getValue()).toString()).get("value"));
		                           resultMap.put(groupBy[counter], newJson.get("key"));
		                       newJson.remove(entry.getValue());    
		                       }
		                       }
		                   newJson.remove("doc_count");
		                   newJson.remove("key_as_string");
		                   newJson.remove("key");
		                   newJson.remove("buckets");
		                   Iterator<String> rowKeys = newJson.sortedKeys();
		                   while(rowKeys.hasNext()){
		                	   String rowKey = rowKeys.next();
		                	   resultMap.put(rowKey, newJson.get(rowKey));
		                   }
		                   dataList.add(resultMap);
		               }else{
		                   JSONArray tempArray = new JSONArray();
		                   JSONObject dataJson = new JSONObject(newJson.get("field"+(counter+1)).toString());
		                   tempArray = new JSONArray(dataJson.get("buckets").toString());
		                   hasSubAggregate = true;
		                   for(int j=0;j<tempArray.length();j++){
		                       JSONObject subJson = new JSONObject(tempArray.get(j).toString());
		                       subJson.put(groupBy[counter], key);
		                       newJson.remove("field"+(counter+1));
		                       newJson.remove("doc_count");
			                   newJson.remove("key_as_string");
			                   newJson.remove("key");
			                   newJson.remove("buckets");
		                       Iterator<String> dataKeys = newJson.sortedKeys();
		                       while(dataKeys.hasNext()){
		                       String dataKey = dataKeys.next();
		                       subJson.put(dataKey, newJson.get(dataKey));
		                        }

		                       subJsonArray.put(subJson);
		                   }
		               }
		           }
		           if(hasSubAggregate){
		               json = new JSONObject();
		               requestJSON.put("buckets", subJsonArray);
		               json.put("field"+(counter+1), requestJSON);
		           }
		        	   }
		           counter++;
		           }
		       } catch (JSONException e) {
		           System.out.println("some logical problem in filter aggregate json ");
		           e.printStackTrace();
		       }
		       return dataList;
		}
		
		  public List<Map<String,Object>> formDataList(Map<Integer,Map<String,Object>> requestMap){
		       List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		       for(Map.Entry<Integer,Map<String,Object>> entry : requestMap.entrySet()){
		           resultList.add(entry.getValue());
		       }
		       return resultList;
		   }
		public Map<Integer,Map<String,Object>> processAggregateJSON(String groupBy,String resultData,Map<String,String> metrics,boolean hasFilter){

			Map<Integer,Map<String,Object>> dataMap = new LinkedHashMap<Integer,Map<String,Object>>();
			try {
				int counter=0;
				String[] fields = groupBy.split(",");
				JSONObject json = new JSONObject(resultData);
				json = new JSONObject(json.get("aggregations").toString());
				if(hasFilter){
					json = new JSONObject(json.get("filters").toString());
				}
				while(counter < fields.length){
					JSONObject requestJSON = new JSONObject(json.get("field"+counter).toString());
				JSONArray jsonArray = new JSONArray(requestJSON.get("buckets").toString());
				JSONArray subJsonArray = new JSONArray();
				boolean hasSubAggregate = false;
				for(int i=0;i<jsonArray.length();i++){
					JSONObject newJson = new JSONObject(jsonArray.get(i).toString());
					Object key=newJson.get("key");
//					if(counter == (fields.length -1)){
						if(counter+1 == (fields.length)){
						Map<String,Object> resultMap = new LinkedHashMap<String,Object>();
						boolean processed = false;
						for(Map.Entry<String,String> entry : metrics.entrySet()){
							if(newJson.has(entry.getValue())){
								resultMap.put(entry.getKey(), new JSONObject(newJson.get(entry.getValue()).toString()).get("value"));
								resultMap.put(fields[counter], newJson.get("key"));
							}
							}
						if(baseAPIService.checkNull(dataMap)){
							if(dataMap.containsKey(i)){
								processed = true;
								Map<String,Object> tempMap = new LinkedHashMap<String,Object>();
								tempMap = dataMap.get(i);
								resultMap.putAll(tempMap);
								dataMap.put(i, resultMap);
							}
						}
						if(!processed){
							dataMap.put(i, resultMap);	
						}
							
					}else{
						JSONArray tempArray = new JSONArray();
						newJson = new JSONObject(newJson.get("field"+(counter+1)).toString());
						tempArray = new JSONArray(newJson.get("buckets").toString());
						for(int j=0;j<tempArray.length();j++){
							subJsonArray.put(tempArray.get(j));
						}
						Map<String,Object> tempMap = new LinkedHashMap<String,Object>();
						if(baseAPIService.checkNull(dataMap)){
							if(dataMap.containsKey(i)){
								tempMap = dataMap.get(i);
							}
						}
						tempMap.put(fields[counter], key);
						dataMap.put(i, tempMap);
						hasSubAggregate = true;
					}
				}
				if(hasSubAggregate){
					json = new JSONObject();
					requestJSON.put("buckets", subJsonArray);
					json.put("field"+(counter+1), requestJSON);
				}
				
				counter++;
				}
			} catch (JSONException e) {
				System.out.println("some logical problem in filter aggregate json ");
				e.printStackTrace();
			}
			System.out.println("dataMap "+dataMap);
			return dataMap;
		}
		
		public void includeOrder(RequestParamsDTO requestParamsDTO,Map<String,Boolean> validatedData,String fieldName,TermsBuilder termsBuilder,DateHistogramBuilder dateHistogramBuilder,Map<String,String> metricsName){
			
			if(validatedData.get(hasdata.HAS_SORTBY.check())){
				RequestParamsPaginationDTO pagination = requestParamsDTO.getPagination();
				List<RequestParamsSortDTO> orderDatas = pagination.getOrder();
				for(RequestParamsSortDTO orderData : orderDatas){
					if(termsBuilder != null){
						if(fieldName.equalsIgnoreCase(orderData.getSortBy())){
							if(orderData.getSortOrder().equalsIgnoreCase("DESC")){
								
								termsBuilder.order(org.elasticsearch.search.aggregations.bucket.terms.Terms.Order.term(false));
							}else{
								termsBuilder.order(org.elasticsearch.search.aggregations.bucket.terms.Terms.Order.term(true));	
							}
						}
						//sort the aggregate data
						sortAggregatedValue(termsBuilder,null, requestParamsDTO, orderData,metricsName);
					}else if(dateHistogramBuilder != null){
						if(fieldName.equalsIgnoreCase(orderData.getSortBy())){
							if(orderData.getSortOrder().equalsIgnoreCase("DESC")){
								
								dateHistogramBuilder.order(Order.KEY_DESC);
							}else{
								dateHistogramBuilder.order(Order.KEY_ASC);	
							}
						}
						sortAggregatedValue(null,dateHistogramBuilder, requestParamsDTO, orderData,metricsName);
					}
				}
			}
		}
		public List<Map<String,Object>> leftJoin(List<Map<String,Object>> parent,List<Map<String,Object>> child,Set<String> keys){
			List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
			for (Map<String, Object> parentEntry : parent) {
				boolean occured = false;
				Map<String, Object> appended = new HashMap<String, Object>();
				for (Map<String, Object> childEntry : child) {
					boolean validated = false;
					for(String key : keys){
						
					if (childEntry.containsKey(key) && parentEntry.containsKey(key)) {
						if (childEntry.get(key).toString().equals(parentEntry.get(key).toString())) {
						}else{
							
							validated = true;
						}
					}else{
						validated = true;
					}
					}
					if(!validated){
							occured = true;
							appended.putAll(childEntry);
							appended.putAll(parentEntry);
							break;
					}
				}
				if (!occured) {
					appended.putAll(parentEntry);
				}

				resultList.add(appended);
			}
			return resultList;
		}
		
		public List<Map<String, Object>> leftJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String parentKey, String childKey) {
			List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
			for (Map<String, Object> parentEntry : parent) {
				boolean occured = false;
				Map<String, Object> appended = new HashMap<String, Object>();
				for (Map<String, Object> childEntry : child) {
					if (childEntry.containsKey(childKey) && parentEntry.containsKey(parentKey)) {
						if (childEntry.get(childKey).equals(parentEntry.get(parentKey))) {
							occured = true;
							appended.putAll(childEntry);
							appended.putAll(parentEntry);
							break;
						}
					}
				}
				if (!occured) {
					appended.putAll(parentEntry);
				}

				resultList.add(appended);
			}
			return resultList;
		}
		
		public Map<String,Set<Object>> fetchFilters(String index,List<Map<String,Object>> dataList){
			Map<String,String> filterFields = new HashMap<String, String>();
			Map<String,Set<Object>> filters = new HashMap<String, Set<Object>>();
			if(baseConnectionService.getFieldsJoinCache().containsKey(index)){
				filterFields = baseConnectionService.getFieldsJoinCache().get(index);
			}
				for(Map<String,Object> dataMap : dataList){
					Set<String> keySets = filterFields.keySet();
					for(String keySet : keySets){
					for(String key : keySet.split(",")){
						if(dataMap.containsKey(key)){
							if(!filters.isEmpty() && filters.containsKey(key)){
								Set<Object> filterValue = filters.get(key);
								try{
									Object[] datas = (Object[]) dataMap.get(key);
									for(Object data : datas){
										filterValue.add(data);
									}
								}catch(Exception e){
									filterValue.add(dataMap.get(key));
								}						
								filters.put(key, filterValue);
							}else{
								Set<Object> filterValue = new HashSet<Object>();
								try{
									Object[] datas = (String[]) dataMap.get(key);
									for(Object data : datas){
										filterValue.add(data);
									}
								}catch(Exception e){
									
									filterValue.add(dataMap.get(key));
								}
								filters.put(key, filterValue);
							}
						}
					}
					}
				}
			
				return filters;
		}
		
		public JSONArray convertJSONArray(List<Map<String,Object>> data){
			try {
			Gson gson = new Gson();
			Type listType = new TypeToken<List<Map<String,Object>>>(){}.getType();
			JsonElement jsonArray = gson.toJsonTree(data, listType);
				return new JSONArray(jsonArray.toString());
			} catch (JSONException e) {
				e.printStackTrace();
				return new JSONArray();
			}
		}
		
		public List<Map<String,Object>> formatAggregateKeyValueJson(List<Map<String,Object>> dataMap,String key) throws org.json.JSONException{
			
			Map<String,List<Map<String,Object>>> resultMap = new LinkedHashMap<String, List<Map<String,Object>>>();
			List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
			for(Map<String,Object> map : dataMap){
				if(map.containsKey(key)){
					List<Map<String,Object>> tempList = new ArrayList<Map<String,Object>>();
					String jsonKey = map.get(key).toString();
					map.remove(key);
					if(resultMap.containsKey(jsonKey)){
						tempList.addAll(resultMap.get(jsonKey));
					}
						tempList.add(map);
					resultMap.put(jsonKey, tempList);
				}
			}
			for(Entry<String, List<Map<String, Object>>> entry : resultMap.entrySet()){
				Map<String,Object> tempMap = new HashMap<String, Object>();
				tempMap.put(entry.getKey(), entry.getValue());
				resultList.add(tempMap);
			}
			return resultList;
		}
		
		public JSONArray buildAggregateJSON(List<Map<String,Object>> resultList) throws JSONException{
			Gson gson = new Gson();
			String resultArray = gson.toJson(resultList,resultList.getClass());
			return new JSONArray(resultArray);
		}
		
		public List<Map<String,Object>> customPaginate(RequestParamsPaginationDTO requestParamsPaginationDTO,List<Map<String,Object>> data,Map<String,Boolean> validatedData,Map<String,Object> returnMap){
			int dataSize = data.size();
			List<Map<String,Object>> customizedData = new ArrayList<Map<String,Object>>();
			if(baseAPIService.checkNull(requestParamsPaginationDTO)){
				if(validatedData.get(hasdata.HAS_SORTBY.check())){
					List<RequestParamsSortDTO> orderDatas = requestParamsPaginationDTO.getOrder();
					for(RequestParamsSortDTO sortData : orderDatas){
						baseAPIService.sortBy(data, sortData.getSortBy(), sortData.getSortOrder());
					}
				}
				int offset = validatedData.get(hasdata.HAS_Offset.check()) ? requestParamsPaginationDTO.getOffset() == 0 ? 0 : requestParamsPaginationDTO.getOffset() -1 : 0; 
				int limit = validatedData.get(hasdata.HAS_LIMIT.check()) ? requestParamsPaginationDTO.getLimit() == 0 ? 1 : requestParamsPaginationDTO.getLimit() : 10; 
				
				if(limit < dataSize && offset < dataSize){
					customizedData = data.subList(offset, limit);
				}else if(limit >= dataSize &&  offset < dataSize){
					customizedData = data.subList(offset, dataSize);
				}else if(limit < dataSize &&  offset >= dataSize){
					customizedData = data.subList(0,limit);
				}else if(limit >= dataSize &&  offset >= dataSize){
					customizedData = data.subList(0,dataSize);
				}
			}else{
				customizedData = data;
			}
			returnMap.put("totalRows",customizedData.size());
			return customizedData;
		}
		
		public List<Map<String,Object>> aggregatePaginate(RequestParamsPaginationDTO requestParamsPaginationDTO,List<Map<String,Object>> data,Map<String,Boolean> validatedData,Map<String,Object> returnMap){
			int dataSize = data.size();
			List<Map<String,Object>> customizedData = new ArrayList<Map<String,Object>>();
			if(baseAPIService.checkNull(requestParamsPaginationDTO)){
				int offset = validatedData.get(hasdata.HAS_Offset.check()) ? requestParamsPaginationDTO.getOffset() : 0; 
				int limit = validatedData.get(hasdata.HAS_LIMIT.check()) ? requestParamsPaginationDTO.getLimit() : 10; 
				
				if(limit < dataSize && offset < dataSize){
					customizedData = data.subList(offset, limit);
				}else if(limit >= dataSize &&  offset < dataSize){
					customizedData = data.subList(offset, dataSize);
				}else if(limit < dataSize &&  offset >= dataSize){
					customizedData = data.subList(0,limit);
				}else if(limit >= dataSize &&  offset >= dataSize){
					customizedData = data.subList(0,dataSize);
				}
			}else{
				customizedData = data;
			}
			returnMap.put("totalRows",customizedData.size());
			return customizedData;
		}
		
		public List<Map<String,Object>> aggregateSortBy(RequestParamsPaginationDTO requestParamsPaginationDTO,List<Map<String,Object>> data,Map<String,Boolean> validatedData,Map<String,Object> returnMap){
			if(baseAPIService.checkNull(requestParamsPaginationDTO)){
				if(validatedData.get(hasdata.HAS_SORTBY.check())){
					List<RequestParamsSortDTO> orderDatas = requestParamsPaginationDTO.getOrder();
					for(RequestParamsSortDTO sortData : orderDatas){
						baseAPIService.sortBy(data, sortData.getSortBy(), sortData.getSortOrder());
					}
				}
				
			}	
//			aggregatePaginate(requestParamsPaginationDTO, data, validatedData, returnMap);
			return data;
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
		

		public List<Map<String,Object>> getRecords(String[] indices,String data,Map<Integer,String> errorRecord,String dataKey){
			JSONObject json;
			JSONArray jsonArray = new JSONArray();
			List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
			try {
				json = new JSONObject(data);
				json = new JSONObject(json.get("hits").toString());
				jsonArray = new JSONArray(json.get("hits").toString());
				if(!dataKey.equalsIgnoreCase("fields")){
				for(int i =0;i< jsonArray.length();i++){
					json = new JSONObject(jsonArray.get(i).toString());
					JSONObject fieldJson = new JSONObject(json.get(dataKey).toString());
					
					Iterator<String> keys = fieldJson.keys();
					Map<String,Object> resultMap = new HashMap<String, Object>();
					while(keys.hasNext()){
						String key =keys.next();
						if(baseConnectionService.getDependentFieldsCache().containsKey(indices[0])){
							Map<String,Map<String,String>> dependentKey = baseConnectionService.getDependentFieldsCache().get(indices[0]);	
							if(dependentKey.containsKey(key)){
								Map<String,String> dependentColumn = dependentKey.get(key);
								Set<String> columnKeys = dependentColumn.keySet();
								for(String columnKey : columnKeys){
									if(!columnKey.equalsIgnoreCase("field_name") && !columnKey.equalsIgnoreCase("dependent_name")){
										if(columnKey.equalsIgnoreCase(fieldJson.getString(dependentColumn.get("dependent_name")))){
											try{
												JSONArray dataArray = new JSONArray(json.get(key).toString());
												if(dataArray.length() == 1){	 
													resultMap.put(dependentColumn.get(columnKey),dataArray.get(0));
												}else{
													Set<Object> arrayData = new HashSet<Object>();
													for(int j=0;j < dataArray.length();j++){
														arrayData.add(dataArray.get(j));
													}
													resultMap.put(dependentColumn.get(columnKey),arrayData);
												}
											}catch(Exception e){
												
												resultMap.put(dependentColumn.get(columnKey), json.get(key));		
											}
										}
									}
								}
								
							}
						}else{
							try{
								JSONArray dataArray = new JSONArray(fieldJson.get(key).toString());
								if(dataArray.length() == 1){	 
									resultMap.put(apiFields(indices[0],key),dataArray.get(0));
								}else{
									Object[] arrayData = new Object[dataArray.length()];
									for(int j=0;j < dataArray.length();j++){
										arrayData[j]=dataArray.get(j);
									}
									resultMap.put(apiFields(indices[0],key),arrayData);
								}
							}catch(Exception e){
								
								resultMap.put(apiFields(indices[0],key), fieldJson.get(key));
							}
						}
					}
					resultList.add(resultMap);
				}
				}else{
					for(int i =0;i< jsonArray.length();i++){
						Map<String,Object> resultMap = new HashMap<String, Object>();
					json = new JSONObject(jsonArray.get(i).toString());
					json = new JSONObject(json.get(dataKey).toString());
					 Iterator<String> keys =json.keys();
					 while(keys.hasNext()){
						 String key = keys.next();
						 if(baseConnectionService.getDependentFieldsCache().containsKey(indices[0])){
								Map<String,Map<String,String>> dependentKey = baseConnectionService.getDependentFieldsCache().get(indices[0]);	
								
								if(dependentKey.containsKey(key)){
									Map<String,String> dependentColumn = dependentKey.get(key);
									Set<String> columnKeys = dependentColumn.keySet();
									for(String columnKey : columnKeys){
										if(!columnKey.equalsIgnoreCase("field_name") && !columnKey.equalsIgnoreCase("dependent_name")){
											if(columnKey.equalsIgnoreCase(new JSONArray(json.getString(dependentColumn.get("dependent_name"))).getString(0))){
												try{
													JSONArray dataArray = new JSONArray(json.get(key).toString());
													if(dataArray.length() == 1){	 
														resultMap.put(dependentColumn.get(columnKey),dataArray.get(0));
													}else{
														Object[] arrayData = new Object[dataArray.length()];
														for(int j=0;j < dataArray.length();j++){
															arrayData[j]=dataArray.get(j);
														}
														resultMap.put(dependentColumn.get(columnKey),arrayData);
													}
												}catch(Exception e){
													
													resultMap.put(dependentColumn.get(columnKey), json.get(key));		
												}
											}
										}
									} 
									
								}
							}else{
								try{
						 JSONArray fieldJsonArray = new JSONArray(json.get(key).toString());
						if(fieldJsonArray.length() == 1){	 
							resultMap.put(apiFields(indices[0],key),fieldJsonArray.get(0));
						}else{
							Set<Object> arrayData = new HashSet<Object>();
							for(int j=0;j < fieldJsonArray.length();j++){
								arrayData.add(fieldJsonArray.get(j));
							}
							if(arrayData.size() == 1){
								for(Object dataObject : arrayData){
									resultMap.put(apiFields(indices[0],key),dataObject);
									
								}
							}else{
								
								resultMap.put(apiFields(indices[0],key),arrayData);
							}
						}
								}catch(Exception e){
									resultMap.put(apiFields(indices[0],key), json.get(key));
								}
					 }
					 }
					 resultList.add(resultMap);
				}
				}
				return resultList;
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return resultList;
		}
		
		public List<Map<String,Object>> getMultiGetRecords(String[] indices,Map<String,Map<String,String>> comparekey,String data,Map<Integer,String> errorRecord,String dataKey){
			JSONObject json;
			JSONArray jsonArray = new JSONArray();
			List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
			try {
				json = new JSONObject(data);
				json = new JSONObject(json.get("hits").toString());
				jsonArray = new JSONArray(json.get("hits").toString());
				if(!dataKey.equalsIgnoreCase("fields")){
				for(int i =0;i< jsonArray.length();i++){
					json = new JSONObject(jsonArray.get(i).toString());
					JSONObject fieldJson = new JSONObject(json.get(dataKey).toString());
					
					Iterator<String> keys = fieldJson.keys();
					Map<String,Object> resultMap = new HashMap<String, Object>();
					while(keys.hasNext()){
						String key =keys.next(); 
						if(comparekey.containsKey(key)){
							Map<String,String> comparable = new HashMap<String, String>();
							comparable = comparekey.get(key);
							for(Map.Entry<String, String> compare : comparable.entrySet()){
								
								if(compare.getKey().equalsIgnoreCase(fieldJson.get(key).toString()))
										resultMap.put(compare.getValue(), fieldJson.get(key));
							}
						}else{
						resultMap.put(apiFields(indices[0],key), fieldJson.get(key));
						}
					}
					resultList.add(resultMap);
				}
				}else{
					for(int i =0;i< jsonArray.length();i++){
						Map<String,Object> resultMap = new HashMap<String, Object>();
					json = new JSONObject(jsonArray.get(i).toString());
					json = new JSONObject(json.get(dataKey).toString());
					 Iterator<String> keys =json.keys();
					 while(keys.hasNext()){
						 String key = keys.next();
						 JSONArray fieldJsonArray = new JSONArray(json.get(key).toString());
						if(fieldJsonArray.length() == 1){	 
							if(comparekey.containsKey(key)){
								Map<String,String> comparable = new HashMap<String, String>();
								comparable = comparekey.get(key);
								for(Map.Entry<String, String> compare : comparable.entrySet()){
									
									if(compare.getKey().equalsIgnoreCase(fieldJsonArray.get(0).toString()))
											resultMap.put(compare.getValue(), fieldJsonArray.getString(0));
								}
							}else{
							resultMap.put(apiFields(indices[0],key),fieldJsonArray.get(0));
							}
						}else{
							if(comparekey.containsKey(key)){
								Map<String,String> comparable = new HashMap<String, String>();
								comparable = comparekey.get(key);
								for(Map.Entry<String, String> compare : comparable.entrySet()){
									
									if(compare.getKey().equalsIgnoreCase(fieldJsonArray.get(0).toString()))
											resultMap.put(compare.getValue(), fieldJsonArray);
								}
							}else{
							resultMap.put(apiFields(indices[0],key),fieldJsonArray);
							}
						}
					 }
					 resultList.add(resultMap);
				}
				}
				return resultList;
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return resultList;
		}

		public String apiFields(String index,String fields){
			Map<String,String> mappingfields = baseConnectionService.getFields().get(index);
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

 
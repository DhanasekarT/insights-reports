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
			String[] indices, String[] types, String fields,
			QueryBuilder query, FilterBuilder filters, Integer offset,
			Integer limit, Map<String, String> sort,Map<String,Boolean> validatedData,Map<String,String> dataRecord,Map<Integer,String> errorRecord) {
		SearchRequestBuilder searchRequestBuilder = getClient().prepareSearch(
				indices).setSearchType(SearchType.DFS_QUERY_AND_FETCH);
		QueryBuilder queryBuilder = null;
		FilterBuilder filterBuilder = null;
		Map<String,String> metricsName = new HashMap<String,String>();
		boolean aggregate = false;
		String result ="[{}]";
		fields = esFields(fields);
		
		String dataKey=esSources.SOURCE.esSource();
		

		if(validatedData.get(hasdata.HAS_FEILDS.check())){
			dataKey=esSources.FIELDS.esSource();
		}
		
		if (validatedData.get(hasdata.HAS_FEILDS.check())) {
			for (String field : fields.split(",")) {
				searchRequestBuilder.addField(field);
			}
		}
		
		// Add filter in Query
		addFilters(requestParamsDTO.getFilter(), searchRequestBuilder);
		
		
		if (validatedData.get(hasdata.HAS_GRANULARITY.check()) && validatedData.get(hasdata.HAS_GROUPBY.check())) {
			updatedService.granularityAggregate(requestParamsDTO, searchRequestBuilder,metricsName);
			aggregate = true;
			} 
		
		if (!validatedData.get(hasdata.HAS_GRANULARITY.check()) && validatedData.get(hasdata.HAS_GROUPBY.check())) {
			updatedService.aggregate(requestParamsDTO, searchRequestBuilder,metricsName);
			aggregate = true;
		}
		
		sortData(sort, searchRequestBuilder);
		
		 searchRequestBuilder.setPreference("_primaries");
		 searchRequestBuilder.setSize(1000);

		paginate(offset, limit, searchRequestBuilder);
		System.out.println("query \n"+searchRequestBuilder);
		try{
		result =  searchRequestBuilder.execute().actionGet().toString();
		}catch(Exception e){
			e.printStackTrace();
			errorRecord.put(500, "please contact the developer team for knowing about the error details.");
		}
		System.out.println("result "+result);
		if(aggregate){
		try {
			String groupBy[] = requestParamsDTO.getGroupBy().split(",");
			return baseAPIService.formatKeyValueJson(formDataList(updatedService.processAggregateJSON(requestParamsDTO.getGroupBy(), result, metricsName, validatedData.get(hasdata.HAS_FILTER.check()))),groupBy[groupBy.length]);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		}
		
		return getRecords(result,dataRecord,errorRecord,dataKey);

	}
	
	public boolean aggregateFunction(RequestParamsDTO requestParamsDTO,
			SearchRequestBuilder searchRequestBuilder) {
		TermsBuilder termBuilder = null;
		TermsBuilder aggregateBuilder = null;
		boolean includedAggregate = false;
		boolean firstEntry = false;
		if (requestParamsDTO.getGroupBy() != null) {
			try {
				String[] groupBy = requestParamsDTO.getGroupBy().split(",");
				for (int j = groupBy.length - 1; j >= 0; j--) {

				String firstField = esFields(groupBy[0]);
					if (j == 0 && groupBy.length > 1) {
						termBuilder = new TermsBuilder(firstField)
								.field(firstField);
						continue;
					} else {
						termBuilder = new TermsBuilder(firstField)
								.field(firstField);
					}
					
					String currentField = esFields(groupBy[j]);
					TermsBuilder subTermBuilder = new TermsBuilder(currentField)
							.field(currentField);
					subTermBuilder.size(1000);
					
					if (j == groupBy.length - 2) {
						if (includedAggregate) {
							subTermBuilder.subAggregation(aggregateBuilder);
							includedAggregate = true;
						}
					}
					if (!firstEntry) {
						if (!requestParamsDTO.getAggregations().isEmpty()) {
							Gson gson = new Gson();
							String requestJsonArray = gson
									.toJson(requestParamsDTO.getAggregations());
							JSONArray jsonArray = new JSONArray(
									requestJsonArray);
							for (int i = 0; i < jsonArray.length(); i++) {
								JSONObject jsonObject;
								jsonObject = new JSONObject(jsonArray.get(i)
										.toString());
								if (!jsonObject.has("operator")
										&& !jsonObject.has("formula")
										&& !jsonObject.has("requestValues")) {
									continue;
								}
								if (jsonObject.get("operator").toString()
										.equalsIgnoreCase("es")) {
									if (baseAPIService.checkNull(jsonObject
											.get("formula"))) {
										if (jsonObject.get("formula")
												.toString()
												.equalsIgnoreCase("sum")) {
											String requestValues = jsonObject
													.get("requestValues")
													.toString();
											for (String aggregateName : requestValues
													.split(",")) {
												if (!jsonObject
														.has(aggregateName)) {
													continue;
												}
											performAggregation(jsonObject,jsonObject.getString("formula"), aggregateName, subTermBuilder);
											}

										}
									}
								}
							}
						}
						firstEntry = true;
					}
					aggregateBuilder = subTermBuilder;
					includedAggregate = true;
				}
				termBuilder.subAggregation(aggregateBuilder);
				if(groupBy.length == 1){
					termBuilder = aggregateBuilder;
				}
				termBuilder.size(1000);
				searchRequestBuilder.addAggregation(termBuilder);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return true;
		}
		return false;
	}
	public boolean filterAggregateFunction(RequestParamsDTO requestParamsDTO,
			SearchRequestBuilder searchRequestBuilder,Map<String,String> metricsName,Map<String,Boolean> validatedData) {
		TermsBuilder termBuilder = null;
		TermsBuilder aggregateBuilder = null;
		boolean includedAggregate = false;
		boolean firstEntry = false;
		boolean singleAggregate = false;
		if (validatedData.get(hasdata.HAS_GROUPBY.check())) {
			try {
				String[] groupBy = requestParamsDTO.getGroupBy().split(",");
				for (int j = groupBy.length - 1; j >= 0; j--) {

					String firstField = esFields(groupBy[0]);
					if (j == 0 && groupBy.length > 1) {
						termBuilder = new TermsBuilder(firstField)
								.field(firstField);
						continue;
					} else {
						termBuilder = new TermsBuilder(firstField)
								.field(firstField);
					}
					if(groupBy.length == 1){
						singleAggregate = true;
						
					}
					String currentField = esFields(groupBy[j]);
					TermsBuilder subTermBuilder = new TermsBuilder(currentField)
							.field(currentField);
					subTermBuilder.size(1000);
					if (j == groupBy.length - 2) {
						if (includedAggregate) {
							subTermBuilder.subAggregation(aggregateBuilder);
							includedAggregate = true;
						}
					}
					if (!firstEntry) {
						if (!requestParamsDTO.getAggregations().isEmpty()) {
							Gson gson = new Gson();
							String requestJsonArray = gson
									.toJson(requestParamsDTO.getAggregations());
							JSONArray jsonArray = new JSONArray(
									requestJsonArray);
							FilterAggregationBuilder mainFilter = addFilters(requestParamsDTO
									.getFilter());
							
							includeFilterAggregation(requestParamsDTO, jsonArray, singleAggregate, termBuilder, subTermBuilder, metricsName,mainFilter);
							termBuilder.order(org.elasticsearch.search.aggregations.bucket.terms.Terms.Order.term(false));
						}
						firstEntry = true;
					}
					if (singleAggregate) {
						aggregateBuilder = termBuilder;

					} else {
						aggregateBuilder = subTermBuilder;
						includedAggregate = true;
					}
				}
				if (singleAggregate) {
					termBuilder = aggregateBuilder;
				} else {
					termBuilder.subAggregation(aggregateBuilder);
				}
				termBuilder.size(1000);
				searchRequestBuilder.addAggregation(termBuilder);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return true;
		}
		return false;
	}

	public boolean aggregateFunction(RequestParamsDTO requestParamsDTO,
			SearchRequestBuilder searchRequestBuilder,Map<String,String> metricsName,Map<String,Boolean> validatedData) {
		TermsBuilder termBuilder = null;
		TermsBuilder aggregateBuilder = null;
		boolean includedAggregate = false;
		boolean firstEntry = false;
		boolean singleAggregate = false;
		if (validatedData.get(hasdata.HAS_GROUPBY.check())) {
			try {
				String[] groupBy = requestParamsDTO.getGroupBy().split(",");
				for (int j = groupBy.length - 1; j >= 0; j--) {
					
					String firstField = esFields(groupBy[0]);
					if (j == 0 && groupBy.length > 1) {
						termBuilder = new TermsBuilder(firstField)
								.field(firstField);
						continue;
					} else {
						termBuilder = new TermsBuilder(firstField)
								.field(firstField);
					}
					if(groupBy.length == 1){
						singleAggregate = true;
					}
					
					String currentField = esFields(groupBy[j]);
					TermsBuilder subTermBuilder = new TermsBuilder(currentField)
							.field(currentField);
					subTermBuilder.size(1000);
					if (j == groupBy.length - 2) {
						if (includedAggregate) {
							subTermBuilder.subAggregation(aggregateBuilder);
							includedAggregate = true;
						}
					}
					if (!firstEntry) {
						if (!requestParamsDTO.getAggregations().isEmpty()) {
							Gson gson = new Gson();
							String requestJsonArray = gson
									.toJson(requestParamsDTO.getAggregations());
							JSONArray jsonArray = new JSONArray(
									requestJsonArray);
							FilterAggregationBuilder mainFilter = addFilters(requestParamsDTO
									.getFilter());
							
							includeFilterAggregation(requestParamsDTO, jsonArray, singleAggregate, termBuilder, subTermBuilder, metricsName,mainFilter);
							termBuilder.order(org.elasticsearch.search.aggregations.bucket.terms.Terms.Order.term(false));
						}
						firstEntry = true;
					}
					if (singleAggregate) {
						aggregateBuilder = termBuilder;

					} else {
						aggregateBuilder = subTermBuilder;
						includedAggregate = true;
					}
				}
				if (singleAggregate) {
					termBuilder = aggregateBuilder;
				} else {
					termBuilder.subAggregation(aggregateBuilder);
				}
				termBuilder.size(1000);
				searchRequestBuilder.addAggregation(termBuilder);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return true;
		}
		return false;
	}

	public boolean granularityFAFunction(RequestParamsDTO requestParamsDTO,
			SearchRequestBuilder searchRequestBuilder,Map<String,String> metricsName,Map<String,Boolean> validatedData) {
		TermsBuilder termBuilder = null;
		boolean firstEntry = false;
		boolean singleAggregate = false;
		FilterAggregationBuilder mainFilter = null;
		List<TermsBuilder> termsBuilders = new ArrayList<TermsBuilder>();
		DateHistogramBuilder histogramBuilder = null;
		if (validatedData.get(hasdata.HAS_GROUPBY.check())) {
			try {
				JSONArray jsonArray = new JSONArray();
				String[] groupBy = requestParamsDTO.getGroupBy().split(",");
				for (int j = 0; j <groupBy.length; j++) {
					
					//db field name
					String field = esFields(groupBy[j]);

					if (!firstEntry) {
						mainFilter = addFilters(requestParamsDTO
								.getFilter());
						if (!requestParamsDTO.getAggregations().isEmpty()) {
							Gson gson = new Gson();
							String requestJsonArray = gson
									.toJson(requestParamsDTO.getAggregations());
							jsonArray = new JSONArray(
									requestJsonArray);
							histogramBuilder = dateHistogram(requestParamsDTO.getGranularity(),field);
							if(groupBy.length == 1){	
								includeFilterAggregation(requestParamsDTO, jsonArray, singleAggregate, null, null, metricsName,mainFilter);
							singleAggregate = true;
							}
						}
						firstEntry = true;
					}else{
						termsBuilders.add(AggregationBuilders.terms(field).field(field));
					}
				}
				for(int i=termsBuilders.size()-1; i >= 0 ;i--){
					if(termBuilder == null){
						termBuilder = termsBuilders.get(i);
						termBuilder.size(1000);
						if(!singleAggregate){
							includeFilterAggregation(requestParamsDTO, jsonArray, !singleAggregate, null, null, metricsName,mainFilter);
							termBuilder.subAggregation(mainFilter);
						}
					}else{
						TermsBuilder tempBuilder = null;
						tempBuilder = termsBuilders.get(i);
						tempBuilder.size(1000);
						tempBuilder.subAggregation(termBuilder);
						termBuilder = tempBuilder;
					}
				}
				if(termBuilder != null){
					if(singleAggregate){
					mainFilter.subAggregation(termBuilder);
					}
				}
				if(mainFilter !=null){
					if(singleAggregate){
						
						histogramBuilder.subAggregation(mainFilter);
					}else{
						histogramBuilder.subAggregation(termBuilder);
					}
					histogramBuilder.minDocCount(1000);
					histogramBuilder.order(org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order.KEY_ASC);
					searchRequestBuilder.addAggregation(histogramBuilder);
				}
				System.out.println("search request "+searchRequestBuilder);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return true;
		}
		return false;
	}

	public boolean granularityAFunction(RequestParamsDTO requestParamsDTO,
			SearchRequestBuilder searchRequestBuilder,Map<String,String> metricsName,Map<String,Boolean> validatedData) {
		TermsBuilder termBuilder = null;
		boolean firstEntry = false;
		boolean singleAggregate = false;
		List<TermsBuilder> termsBuilders = new ArrayList<TermsBuilder>();
		DateHistogramBuilder histogramBuilder = null;
		if (validatedData.get(hasdata.HAS_GROUPBY.check())) {
			try {
				JSONArray jsonArray = new JSONArray();
				String[] groupBy = requestParamsDTO.getGroupBy().split(",");
				for (int j = 0; j <groupBy.length; j++) {
					
					String field = esFields(groupBy[j]);
					if (!firstEntry) {
						if (!requestParamsDTO.getAggregations().isEmpty()) {
							Gson gson = new Gson();
							String requestJsonArray = gson
									.toJson(requestParamsDTO.getAggregations());
							jsonArray = new JSONArray(
									requestJsonArray);
							histogramBuilder = dateHistogram(requestParamsDTO.getGranularity(),field);
							if(groupBy.length == 1){	
							includeAggregation(requestParamsDTO, jsonArray,!singleAggregate,histogramBuilder,null, metricsName);
							singleAggregate = true;
							}
						}
						firstEntry = true;
					}else{
						termsBuilders.add(AggregationBuilders.terms(field).field(field));
					}
				}
				for(int i=termsBuilders.size()-1; i >= 0 ;i--){
					if(termBuilder == null){
						termBuilder = termsBuilders.get(i);
						termBuilder.size(1000);
						if(!singleAggregate){
							includeAggregation(requestParamsDTO, jsonArray,singleAggregate, null,termBuilder,metricsName);
						}
					}else{
						TermsBuilder tempBuilder = null;
						tempBuilder = termsBuilders.get(i);
						tempBuilder.size(1000);
						tempBuilder.subAggregation(termBuilder);
						termBuilder = tempBuilder;
					}
				}
					if(!singleAggregate){
						
						histogramBuilder.subAggregation(termBuilder);
					}
					histogramBuilder.minDocCount(1000);
					histogramBuilder.order(org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order.KEY_ASC);
					searchRequestBuilder.addAggregation(histogramBuilder);
				System.out.println("search request "+searchRequestBuilder);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return true;
		}
		return false;
	}

	public void includeFilterAggregation(RequestParamsDTO requestParamsDTO,JSONArray jsonArray,boolean singleAggregate,TermsBuilder termBuilder,TermsBuilder subTermBuilder,Map<String,String> metricsName,FilterAggregationBuilder mainFilter){
		try {
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
						for (String aggregateName : requestValues
								.split(",")) {
							if (!jsonObject
									.has(aggregateName)) {
								continue;
							}
							if (singleAggregate) {
								performFilterAggregation(mainFilter,jsonObject, jsonObject.get("formula")
										.toString(), aggregateName);
							} else {
								performFilterAggregation(mainFilter,jsonObject, jsonObject.get("formula")
										.toString(), aggregateName);
							}
							String fieldName = esFields(jsonObject.get(aggregateName).toString());
							metricsName.put(jsonObject.getString("name") != null ? jsonObject.getString("name") : fieldName, fieldName);

					}
				}
			}
						if(singleAggregate){
							if(termBuilder != null){
							termBuilder.subAggregation(mainFilter);
							}
						}else{
							if(subTermBuilder != null){
							subTermBuilder.subAggregation(mainFilter);
						}
						}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
	
	public void includeAggregation(RequestParamsDTO requestParamsDTO,JSONArray jsonArray,boolean singleAggregate,DateHistogramBuilder dateHistogramBuilder,TermsBuilder termBuilder,Map<String,String> metricsName){
		try {
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
						for (String aggregateName : requestValues
								.split(",")) {
							if (!jsonObject
									.has(aggregateName)) {
								continue;
							}
							if(singleAggregate){
								performAggregation(jsonObject, jsonObject.get("formula")
										.toString(), aggregateName,dateHistogramBuilder);
							}else{
								performAggregation(jsonObject, jsonObject.get("formula")
										.toString(), aggregateName,termBuilder);
							}
							String fieldName = esFields(jsonObject.get(aggregateName).toString());
								metricsName.put(jsonObject.getString("name") != null ? jsonObject.getString("name") : fieldName, fieldName);

					}
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
	
	public void performFilterAggregation(FilterAggregationBuilder mainFilter,JSONObject jsonObject,String aggregateType,String aggregateName){
		try {
			String esAggregateName= esFields(jsonObject.get(aggregateName).toString());
			if("SUM".equalsIgnoreCase(aggregateType)){
			mainFilter
			.subAggregation(AggregationBuilders
					.sum(esAggregateName)
					.field(esAggregateName));
			}else if("AVG".equalsIgnoreCase(aggregateType)){
				mainFilter
				.subAggregation(AggregationBuilders.avg(esAggregateName).field(esAggregateName));
			}else if("MAX".equalsIgnoreCase(aggregateType)){
				mainFilter
				.subAggregation(AggregationBuilders.max(esAggregateName).field(esAggregateName));
			}else if("MIN".equalsIgnoreCase(aggregateType)){
				mainFilter
				.subAggregation(AggregationBuilders.min(esAggregateName).field(esAggregateName));
				
			}else if("COUNT".equalsIgnoreCase(aggregateType)){
				mainFilter
				.subAggregation(AggregationBuilders.count(esAggregateName).field(esAggregateName));
			}else if("DISTINCT".equalsIgnoreCase(aggregateType)){
				mainFilter
				.subAggregation(AggregationBuilders.cardinality(esAggregateName).field(esAggregateName));
			}
	
		} catch (JSONException e) {
			e.printStackTrace();
		} 
		}
	
	public void performAggregation(JSONObject jsonObject,String aggregateType,String aggregateName,TermsBuilder termBuilder){
		try {
			String esAggregateName= esFields(jsonObject.get(aggregateName).toString());
			if("SUM".equalsIgnoreCase(aggregateType)){
				termBuilder
			.subAggregation(AggregationBuilders
					.sum(esAggregateName)
					.field(esAggregateName));
			}else if("AVG".equalsIgnoreCase(aggregateType)){
				termBuilder
				.subAggregation(AggregationBuilders.avg(esAggregateName).field(esAggregateName));
			}else if("MAX".equalsIgnoreCase(aggregateType)){
				termBuilder
				.subAggregation(AggregationBuilders.max(esAggregateName).field(esAggregateName));
			}else if("MIN".equalsIgnoreCase(aggregateType)){
				termBuilder
				.subAggregation(AggregationBuilders.min(esAggregateName).field(esAggregateName));
				
			}else if("COUNT".equalsIgnoreCase(aggregateType)){
				termBuilder
				.subAggregation(AggregationBuilders.count(esAggregateName).field(esAggregateName));
			}else if("DISTINCT".equalsIgnoreCase(aggregateType)){
				termBuilder
				.subAggregation(AggregationBuilders.cardinality(esAggregateName).field(esAggregateName));
			}
	
		} catch (JSONException e) {
			e.printStackTrace();
		}
		}
	
	public void performAggregation(JSONObject jsonObject,String aggregateType,String aggregateName,DateHistogramBuilder dateHistogram){
		try {
			String esAggregateName= esFields(jsonObject.get(aggregateName).toString());
			if("SUM".equalsIgnoreCase(aggregateType)){
				dateHistogram
			.subAggregation(AggregationBuilders
					.sum(esAggregateName)
					.field(esAggregateName));
			}else if("AVG".equalsIgnoreCase(aggregateType)){
				dateHistogram
				.subAggregation(AggregationBuilders.avg(esAggregateName).field(esAggregateName));
			}else if("MAX".equalsIgnoreCase(aggregateType)){
				dateHistogram
				.subAggregation(AggregationBuilders.max(esAggregateName).field(esAggregateName));
			}else if("MIN".equalsIgnoreCase(aggregateType)){
				dateHistogram
				.subAggregation(AggregationBuilders.min(esAggregateName).field(esAggregateName));
				
			}else if("COUNT".equalsIgnoreCase(aggregateType)){
				dateHistogram
				.subAggregation(AggregationBuilders.count(esAggregateName).field(esAggregateName));
			}else if("DISTINCT".equalsIgnoreCase(aggregateType)){
				dateHistogram
				.subAggregation(AggregationBuilders.cardinality(esAggregateName).field(esAggregateName));
			}
	
		} catch (JSONException e) {
			e.printStackTrace();
		}
		}
	
	public DateHistogramBuilder dateHistogram(String granularity,String field){
		
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
				
				DateHistogramBuilder dateHistogram = AggregationBuilders.dateHistogram(field).field(field).interval(interval).format(format);
				return dateHistogram;
		}
			return null;
	}
	public boolean duplicateFilterAggregateFunction(
			RequestParamsDTO requestParamsDTO,
			SearchRequestBuilder searchRequestBuilder,Map<String,Boolean> validateData) {
		TermsBuilder termBuilder = null;
		FilterAggregationBuilder subAggregateFilter = null;
		TermsBuilder subTermBuilder = null;
		System.out.println("entered main loop "+searchRequestBuilder);
		if (validateData.get(hasdata.HAS_GROUPBY.check())) {
			System.out.println("has group By");
			try {
				String[] groupBy = requestParamsDTO.getGroupBy().split(",");
				boolean firstFilter = true;
				boolean isTermBuilder = true;
				for (int j =0;j<groupBy.length;j++) {
					
					FilterAggregationBuilder aggregateFilter = null;
					aggregateFilter = duplicateFilters(requestParamsDTO.getFilter(),groupBy,groupBy[j],firstFilter);
					if(j == 0){
					termBuilder = new TermsBuilder(groupBy[0])
						.field(groupBy[0]);
						firstFilter = false;
//					termBuilder.subAggregation(aggregateFilter);
						if(aggregateFilter != null){
							isTermBuilder = false;
							subAggregateFilter = aggregateFilter;
						}
					}else{
					subTermBuilder = new TermsBuilder(groupBy[j])
					.field(groupBy[j]);
					}
					if(j == 0 && groupBy.length > 1){
					continue;
					}
					if(j != groupBy.length-1){
						if(aggregateFilter != null){
							subTermBuilder.subAggregation(aggregateFilter);
						}
						if(subAggregateFilter != null){
							subAggregateFilter.subAggregation(subTermBuilder);
						}
					}
					if(j == groupBy.length-1){
						if (!requestParamsDTO.getAggregations().isEmpty()) {
							Gson gson = new Gson();
							String requestJsonArray = gson
									.toJson(requestParamsDTO.getAggregations());
							JSONArray jsonArray = new JSONArray(
									requestJsonArray);
							for (int i = 0; i < jsonArray.length(); i++) {
								JSONObject jsonObject;
								jsonObject = new JSONObject(jsonArray.get(i)
										.toString());
								if (!jsonObject.has("operator")
										&& !jsonObject.has("formula")
										&& !jsonObject.has("requestValues")) {
									continue;
								}
								if (jsonObject.get("operator").toString()
										.equalsIgnoreCase("es")) {
									if (baseAPIService.checkNull(jsonObject
											.get("formula"))) {
										if (jsonObject.get("formula")
												.toString()
												.equalsIgnoreCase("sum")) {
											String requestValues = jsonObject
													.get("requestValues")
													.toString();
											for (String aggregateName : requestValues
													.split(",")) {
												if (!jsonObject
														.has(aggregateName)) {
													continue;
												}
												aggregateFilter
														.subAggregation(AggregationBuilders
																.sum(jsonObject
																		.get(aggregateName)
																		.toString())
																.field(jsonObject
																		.get(aggregateName)
																		.toString()));
												if(j == 0){
												
													termBuilder
													.subAggregation(aggregateFilter);
												}else{
												subTermBuilder
														.subAggregation(aggregateFilter);
												}
												}

										}
									}
								}
							}
						}
						if(subAggregateFilter != null){
							subAggregateFilter.subAggregation(subTermBuilder);
						}
				}
				}
				if(groupBy.length == 1){
					System.out.println("getting in to what i expect");
				}else{
					termBuilder.subAggregation(subAggregateFilter);
				}
				searchRequestBuilder.addAggregation(termBuilder);
				System.out.println("now check " +searchRequestBuilder);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return true;
		}
		return false;
	}

	public FilterAggregationBuilder addFilters(
			List<RequestParamsFilterDetailDTO> requestParamsFiltersDetailDTO) {
		FilterAggregationBuilder subFilter = null;
		BoolFilterBuilder mainFilter = FilterBuilders.boolFilter();
		if (requestParamsFiltersDetailDTO != null) {
			for (RequestParamsFilterDetailDTO fieldData : requestParamsFiltersDetailDTO) {
				BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
				if (fieldData != null) {
					List<RequestParamsFilterFieldsDTO> requestParamsFilterFieldsDTOs = fieldData
							.getFields();
					for (RequestParamsFilterFieldsDTO fieldsDetails : requestParamsFilterFieldsDTOs) {
						String fieldName = esFields(fieldsDetails.getFieldName());
						if (fieldsDetails.getType()
								.equalsIgnoreCase("selector")) {
							if (fieldsDetails.getOperator().equalsIgnoreCase(
									"rg")) {
								boolFilter.must(FilterBuilders
										.rangeFilter(fieldName).from(checkDataType(fieldsDetails.getFrom(),fieldsDetails.getValueType()))
										.to(checkDataType(
												fieldsDetails.getTo(),
												fieldsDetails.getValueType())));
							} else if (fieldsDetails.getOperator()
									.equalsIgnoreCase("nrg")) {
								boolFilter.must(FilterBuilders
										.rangeFilter(fieldName)
										.from(checkDataType(
												fieldsDetails.getFrom(),
												fieldsDetails.getValueType()))
										.to(checkDataType(
												fieldsDetails.getTo(),
												fieldsDetails.getValueType())));
							} else if (fieldsDetails.getOperator()
									.equalsIgnoreCase("eq")) {
								boolFilter.must(FilterBuilders.inFilter(fieldName,
										checkDataType(fieldsDetails.getValue(),
												fieldsDetails.getValueType())));
							} else if (fieldsDetails.getOperator()
									.equalsIgnoreCase("lk")) {
								boolFilter.must(FilterBuilders.prefixFilter(fieldName,
										checkDataType(fieldsDetails.getValue(),
												fieldsDetails.getValueType())
												.toString()));
							}  else if (fieldsDetails.getOperator()
									.equalsIgnoreCase("in")) {
								boolFilter.must(FilterBuilders.inFilter(fieldName,
										fieldsDetails.getValue().split(",")));
							} else if (fieldsDetails.getOperator()
									.equalsIgnoreCase("ex")) {
								boolFilter.must(FilterBuilders
										.existsFilter(checkDataType(
												fieldsDetails.getValue(),
												fieldsDetails.getValueType())
												.toString()));
							} else if (fieldsDetails.getOperator()
									.equalsIgnoreCase("le")) {
								boolFilter.must(FilterBuilders.rangeFilter(
										fieldName).lte(
										checkDataType(fieldsDetails.getValue(),
												fieldsDetails.getValueType())));
							} else if (fieldsDetails.getOperator()
									.equalsIgnoreCase("ge")) {
								boolFilter.must(FilterBuilders.rangeFilter(
										fieldName).gte(
										checkDataType(fieldsDetails.getValue(),
												fieldsDetails.getValueType())));
							} else if (fieldsDetails.getOperator()
									.equalsIgnoreCase("lt")) {
								boolFilter.must(FilterBuilders.rangeFilter(
										fieldName).lt(
										checkDataType(fieldsDetails.getValue(),
												fieldsDetails.getValueType())));
							} else if (fieldsDetails.getOperator()
									.equalsIgnoreCase("gt")) {
								boolFilter.must(FilterBuilders.rangeFilter(
										fieldName).gt(
										checkDataType(fieldsDetails.getValue(),
												fieldsDetails.getValueType())));
							}
						} else {

						}
					}
					if (fieldData.getLogicalOperatorPrefix().equalsIgnoreCase(
							"AND")) {
						mainFilter.must(FilterBuilders.andFilter(boolFilter));
					} else if (fieldData.getLogicalOperatorPrefix()
							.equalsIgnoreCase("OR")) {
						mainFilter.must(FilterBuilders.orFilter(boolFilter));
					} else if (fieldData.getLogicalOperatorPrefix()
							.equalsIgnoreCase("NOT")) {
						mainFilter.must(FilterBuilders.notFilter(boolFilter));
					}
				}
			}
			subFilter = AggregationBuilders.filter("filters")
					.filter(mainFilter);
		}
		return subFilter;
	}
	
	public FilterAggregationBuilder duplicateFilters(
			List<RequestParamsFilterDetailDTO> requestParamsFiltersDetailDTO,String[] groupBy,String includeField,boolean firstFilter) {
		
		FilterAggregationBuilder subFilter = null;
		BoolFilterBuilder mainFilter = FilterBuilders.boolFilter();
		if (requestParamsFiltersDetailDTO != null) {
			for (RequestParamsFilterDetailDTO fieldData : requestParamsFiltersDetailDTO) {
				BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
				if (fieldData != null) {
					List<RequestParamsFilterFieldsDTO> requestParamsFilterFieldsDTOs = fieldData
							.getFields();
					List<RequestParamsFilterFieldsDTO>  mainFilterFieldsDTOs = new ArrayList<RequestParamsFilterFieldsDTO>();
					if(firstFilter){
					if(groupBy.length == 1){
						System.out.println("reassinged value");
						mainFilterFieldsDTOs = requestParamsFilterFieldsDTOs;
					}
							for (RequestParamsFilterFieldsDTO fieldsDetails : requestParamsFilterFieldsDTOs) {
								if(!(baseAPIService.convertArraytoString(groupBy).contains(fieldsDetails.getFieldName()))){
									System.out.println("trap "+fieldsDetails.getFieldName());
									mainFilterFieldsDTOs.add(fieldsDetails);
								}
								if(includeField.equalsIgnoreCase(fieldsDetails.getFieldName())){
									System.out.println("trap "+fieldsDetails.getFieldName());
								mainFilterFieldsDTOs.add(fieldsDetails);	
								}
					}
					}else{
							for (RequestParamsFilterFieldsDTO fieldsDetails : requestParamsFilterFieldsDTOs) {
							if(includeField.equalsIgnoreCase(fieldsDetails.getFieldName())){
								System.out.println("include "+fieldsDetails.getFieldName());
								mainFilterFieldsDTOs.add(fieldsDetails);
						}
						}
						
					}
					if(mainFilterFieldsDTOs.isEmpty()){
					return null;
					}
					filterFormula(boolFilter, mainFilterFieldsDTOs);
					if (fieldData.getLogicalOperatorPrefix().equalsIgnoreCase(
							"AND")) {
						mainFilter.must(FilterBuilders.andFilter(boolFilter));
					} else if (fieldData.getLogicalOperatorPrefix()
							.equalsIgnoreCase("OR")) {
						mainFilter.must(FilterBuilders.orFilter(boolFilter));
					} else if (fieldData.getLogicalOperatorPrefix()
							.equalsIgnoreCase("NOT")) {
						mainFilter.must(FilterBuilders.notFilter(boolFilter));
					}
				}
			}
			subFilter = AggregationBuilders.filter("filters")
					.filter(mainFilter);
		}
		return subFilter;
	}

	public void filterFormula(BoolFilterBuilder boolFilter,List<RequestParamsFilterFieldsDTO>  mainFilterFieldsDTOs){
		for (RequestParamsFilterFieldsDTO fieldsDetails : mainFilterFieldsDTOs) {
			System.out.println("entered filters");
//		BoolFilterBuilder subFilter =  FilterBuilders.boolFilter();	
			if (fieldsDetails.getOperator().equalsIgnoreCase(
					"rg")) {
				boolFilter.must(FilterBuilders
						.rangeFilter(
								fieldsDetails.getFieldName())
						.from(checkDataType(
								fieldsDetails.getFrom(),
								fieldsDetails.getValueType()))
						.to(checkDataType(
								fieldsDetails.getTo(),
								fieldsDetails.getValueType())));
			} else if (fieldsDetails.getOperator()
					.equalsIgnoreCase("nrg")) {
				boolFilter.must(FilterBuilders
						.rangeFilter(
								fieldsDetails.getFieldName())
						.from(checkDataType(
								fieldsDetails.getFrom(),
								fieldsDetails.getValueType()))
						.to(checkDataType(
								fieldsDetails.getTo(),
								fieldsDetails.getValueType())));
			} else if (fieldsDetails.getOperator()
					.equalsIgnoreCase("eq")) {
				boolFilter.must(FilterBuilders.inFilter(
						fieldsDetails.getFieldName(),
						checkDataType(fieldsDetails.getValue(),
								fieldsDetails.getValueType())));
			} else if (fieldsDetails.getOperator()
					.equalsIgnoreCase("lk")) {
				boolFilter.must(FilterBuilders.prefixFilter(
						fieldsDetails.getFieldName(),
						checkDataType(fieldsDetails.getValue(),
								fieldsDetails.getValueType())
								.toString()));
			} else if (fieldsDetails.getOperator()
					.equalsIgnoreCase("ex")) {
				boolFilter.must(FilterBuilders
						.existsFilter(checkDataType(
								fieldsDetails.getValue(),
								fieldsDetails.getValueType())
								.toString()));
			} else if (fieldsDetails.getOperator()
					.equalsIgnoreCase("le")) {
				boolFilter.must(FilterBuilders.rangeFilter(
						fieldsDetails.getFieldName()).lte(
						checkDataType(fieldsDetails.getValue(),
								fieldsDetails.getValueType())));
			} else if (fieldsDetails.getOperator()
					.equalsIgnoreCase("ge")) {
				boolFilter.must(FilterBuilders.rangeFilter(
						fieldsDetails.getFieldName()).gte(
						checkDataType(fieldsDetails.getValue(),
								fieldsDetails.getValueType())));
			} else if (fieldsDetails.getOperator()
					.equalsIgnoreCase("lt")) {
				boolFilter.must(FilterBuilders.rangeFilter(
						fieldsDetails.getFieldName()).lt(
						checkDataType(fieldsDetails.getValue(),
								fieldsDetails.getValueType())));
			} else if (fieldsDetails.getOperator()
					.equalsIgnoreCase("gt")) {
				boolFilter.must(FilterBuilders.rangeFilter(
						fieldsDetails.getFieldName()).gt(
						checkDataType(fieldsDetails.getValue(),
								fieldsDetails.getValueType())));
			}
//			boolFilter.must(subFilter);
		}
//		return boolFilter;
	}
	
	//search Filter
	public void addFilters(
			List<RequestParamsFilterDetailDTO> requestParamsFiltersDetailDTO,
			SearchRequestBuilder searchRequestBuilder) {
		if (requestParamsFiltersDetailDTO != null) {
			for (RequestParamsFilterDetailDTO fieldData : requestParamsFiltersDetailDTO) {
				if (fieldData != null) {
					FilterBuilder subFilter = null;

					List<RequestParamsFilterFieldsDTO> requestParamsFilterFieldsDTOs = fieldData
							.getFields();
					BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
					for (RequestParamsFilterFieldsDTO fieldsDetails : requestParamsFilterFieldsDTOs) {
						String fieldName = esFields(fieldsDetails.getFieldName());
						if (fieldsDetails.getType()
								.equalsIgnoreCase("selector")) {
							if (fieldsDetails.getOperator().equalsIgnoreCase(
									"rg")) {
								boolFilter.must(FilterBuilders
										.rangeFilter(fieldName)
										.from(checkDataType(
												fieldsDetails.getFrom(),
												fieldsDetails.getValueType()))
										.to(checkDataType(
												fieldsDetails.getTo(),
												fieldsDetails.getValueType())));
							} else if (fieldsDetails.getOperator()
									.equalsIgnoreCase("nrg")) {
								boolFilter.must(FilterBuilders
										.rangeFilter(fieldName)
										.from(checkDataType(
												fieldsDetails.getFrom(),
												fieldsDetails.getValueType()))
										.to(checkDataType(
												fieldsDetails.getTo(),
												fieldsDetails.getValueType())));
							} else if (fieldsDetails.getOperator()
									.equalsIgnoreCase("eq")) {
								boolFilter.must(FilterBuilders.termFilter(
										fieldName,
										checkDataType(fieldsDetails.getValue(),
												fieldsDetails.getValueType())));
							} else if (fieldsDetails.getOperator()
									.equalsIgnoreCase("lk")) {
								boolFilter.must(FilterBuilders.prefixFilter(
										fieldName,
										checkDataType(fieldsDetails.getValue(),
												fieldsDetails.getValueType())
												.toString()));
							} else if (fieldsDetails.getOperator()
									.equalsIgnoreCase("ex")) {
								boolFilter.must(FilterBuilders
										.existsFilter(checkDataType(
												fieldsDetails.getValue(),
												fieldsDetails.getValueType())
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
												fieldsDetails.getValueType())));
							} else if (fieldsDetails.getOperator()
									.equalsIgnoreCase("ge")) {
								boolFilter.must(FilterBuilders.rangeFilter(
										fieldName).gte(
										checkDataType(fieldsDetails.getValue(),
												fieldsDetails.getValueType())));
							} else if (fieldsDetails.getOperator()
									.equalsIgnoreCase("lt")) {
								boolFilter.must(FilterBuilders.rangeFilter(
										fieldName).lt(
										checkDataType(fieldsDetails.getValue(),
												fieldsDetails.getValueType())));
							} else if (fieldsDetails.getOperator()
									.equalsIgnoreCase("gt")) {
								boolFilter.must(FilterBuilders.rangeFilter(
										fieldName).gt(
										checkDataType(fieldsDetails.getValue(),
												fieldsDetails.getValueType())));
							}
						} 
						}
					if (fieldData.getLogicalOperatorPrefix().equalsIgnoreCase(
							"AND")) {
						subFilter = FilterBuilders.andFilter(boolFilter);
					} else if (fieldData.getLogicalOperatorPrefix()
							.equalsIgnoreCase("OR")) {
						subFilter = FilterBuilders.orFilter(boolFilter);
					} else if (fieldData.getLogicalOperatorPrefix()
							.equalsIgnoreCase("NOT")) {
						subFilter = FilterBuilders.notFilter(boolFilter);
					}
					searchRequestBuilder.setPostFilter(subFilter);
				}
			}
		}
	}

	//search Filter
		public void addFilters(
				List<RequestParamsFilterDetailDTO> requestParamsFiltersDetailDTO,
				BoolFilterBuilder boolFilter) {
			BoolFilterBuilder mainBool = FilterBuilders.boolFilter();
			if (requestParamsFiltersDetailDTO != null) {
				for (RequestParamsFilterDetailDTO fieldData : requestParamsFiltersDetailDTO) {
					if (fieldData != null) {
						List<RequestParamsFilterFieldsDTO> requestParamsFilterFieldsDTOs = fieldData
								.getFields();
						for (RequestParamsFilterFieldsDTO fieldsDetails : requestParamsFilterFieldsDTOs) {
							String fieldName = esFields(fieldsDetails.getFieldName());
							if (fieldsDetails.getType()
									.equalsIgnoreCase("selector")) {
								if (fieldsDetails.getOperator().equalsIgnoreCase(
										"rg")) {
									boolFilter.must(FilterBuilders
											.rangeFilter(fieldName)
											.from(checkDataType(
													fieldsDetails.getFrom(),
													fieldsDetails.getValueType()))
											.to(checkDataType(
													fieldsDetails.getTo(),
													fieldsDetails.getValueType())));
								} else if (fieldsDetails.getOperator()
										.equalsIgnoreCase("nrg")) {
									boolFilter.must(FilterBuilders
											.rangeFilter(fieldName)
											.from(checkDataType(
													fieldsDetails.getFrom(),
													fieldsDetails.getValueType()))
											.to(checkDataType(
													fieldsDetails.getTo(),
													fieldsDetails.getValueType())));
								} else if (fieldsDetails.getOperator()
										.equalsIgnoreCase("eq")) {
									boolFilter.must(FilterBuilders.termFilter(fieldName,
											checkDataType(fieldsDetails.getValue(),
													fieldsDetails.getValueType())));
								} else if (fieldsDetails.getOperator()
										.equalsIgnoreCase("lk")) {
									boolFilter.must(FilterBuilders.prefixFilter(fieldName,
											checkDataType(fieldsDetails.getValue(),
													fieldsDetails.getValueType())
													.toString()));
								} else if (fieldsDetails.getOperator()
										.equalsIgnoreCase("ex")) {
									boolFilter.must(FilterBuilders
											.existsFilter(checkDataType(
													fieldsDetails.getValue(),
													fieldsDetails.getValueType())
													.toString()));
								} else if (fieldsDetails.getOperator()
										.equalsIgnoreCase("le")) {
									boolFilter.must(FilterBuilders.rangeFilter(fieldName).lte(
											checkDataType(fieldsDetails.getValue(),
													fieldsDetails.getValueType())));
								} else if (fieldsDetails.getOperator()
										.equalsIgnoreCase("ge")) {
									boolFilter.must(FilterBuilders.rangeFilter(fieldName).gte(
											checkDataType(fieldsDetails.getValue(),
													fieldsDetails.getValueType())));
								} else if (fieldsDetails.getOperator()
										.equalsIgnoreCase("lt")) {
									boolFilter.must(FilterBuilders.rangeFilter(fieldName).lt(
											checkDataType(fieldsDetails.getValue(),
													fieldsDetails.getValueType())));
								} else if (fieldsDetails.getOperator()
										.equalsIgnoreCase("in")) {
									boolFilter.must(FilterBuilders.inFilter(fieldName,
											fieldsDetails.getValue().split(",")));
								}else if (fieldsDetails.getOperator()
										.equalsIgnoreCase("gt")) {
									boolFilter.must(FilterBuilders.rangeFilter(fieldName).gt(
											checkDataType(fieldsDetails.getValue(),
													fieldsDetails.getValueType())));
								}
							} 
							}
						if (fieldData.getLogicalOperatorPrefix().equalsIgnoreCase(
								"AND")) {
							mainBool.must(FilterBuilders.andFilter(boolFilter));
						} else if (fieldData.getLogicalOperatorPrefix()
								.equalsIgnoreCase("OR")) {
							mainBool.must(FilterBuilders.orFilter(boolFilter));
						} else if (fieldData.getLogicalOperatorPrefix()
								.equalsIgnoreCase("NOT")) {
							mainBool.must(FilterBuilders.notFilter(boolFilter));
						}
					}
				}
			}
			boolFilter = mainBool;
		}
		
	public Object checkDataType(String value, String valueType) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
		
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
	
	public void sortData(Map<String,String> sort,SearchRequestBuilder searchRequestBuilder){
		
		if (baseAPIService.checkNull(sort)) {
			for (Map.Entry<String, String> map : sort.entrySet()) {
				searchRequestBuilder.addSort(esFields(map.getKey()), (map.getValue()
						.equalsIgnoreCase("ASC") ? SortOrder.ASC
						: SortOrder.DESC));
			}
		}
	}

	public void paginate(Integer offset, Integer limit,SearchRequestBuilder searchRequestBuilder) {
		searchRequestBuilder.setFrom(offset.intValue());
		searchRequestBuilder.setSize(limit.intValue());
	}

	public Map<String,Object>  fetchAggregateData(String resultJson,String groupByData,String metrics){

		try {
			String[] groupBy = groupByData.split(",");
			JSONObject aggregateJSON = new JSONObject(resultJson);
			aggregateJSON = new JSONObject(aggregateJSON.get("aggregations").toString());
			aggregateJSON = new JSONObject(aggregateJSON.get(groupBy[0]).toString());
			JSONArray bucketArray = new JSONArray(aggregateJSON.get("buckets").toString());
			for(int j=0; j< bucketArray.length(); j++){
				int i =1;
				Map<String,Object> data = new HashMap<String,Object>();
				JSONObject dataJSON = new JSONObject(bucketArray.get(j).toString());
				data.put(groupBy[0], dataJSON.get("key"));
				while(i < groupBy.length){
					try{
				dataJSON = new JSONObject(dataJSON.get(groupBy[i]).toString());
				data.put(groupBy[i], dataJSON.get("key"));
				JSONArray innerBucketArray = new JSONArray(aggregateJSON.get("buckets").toString());
				for(int k=0; k< innerBucketArray.length(); k++){
					dataJSON = new JSONObject(bucketArray.get(k).toString());
				}
					}catch(Exception e){
						dataJSON = new JSONObject(dataJSON.get("filters").toString());
						for(String metric : metrics.split(",")){
							data.put(metric, dataJSON.get(metric));
						}
						e.printStackTrace();
					}
				i++;
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	//just pass the result of filterAggregator
	public Map<Integer,Map<String,Object>> processAggregateJSON(String groupBy,String resultData,Map<String,String> metrics,Map<String,Set<Object>> filterMap,List<Map<String,Object>> resultList){

		Map<Integer,Map<String,Object>> dataMap = new LinkedHashMap<Integer,Map<String,Object>>();
		try {
			int counter=0;
			String[] fields = groupBy.split(",");
			JSONObject json = new JSONObject(resultData);
			json = new JSONObject(json.get("aggregations").toString());
			while(counter < fields.length){
				JSONObject requestJSON = new JSONObject(json.get(esFields(fields[counter])).toString());
				System.out.println("request JSOn "+requestJSON);
			JSONArray jsonArray = new JSONArray(requestJSON.get("buckets").toString());
			JSONArray subJsonArray = new JSONArray();
			Set<Object> keys = new HashSet<Object>();
			boolean hasSubAggregate = false;
			for(int i=0;i<jsonArray.length();i++){
				JSONObject newJson = new JSONObject(jsonArray.get(i).toString());
				Object key=newJson.get("key");
				keys.add(key);
				if(counter+1 == fields.length){
					Map<String,Object> resultMap = new LinkedHashMap<String,Object>();
					boolean processed = false;
					for(Map.Entry<String,String> entry : metrics.entrySet()){
						if(newJson.has(entry.getValue())){
							resultMap.put(entry.getKey(), new JSONObject(newJson.get(entry.getValue()).toString()).get("value"));
							resultMap.put(fields[counter], newJson.get("key"));
						}
						}
					resultList.add(resultMap);
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
					newJson = new JSONObject(newJson.get(esFields(fields[counter+1])).toString());
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
				json.put(fields[counter+1], requestJSON);
			}
			
			filterMap.put(fields[counter], keys);
			counter++;
			}
		} catch (JSONException e) {
			System.out.println("some logical problem in aggregate json ");
			e.printStackTrace();
		}
		return dataMap;
	}
	
	public Map<Integer,Map<String,Object>> processFilterAggregateJSON(String groupBy,String resultData,Map<String,String> metrics,Map<String,Set<Object>> filterMap,List<Map<String,Object>> resultList){

		Map<Integer,Map<String,Object>> dataMap = new LinkedHashMap<Integer,Map<String,Object>>();
		try {
			int counter=0;
			String[] fields = groupBy.split(",");
			JSONObject json = new JSONObject(resultData);
			json = new JSONObject(json.get("aggregations").toString());
			while(counter < fields.length){
				JSONObject requestJSON = new JSONObject(json.get(esFields(fields[counter])).toString());
			JSONArray jsonArray = new JSONArray(requestJSON.get("buckets").toString());
			JSONArray subJsonArray = new JSONArray();
			Set<Object> keys = new HashSet<Object>();
			boolean hasSubAggregate = false;
			for(int i=0;i<jsonArray.length();i++){
				JSONObject newJson = new JSONObject(jsonArray.get(i).toString());
				Object key=newJson.get("key");
				keys.add(key);
				if(newJson.has("filters")){
					JSONObject metricsJson = new JSONObject(newJson.get("filters").toString());
					Map<String,Object> resultMap = new LinkedHashMap<String,Object>();
					boolean processed = false;
					for(Map.Entry<String,String> entry : metrics.entrySet()){
						if(metricsJson.has(entry.getValue())){
							resultMap.put(entry.getKey(), new JSONObject(metricsJson.get(entry.getValue()).toString()).get("value"));
							resultMap.put(fields[counter], newJson.get("key"));
						}
						}
					resultList.add(resultMap);
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
					newJson = new JSONObject(newJson.get(esFields(fields[counter+1])).toString());
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
				json.put(fields[counter+1], requestJSON);
			}
			
			filterMap.put(fields[counter], keys);
			counter++;
			}
		} catch (JSONException e) {
			System.out.println("some logical problem in filter aggregate json ");
			e.printStackTrace();
		}
		return dataMap;
	}

	
	public List<Map<String,Object>> processGFAJson(String groupBy,String resultData,Map<String,String> metrics,Map<String,Set<Object>> filterMap){
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		try {
			String[] fields = groupBy.split(",");
			JSONObject json = new JSONObject(resultData);
			json = new JSONObject(json.get("aggregations").toString());
			JSONObject requestJSON = new JSONObject(json.get(esFields(fields[0])).toString());
			JSONArray jsonArray = new JSONArray(requestJSON.get("buckets").toString());
			if(fields.length == 2){
			Set<Object> subKeys = new HashSet<Object>();
			for(int i=0;i<jsonArray.length();i++){
				JSONObject newJson = new JSONObject(jsonArray.get(i).toString());
				Object key=newJson.get("key");
				String granularity=newJson.getString("key_as_string");
				newJson = new JSONObject(newJson.get(esFields(fields[1])).toString());
				JSONArray subJsonArray = new JSONArray(newJson.get("buckets").toString());
				for(int j=0;j<subJsonArray.length();j++){
					JSONObject metricsJson = new JSONObject(subJsonArray.get(j).toString());
					Object subKey=metricsJson.get("key");
					subKeys.add(subKey);
					metricsJson = new JSONObject(metricsJson.get("filters").toString());
					Map<String,Object> resultMap = new HashMap<String,Object>();
					for(Map.Entry<String,String> entry : metrics.entrySet()){
						if(metricsJson.has(entry.getValue())){
							resultMap.put(entry.getKey(), new JSONObject(metricsJson.get(entry.getValue()).toString()).get("value"));
							resultMap.put(fields[1], subKey);
							resultMap.put(fields[0], baseAPIService.convertTimeMstoISO(key));
							resultMap.put("date", granularity);
						}
					}
					resultList.add(resultMap);
				}
			}
			filterMap.put(esFields(fields[1]), subKeys);
			}else if(fields.length == 1){
				for(int i=0;i<jsonArray.length();i++){
					JSONObject newJson = new JSONObject(jsonArray.get(i).toString());
					Object key=newJson.get("key");
					String granularity=newJson.getString("key_as_string");
					newJson = new JSONObject(newJson.get("filters").toString());
					for(Map.Entry<String,String> entry : metrics.entrySet()){
						if(newJson.has(entry.getValue())){
						Map<String,Object> resultMap = new HashMap<String,Object>();
							resultMap.put(entry.getKey(), new JSONObject(newJson.get(entry.getValue()).toString()).get("value"));
							resultMap.put(fields[0], baseAPIService.convertTimeMstoISO(key));
							resultMap.put("date", granularity);
							resultList.add(resultMap);
						}
					}
				}
			}else{
				//complex logic need to decide
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return resultList;
	}
	
	public List<Map<String,Object>> processGAJson(String groupBy,String resultData,Map<String,String> metrics,Map<String,Set<Object>> filterMap){
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		try {
			String[] fields = groupBy.split(",");
			JSONObject json = new JSONObject(resultData);
			System.out.println("result "+json.get("aggregations"));
			json = new JSONObject(json.get("aggregations").toString());
			System.out.println("aggregation "+json);
			JSONObject requestJSON = new JSONObject(json.get(esFields(fields[0])).toString());
			JSONArray jsonArray = new JSONArray(requestJSON.get("buckets").toString());
			if(fields.length == 2){
			Set<Object> subKeys = new HashSet<Object>();
			for(int i=0;i<jsonArray.length();i++){
				JSONObject newJson = new JSONObject(jsonArray.get(i).toString());
				System.out.println("sub json "+newJson);
				Object key=newJson.get("key");
				String granularity=newJson.getString("key_as_string");
				newJson = new JSONObject(newJson.get(esFields(fields[1])).toString());
				JSONArray subJsonArray = new JSONArray(newJson.get("buckets").toString());
				System.out.println(" sub array "+ subJsonArray);
				for(int j=0;j<subJsonArray.length();j++){
					JSONObject metricsJson = new JSONObject(subJsonArray.get(j).toString());
					Object subKey=metricsJson.get("key");
					subKeys.add(subKey);
					Map<String,Object> resultMap = new HashMap<String,Object>();
					for(Map.Entry<String,String> entry : metrics.entrySet()){
						if(metricsJson.has(entry.getValue())){
							resultMap.put(entry.getKey(), new JSONObject(metricsJson.get(entry.getValue()).toString()).get("value"));
							resultMap.put(fields[1], subKey);
							resultMap.put(fields[0], baseAPIService.convertTimeMstoISO(key));
							resultMap.put("date", granularity);
						}
					}
					resultList.add(resultMap);
				}
			}
			filterMap.put(esFields(fields[1]), subKeys);
			}else if(fields.length == 1){
				for(int i=0;i<jsonArray.length();i++){
					JSONObject newJson = new JSONObject(jsonArray.get(i).toString());
					Object key=newJson.get("key");
					String granularity=newJson.getString("key_as_string");
					for(Map.Entry<String,String> entry : metrics.entrySet()){
						if(newJson.has(entry.getValue())){
						Map<String,Object> resultMap = new HashMap<String,Object>();
							resultMap.put(entry.getKey(), new JSONObject(newJson.get(entry.getValue()).toString()).get("value"));
							resultMap.put(fields[0], baseAPIService.convertTimeMstoISO(key));
							resultMap.put("date", granularity);
							resultList.add(resultMap);
						}
					}
				}
			}else{
				//complex logic need to decide
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return resultList;
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
		addFilters(requestParamsDTOs.getFilter(),boolFilter);
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
	
	
	public JSONArray getRecords(String data,Map<String,String> dataRecord,Map<Integer,String> errorRecord,String dataKey){
		JSONObject json;
		JSONArray jsonArray = new JSONArray();
		JSONArray resultJsonArray = new JSONArray();
		try {
			json = new JSONObject(data);
			json = new JSONObject(json.get("hits").toString());
			dataRecord.put("totalRows", json.get("total").toString());
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

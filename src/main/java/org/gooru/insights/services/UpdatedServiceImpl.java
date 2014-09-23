package org.gooru.insights.services;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsFilterFieldsDTO;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;

@Service
public class UpdatedServiceImpl implements UpdatedService{

	@Autowired
	BaseConnectionService baseConnectionService;
	
	@Autowired
	BaseAPIService baseAPIService;
	
	
	public boolean aggregate(RequestParamsDTO requestParamsDTO,SearchRequestBuilder searchRequestBuilder) {
		try{
			TermsBuilder termBuilder = null;
			String[] groupBy = requestParamsDTO.getGroupBy().split(",");
			for(int i=groupBy.length-1; i >= 0;i--){
				TermsBuilder tempBuilder = null;
				if(termBuilder != null){
						tempBuilder = AggregationBuilders.terms("field"+i).field(esFields(groupBy[i]));
						tempBuilder.subAggregation(termBuilder);
						termBuilder = tempBuilder;
				}else{
					termBuilder = AggregationBuilders.terms("field"+i).field(esFields(groupBy[i]));
				}
				termBuilder.size(1000);
				System.out.println("i"+i+"groupBy -1 :"+(groupBy.length-1));
				if( i == groupBy.length-1){
					System.out.println("expected");
					includeAggregation(requestParamsDTO, termBuilder);
				}
			}
			if(baseAPIService.checkNull(requestParamsDTO.getFilter())){
			FilterBuilder filterBuilder = null;
			if(filterBuilder == null){
				filterBuilder = addFilters(requestParamsDTO.getFilter());
			}
			FilterAggregationBuilder filterAgrgregate = new FilterAggregationBuilder("filters").filter(filterBuilder).subAggregation(termBuilder);
			searchRequestBuilder.addAggregation(filterAgrgregate);
			}else{
				searchRequestBuilder.addAggregation(termBuilder);
			}
			return true;
	}catch(Exception e){
		e.printStackTrace();
		return false;
	}
	}
	
	public boolean granularityAggregate(RequestParamsDTO requestParamsDTO,SearchRequestBuilder searchRequestBuilder) {
		try{
			TermsBuilder termBuilder = null;
			DateHistogramBuilder dateHistogram = null;
			String[] groupBy = requestParamsDTO.getGroupBy().split(",");
			boolean isFirstDateHistogram = false;
			for(int i=groupBy.length-1; i >= 0;i--){
				TermsBuilder tempBuilder = null;
				//date field checker	
				if(i==0){
					dateHistogram = dateHistogram(requestParamsDTO.getGranularity(),"field"+i,esFields(groupBy[i]));
					isFirstDateHistogram =true;
					if(termBuilder != null){
						dateHistogram.subAggregation(termBuilder);
						termBuilder = null;
						}
					}else{
						
						if(termBuilder != null){
						tempBuilder = AggregationBuilders.terms("field"+i).field(esFields(groupBy[i]));
						if(dateHistogram != null){
							if(termBuilder != null){
								dateHistogram.subAggregation(termBuilder);
							}
							tempBuilder.subAggregation(dateHistogram);
						}else{
						tempBuilder.subAggregation(termBuilder);
						}
						termBuilder = tempBuilder;
						}else{
							termBuilder = AggregationBuilders.terms("field"+i).field(esFields(groupBy[i]));
						}
						isFirstDateHistogram =false;
					}
				termBuilder.size(1000);
				System.out.println("i"+i+"groupBy -1 :"+(groupBy.length-1));
				if( i == groupBy.length-1 && !isFirstDateHistogram){
					System.out.println("expected");
					if(termBuilder != null ){
					includeAggregation(requestParamsDTO, termBuilder);
					}
					}
				
				if( i == groupBy.length-1 && isFirstDateHistogram){
					System.out.println("expected");
					if(dateHistogram != null ){
					includeAggregation(requestParamsDTO, dateHistogram);
					}
					}
			}
			if(baseAPIService.checkNull(requestParamsDTO.getFilter())){
			FilterBuilder filterBuilder = null;
			if(filterBuilder == null){
				filterBuilder = addFilters(requestParamsDTO.getFilter());
			}
			FilterAggregationBuilder filterAgrgregate = new FilterAggregationBuilder("filters").filter(filterBuilder).subAggregation(termBuilder);
			searchRequestBuilder.addAggregation(filterAgrgregate);
			}else{
				searchRequestBuilder.addAggregation(termBuilder);
			}
			return true;
	}catch(Exception e){
		e.printStackTrace();
		return false;
	}
	}
	
	public void includeAggregation(RequestParamsDTO requestParamsDTO,TermsBuilder termBuilder){
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
						for (String aggregateName : requestValues
								.split(",")) {
							if (!jsonObject
									.has(aggregateName)) {
								continue;
							}
						performAggregation(termBuilder,jsonObject,jsonObject.getString("formula"), aggregateName);
						}
				}
		}
	}catch(Exception e){
		e.printStackTrace();
	}
	}
	}
	
	public void includeAggregation(RequestParamsDTO requestParamsDTO,DateHistogramBuilder  dateHistogramBuilder){
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
							for (String aggregateName : requestValues
									.split(",")) {
								if (!jsonObject
										.has(aggregateName)) {
									continue;
								}
							performAggregation(dateHistogramBuilder,jsonObject,jsonObject.getString("formula"), aggregateName);
							}
					}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		}
		}
	
	public void performAggregation(TermsBuilder mainFilter,JSONObject jsonObject,String aggregateType,String aggregateName){
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
	
	public void performAggregation(DateHistogramBuilder dateHistogramBuilder,JSONObject jsonObject,String aggregateType,String aggregateName){
		try {
			String esAggregateName= esFields(jsonObject.get(aggregateName).toString());
			if("SUM".equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder
			.subAggregation(AggregationBuilders
					.sum(esAggregateName)
					.field(esAggregateName));
			}else if("AVG".equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder
				.subAggregation(AggregationBuilders.avg(esAggregateName).field(esAggregateName));
			}else if("MAX".equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder
				.subAggregation(AggregationBuilders.max(esAggregateName).field(esAggregateName));
			}else if("MIN".equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder
				.subAggregation(AggregationBuilders.min(esAggregateName).field(esAggregateName));
				
			}else if("COUNT".equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder
				.subAggregation(AggregationBuilders.count(esAggregateName).field(esAggregateName));
			}else if("DISTINCT".equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder
				.subAggregation(AggregationBuilders.cardinality(esAggregateName).field(esAggregateName));
			}
	
		} catch (JSONException e) {
			e.printStackTrace();
		} 
		}
	//search Filter
		public BoolFilterBuilder addFilters(
				List<RequestParamsFilterDetailDTO> requestParamsFiltersDetailDTO) {
			BoolFilterBuilder subFilter = FilterBuilders.boolFilter();
			if (requestParamsFiltersDetailDTO != null) {
				for (RequestParamsFilterDetailDTO fieldData : requestParamsFiltersDetailDTO) {
					if (fieldData != null) {
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
							subFilter.must(FilterBuilders.andFilter(boolFilter));
						} else if (fieldData.getLogicalOperatorPrefix()
								.equalsIgnoreCase("OR")) {
							subFilter.must(FilterBuilders.orFilter(boolFilter));
						} else if (fieldData.getLogicalOperatorPrefix()
								.equalsIgnoreCase("NOT")) {
							subFilter.must(FilterBuilders.notFilter(boolFilter));
						}
					}
				}
			}
			return subFilter;
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
}

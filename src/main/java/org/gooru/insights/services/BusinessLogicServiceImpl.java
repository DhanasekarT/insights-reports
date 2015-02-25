package org.gooru.insights.services;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.NotFilterBuilder;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.ESConstants;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsFilterFieldsDTO;
import org.gooru.insights.models.RequestParamsPaginationDTO;
import org.gooru.insights.models.RequestParamsSortDTO;
import org.joda.time.Period;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

@Service
public class BusinessLogicServiceImpl implements BusinessLogicService,ESConstants,APIConstants{

	@Autowired
	BaseConnectionService baseConnectionService;
	
	@Autowired
	BaseAPIService baseAPIService;
	
	public boolean performAggregation(String index,RequestParamsDTO requestParamsDTO,SearchRequestBuilder searchRequestBuilder,Map<String,String> metricsName) {
		
		try{
			TermsBuilder termBuilder = null;
			String[] groupBy = requestParamsDTO.getGroupBy().split(COMMA);
		
			for(int i=groupBy.length-1; i >= 0;i--){
				String fieldName =esFields(index,groupBy[i]);
				TermsBuilder tempBuilder = null;
				if(termBuilder != null){
						tempBuilder = AggregationBuilders.terms(groupBy[i]).field(fieldName);
						tempBuilder.subAggregation(termBuilder);
						termBuilder = tempBuilder;
				}else{
					termBuilder = AggregationBuilders.terms(groupBy[i]).field(fieldName);
				}
				if( i == groupBy.length-1){
					includeAggregation(index,requestParamsDTO, termBuilder,metricsName);
					termBuilder.size(0);
				}
			}
			
			if(baseAPIService.checkNull(requestParamsDTO.getFilter())){
				FilterAggregationBuilder filterBuilder = null;
			if(filterBuilder == null){
				filterBuilder = includeFilterAggregate(index,requestParamsDTO.getFilter());
			}
			if(termBuilder != null){
				termBuilder.size(0);
				filterBuilder.subAggregation(termBuilder);
			}
			searchRequestBuilder.addAggregation(filterBuilder);
			}else{
				termBuilder.size(0);
				searchRequestBuilder.addAggregation(termBuilder);
			}
			return true;
	}catch(Exception e){
		e.printStackTrace();
		return false;
	}
	}
	
	public boolean performGranularityAggregation(String index,RequestParamsDTO requestParamsDTO,SearchRequestBuilder searchRequestBuilder,Map<String,String> metricsName,Map<String,Boolean> validatedData) {
		try{
			
			TermsBuilder termBuilder = null;
			DateHistogramBuilder dateHistogram = null;
			String[] groupBy = requestParamsDTO.getGroupBy().split(COMMA);
			boolean isFirstDateHistogram = false;
			
			for(int i=groupBy.length-1; i >= 0;i--){

				TermsBuilder tempBuilder = null;
				String groupByName = esFields(index,groupBy[i]);
				if(baseConnectionService.getFieldsDataType().containsKey(groupBy[i]) && baseConnectionService.getFieldsDataType().get(groupBy[i]).equalsIgnoreCase(logicalConstants.DATE.value())){
					isFirstDateHistogram =true;
					dateHistogram = generateDateHistogram(requestParamsDTO.getGranularity(),groupBy[i],groupByName);
					if(termBuilder != null){
						dateHistogram.subAggregation(termBuilder);
						termBuilder = null;
						}
				}else{
						if(termBuilder != null){
						tempBuilder = AggregationBuilders.terms(groupBy[i]).field(groupByName);
						if(dateHistogram != null){
							if(termBuilder != null){
								dateHistogram.subAggregation(termBuilder);
							}
						}else{
						tempBuilder.subAggregation(termBuilder);
						}
						termBuilder = tempBuilder;
						}else{
							termBuilder = AggregationBuilders.terms(groupBy[i]).field(groupByName);
						}
						if(dateHistogram != null){
							termBuilder.subAggregation(dateHistogram);
							dateHistogram = null;
						}
						isFirstDateHistogram =false;
					}
				if( i == groupBy.length-1 && !isFirstDateHistogram){
					if(termBuilder != null ){
					includeAggregation(index,requestParamsDTO, termBuilder,metricsName);
					includeOrder(requestParamsDTO, validatedData, groupBy[i], termBuilder,null,metricsName);
					termBuilder.size(0);
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
				termBuilder.size(0);
				filterBuilder.subAggregation(termBuilder);	
			}
			searchRequestBuilder.addAggregation(filterBuilder);
			}else{
				termBuilder.size(0);
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
				if(DESC.equalsIgnoreCase(orderData.getSortOrder())){
				termsBuilder.order(org.elasticsearch.search.aggregations.bucket.terms.Terms.Order.aggregation(metricsName.get(orderData.getSortBy()),false));
				}else{
					termsBuilder.order(org.elasticsearch.search.aggregations.bucket.terms.Terms.Order.aggregation(metricsName.get(orderData.getSortBy()),true));
				}
			}
		}
		if(histogramBuilder != null){
			if(metricsName.containsKey(orderData.getSortBy())){
				if(DESC.equalsIgnoreCase(orderData.getSortOrder())){
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
		String requestJsonArray = gson.toJson(requestParamsDTO.getAggregations());
		JSONArray jsonArray = new JSONArray(requestJsonArray);
	
		for (int i = 0; i < jsonArray.length(); i++) {
		
			JSONObject jsonObject;
			jsonObject = new JSONObject(jsonArray.get(i).toString());
			
			/*
			 * going to comment this line once we implement the error throw logic
			 */
			if (!jsonObject.has(formulaFields.FORMULA.field()) && !baseAPIService.checkNull(jsonObject.get(formulaFields.FORMULA.field())) && !jsonObject.has(formulaFields.REQUEST_VALUES.field()) && !jsonObject.has(jsonObject.getString(formulaFields.REQUEST_VALUES.field()))) {
				continue;
			}
				String requestValue = jsonObject.get(formulaFields.REQUEST_VALUES.field()).toString();
				String fieldName = esFields(index,jsonObject.get(requestValue).toString());
				performAggregation(termBuilder,jsonObject,jsonObject.getString(formulaFields.FORMULA.field()), requestValue,fieldName);
				metricsName.put(jsonObject.getString(formulaFields.NAME.field()) != null ? jsonObject.getString(formulaFields.NAME.field()) : fieldName, requestValue);
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
			String requestJsonArray = gson.toJson(requestParamsDTO.getAggregations());
			JSONArray jsonArray = new JSONArray(requestJsonArray);
		
			for (int i = 0; i < jsonArray.length(); i++) {
				JSONObject jsonObject;
				jsonObject = new JSONObject(jsonArray.get(i).toString());
				if (!jsonObject.has(formulaFields.FORMULA.field()) && !baseAPIService.checkNull(jsonObject.get(formulaFields.FORMULA.field())) && !jsonObject.has(formulaFields.REQUEST_VALUES.field()) && !baseAPIService.checkNull(jsonObject.get(formulaFields.REQUEST_VALUES.field()))) {
					continue;
				}
					String requestValues = jsonObject.get(formulaFields.REQUEST_VALUES.field()).toString();
					String fieldName = esFields(index,jsonObject.get(requestValues).toString());
					performAggregation(dateHistogramBuilder,jsonObject,jsonObject.getString(formulaFields.FORMULA.field()), requestValues, fieldName);
					metricsName.put(jsonObject.getString(formulaFields.NAME.field()) != null ? jsonObject.getString(formulaFields.NAME.field()) : fieldName, requestValues);
				}
			}catch(Exception e){
				e.printStackTrace();
			}
			}
		}
	
	public void performAggregation(TermsBuilder mainFilter,JSONObject jsonObject,String aggregateType,String aggregateName,String fieldName){

		try {
			if(aggregateFields.SUM.field().equalsIgnoreCase(aggregateType)){
			mainFilter.subAggregation(AggregationBuilders.sum(aggregateName).field(fieldName));
			}else if(aggregateFields.AVG.field().equalsIgnoreCase(aggregateType)){
				mainFilter.subAggregation(AggregationBuilders.avg(aggregateName).field(fieldName));
			}else if(aggregateFields.MAX.field().equalsIgnoreCase(aggregateType)){
				mainFilter.subAggregation(AggregationBuilders.max(aggregateName).field(fieldName));
			}else if(aggregateFields.MIN.field().equalsIgnoreCase(aggregateType)){
				mainFilter.subAggregation(AggregationBuilders.min(aggregateName).field(fieldName));
			}else if(aggregateFields.COUNT.field().equalsIgnoreCase(aggregateType)){
				mainFilter.subAggregation(AggregationBuilders.count(aggregateName).field(fieldName));
			}else if(aggregateFields.DISTINCT.field().equalsIgnoreCase(aggregateType)){
				mainFilter.subAggregation(AggregationBuilders.cardinality(aggregateName).field(fieldName));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	public void performAggregation(DateHistogramBuilder dateHistogramBuilder,JSONObject jsonObject,String aggregateType,String aggregateName,String fieldName){
		try {
			if(aggregateFields.SUM.field().equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder.subAggregation(AggregationBuilders.sum(aggregateName).field(fieldName));
			}else if(aggregateFields.AVG.field().equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder.subAggregation(AggregationBuilders.avg(aggregateName).field(fieldName));
			}else if(aggregateFields.MAX.field().equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder.subAggregation(AggregationBuilders.max(aggregateName).field(fieldName));
			}else if(aggregateFields.MIN.field().equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder.subAggregation(AggregationBuilders.min(aggregateName).field(fieldName));
			}else if(aggregateFields.COUNT.field().equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder.subAggregation(AggregationBuilders.count(aggregateName).field(fieldName));
			}else if(aggregateFields.DISTINCT.field().equalsIgnoreCase(aggregateType)){
				dateHistogramBuilder.subAggregation(AggregationBuilders.cardinality(aggregateName).field(fieldName));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} 
		}

	public BoolFilterBuilder customFilter(String index,Map<String,Object> filterData,Set<String> userFilter){
		
			BoolFilterBuilder boolFilter =FilterBuilders.boolFilter();
			
			Set<String> keys = filterData.keySet();
			Map<String,String> supportFilters = baseConnectionService.getFieldsJoinCache().get(index);
			Set<String> supportKeys = supportFilters.keySet();
			String supportKey = EMPTY;
			for(String key : supportKeys){
				if(baseAPIService.checkNull(supportKey)){
					supportKey+=COMMA;	
				}
				supportKey = key;
			}
			for(String key : keys){
				if(supportKey.contains(key)){
					userFilter.add(key);
				Set<Object> data = (Set<Object>) filterData.get(key);	
			if(!data.isEmpty()){
				boolFilter.must(FilterBuilders.inFilter(esFields(index,key),baseAPIService.convertSettoArray(data)));
			}
				}
			}
			return boolFilter;
		}

		public BoolFilterBuilder includeFilter(String index,List<RequestParamsFilterDetailDTO> requestParamsFiltersDetailDTO) {
			
			BoolFilterBuilder boolFilter =FilterBuilders.boolFilter();
			if (requestParamsFiltersDetailDTO != null) {
			for (RequestParamsFilterDetailDTO fieldData : requestParamsFiltersDetailDTO) {
					if (fieldData != null) {
						List<RequestParamsFilterFieldsDTO> requestParamsFilterFieldsDTOs = fieldData.getFields();
						AndFilterBuilder andFilter = null;
						OrFilterBuilder orFilter = null;
						NotFilterBuilder notFilter =null;
			for (RequestParamsFilterFieldsDTO fieldsDetails : requestParamsFilterFieldsDTOs) {
				FilterBuilder filter = null;
				String fieldName = esFields(index,fieldsDetails.getFieldName());
				if (fieldsDetails.getType().equalsIgnoreCase(esFilterFields.SELECTOR.field())) {
					if (esFilterFields.RG.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
							filter = FilterBuilders.rangeFilter(fieldName).from(checkDataType(fieldsDetails.getFrom(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()))
										.to(checkDataType(fieldsDetails.getTo(),fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					} else if (esFilterFields.NRG.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter =  FilterBuilders.notFilter(FilterBuilders.rangeFilter(fieldName).from(checkDataType(fieldsDetails.getFrom(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()))
										.to(checkDataType(fieldsDetails.getTo(),fieldsDetails.getValueType(),fieldsDetails.getFormat())));
					} else if (esFilterFields.EQ.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter = FilterBuilders.termFilter(fieldName,checkDataType(fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					} else if (esFilterFields.LK.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter =  FilterBuilders.prefixFilter(fieldName,checkDataType(fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()).toString());
					} else if (esFilterFields.EX.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter = FilterBuilders.existsFilter(checkDataType(fieldsDetails.getValue(),fieldsDetails.getValueType(),fieldsDetails.getFormat()).toString());
					}   else if (esFilterFields.IN.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter = FilterBuilders.inFilter(fieldName,fieldsDetails.getValue().split(COMMA));
					} else if (esFilterFields.LE.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter = FilterBuilders.rangeFilter(fieldName).lte(checkDataType(fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					} else if (esFilterFields.GE.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter = FilterBuilders.rangeFilter(fieldName).gte(checkDataType(fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					} else if (esFilterFields.LT.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter = FilterBuilders.rangeFilter(fieldName).lt(checkDataType(fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					} else if (esFilterFields.GT.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter = FilterBuilders.rangeFilter(fieldName).gt(checkDataType(fieldsDetails.getValue(),
										fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					}
					}

			
			if (esFilterFields.AND.field().equalsIgnoreCase(fieldData.getLogicalOperatorPrefix())) {
				if(andFilter == null){
					andFilter = FilterBuilders.andFilter(filter);
				}else{
					andFilter.add(filter);
				}
			}else if (esFilterFields.OR.field().equalsIgnoreCase(fieldData.getLogicalOperatorPrefix())) {
				if(orFilter == null){
					orFilter = FilterBuilders.orFilter(filter);
				}else{
					orFilter.add(filter);
				}
			}else if (esFilterFields.NOT.field().equalsIgnoreCase(fieldData.getLogicalOperatorPrefix())) {
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
			FilterAggregationBuilder filterBuilder = new FilterAggregationBuilder(esFilterFields.FILTERS.field());
			if (requestParamsFiltersDetailDTO != null) {
				BoolFilterBuilder boolFilter =FilterBuilders.boolFilter();
				for (RequestParamsFilterDetailDTO fieldData : requestParamsFiltersDetailDTO) {
					if (fieldData != null) {
						List<RequestParamsFilterFieldsDTO> requestParamsFilterFieldsDTOs = fieldData.getFields();
						AndFilterBuilder andFilter = null;
						OrFilterBuilder orFilter = null;
						NotFilterBuilder notFilter =null;
			for (RequestParamsFilterFieldsDTO fieldsDetails : requestParamsFilterFieldsDTOs) {
				FilterBuilder filter = null;
				String fieldName = esFields(index,fieldsDetails.getFieldName());
				if (esFilterFields.SELECTOR.field().equalsIgnoreCase(fieldsDetails.getType())) {
					
					if (esFilterFields.RG.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter = FilterBuilders.rangeFilter(fieldName).from(checkDataType(fieldsDetails.getFrom(),fieldsDetails.getValueType(),fieldsDetails.getFormat()))
									.to(checkDataType(fieldsDetails.getTo(),fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					
					} else if (esFilterFields.NRG.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter = FilterBuilders.notFilter(FilterBuilders.rangeFilter(fieldName).from(checkDataType(fieldsDetails.getFrom(),fieldsDetails.getValueType(),fieldsDetails.getFormat()))
									.to(checkDataType(fieldsDetails.getTo(),fieldsDetails.getValueType(),fieldsDetails.getFormat())));
					
					} else if (esFilterFields.EQ.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter = FilterBuilders.termFilter(fieldName,checkDataType(fieldsDetails.getValue(),
									fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					
					} else if (esFilterFields.LK.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter =  FilterBuilders.prefixFilter(fieldName,checkDataType(fieldsDetails.getValue(),
									fieldsDetails.getValueType(),fieldsDetails.getFormat()).toString());
					
					} else if (esFilterFields.EX.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter = FilterBuilders.existsFilter(checkDataType(fieldsDetails.getValue(),
									fieldsDetails.getValueType(),fieldsDetails.getFormat()).toString());
					
					}   else if (esFilterFields.IN.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter = FilterBuilders.inFilter(fieldName,fieldsDetails.getValue().split(COMMA));
					
					} else if (esFilterFields.LE.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter = FilterBuilders.rangeFilter(fieldName).lte(
								checkDataType(fieldsDetails.getValue(),fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					
					} else if (esFilterFields.GE.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter = FilterBuilders.rangeFilter(fieldName).gte(checkDataType(fieldsDetails.getValue(),
								fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					
					} else if (esFilterFields.LT.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter = FilterBuilders.rangeFilter(
								fieldName).lt(checkDataType(fieldsDetails.getValue(),fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					
					} else if (esFilterFields.GT.field().equalsIgnoreCase(fieldsDetails.getOperator())) {
						filter = FilterBuilders.rangeFilter(fieldName).gt(checkDataType(fieldsDetails.getValue(),
								fieldsDetails.getValueType(),fieldsDetails.getFormat()));
					}
					}

			
			if (esFilterFields.AND.field().equalsIgnoreCase(fieldData.getLogicalOperatorPrefix())) {
				if(andFilter == null){
					andFilter = FilterBuilders.andFilter(filter);
				}else{
					andFilter.add(filter);
				}
			}else if(esFilterFields.OR.field().equalsIgnoreCase(fieldData.getLogicalOperatorPrefix())) {
				if(orFilter == null){
					orFilter = FilterBuilders.orFilter(filter);
				}else{
					orFilter.add(filter);
				}
			}else if(esFilterFields.NOT.field().equalsIgnoreCase(fieldData.getLogicalOperatorPrefix())) {
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
			
			SimpleDateFormat format = new SimpleDateFormat(DEFAULT_FORMAT);
			
			if(baseAPIService.checkNull(dateformat)){
				try{
				format = new SimpleDateFormat(dateformat);
				}catch(Exception e){
					
				}
			}
			if (dataTypes.STRING.dataType().equalsIgnoreCase(valueType)) {
				return value;
			} else if (dataTypes.LONG.dataType().equalsIgnoreCase(valueType)) {
				return Long.valueOf(value);
			} else if (dataTypes.INTEGER.dataType().equalsIgnoreCase(valueType)) {
				return Integer.valueOf(value);
			} else if (dataTypes.DOUBLE.dataType().equalsIgnoreCase(valueType)) {
				return Double.valueOf(value);
			} else if (dataTypes.SHORT.dataType().equalsIgnoreCase(valueType)) {
				return Short.valueOf(value);
			}else if (dataTypes.DATE.dataType().equalsIgnoreCase(valueType)) {
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
		
		public DateHistogramBuilder generateDateHistogram(String granularity,String fieldName,String field){
			
			String format =dateFormats.DEFAULT.format();
			if(baseAPIService.checkNull(granularity)){
				granularity = granularity.toUpperCase();
				org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Interval interval = DateHistogram.Interval.DAY;
				if(dateFormats.YEAR.name().equalsIgnoreCase(granularity)){
					interval = DateHistogram.Interval.YEAR;
					format =dateFormats.YEAR.format();
				}else if(dateFormats.DAY.name().equalsIgnoreCase(granularity)){
					interval = DateHistogram.Interval.DAY;
					format =dateFormats.DAY.format();
				}else if(dateFormats.MONTH.name().equalsIgnoreCase(granularity)){
					interval = DateHistogram.Interval.MONTH;
					format =dateFormats.MONTH.format();
				}else if(dateFormats.HOUR.name().equalsIgnoreCase(granularity)){
					interval = DateHistogram.Interval.HOUR;
					format =dateFormats.HOUR.format();
				}else if(dateFormats.MINUTE.name().equalsIgnoreCase(granularity)){
					interval = DateHistogram.Interval.MINUTE;
					format =dateFormats.MINUTE.format();
				}else if(dateFormats.SECOND.name().equalsIgnoreCase(granularity)){
					interval = DateHistogram.Interval.SECOND;
				}else if(dateFormats.QUARTER.name().equalsIgnoreCase(granularity)){
					interval = DateHistogram.Interval.QUARTER;
					format =dateFormats.QUARTER.format();
				}else if(dateFormats.WEEK.name().equalsIgnoreCase(granularity)){
					format =dateFormats.WEEK.format();
					interval = DateHistogram.Interval.WEEK;
				}else if(granularity.matches(dateHistory.D_CHECKER.replace())){
					int days = new Integer(granularity.replaceFirst(dateHistory.D_REPLACER.replace(),EMPTY));
					format =dateFormats.D.format();
					interval = DateHistogram.Interval.days(days);
				}else if(granularity.matches(dateHistory.W_CHECKER.replace())){
					int weeks = new Integer(granularity.replaceFirst(dateHistory.W_REPLACER.replace(),EMPTY));
					format =dateFormats.W.name();
					interval = DateHistogram.Interval.weeks(weeks);
				}else if(granularity.matches(dateHistory.H_CHECKER.replace())){
					int hours = new Integer(granularity.replaceFirst(dateHistory.H_REPLACER.replace(),EMPTY));
					format =dateFormats.H.name();
					interval = DateHistogram.Interval.hours(hours);
				}else if(granularity.matches(dateHistory.K_CHECKER.replace())){
					int minutes = new Integer(granularity.replaceFirst(dateHistory.K_REPLACER.replace(),EMPTY));
					format =dateFormats.K.format();
					interval = DateHistogram.Interval.minutes(minutes);
				}else if(granularity.matches(dateHistory.S_CHECKER.replace())){
					int seconds = new Integer(granularity.replaceFirst(dateHistory.S_REPLACER.replace(),EMPTY));
					interval = DateHistogram.Interval.seconds(seconds);
				}
				DateHistogramBuilder dateHistogram = AggregationBuilders.dateHistogram(fieldName).field(field).interval(interval).format(format);
				return dateHistogram;
		}
			return null;
	}

		public List<Map<String,Object>> customizeJSON(String[] groupBy,String resultData,Map<String,String> metrics,boolean hasFilter,Map<String,Object> dataMap,int limit){

			List<Map<String,Object>> dataList = new ArrayList<Map<String,Object>>();
		       try {
		    		int counter=0;
		    		int totalRows =0;
					JSONObject json = new JSONObject(resultData);
					json = new JSONObject(json.get(formulaFields.AGGREGATIONS.field()).toString());
 					if(hasFilter){
 						json = new JSONObject(json.get(formulaFields.FILTERS.field()).toString());
					}
					
					while(counter < groupBy.length){
		        	   if(json.length() > 0){
		               JSONObject requestJSON = new JSONObject(json.get(groupBy[counter]).toString());
		           JSONArray jsonArray = new JSONArray(requestJSON.get(formulaFields.BUCKETS.field()).toString());
		           if(counter == 0){
		        	   totalRows = jsonArray.length();
						dataMap.put(formulaFields.TOTAL_ROWS.field(), totalRows); 
		           }
		           JSONArray subJsonArray = new JSONArray();
		           Set<Object> keys = new HashSet<Object>();
		           boolean hasSubAggregate = false;

		           for(int i=0;i<jsonArray.length();i++){
		               JSONObject newJson = new JSONObject(jsonArray.get(i).toString());
		               Object key=newJson.get(formulaFields.KEY.field());
		               keys.add(key);
		               if(counter == 0 && limit == i){
		            	   break;
		               }
		               if(counter+1 == (groupBy.length)){
		            	   fetchMetrics(newJson, dataList, metrics, groupBy, counter);
		               }else{
		            	   hasSubAggregate = true;
		            	   iterateInternalObject(newJson, subJsonArray, groupBy, counter, key);
		               }
		           }
		           
		           if(hasSubAggregate){
		               json = new JSONObject();
		               requestJSON.put(formulaFields.BUCKETS.field(), subJsonArray);
		               json.put(groupBy[counter+1], requestJSON);
		           }
		           
		        	   }
		           counter++;
		           }
		       } catch (JSONException e) {
		           e.printStackTrace();
		       }
		       return dataList;
		}
		
		void fetchMetrics(JSONObject newJson,List<Map<String,Object>> dataList,Map<String,String> metrics,String[] groupBy,Integer counter) throws JSONException{
     	   Map<String,Object> resultMap = new LinkedHashMap<String,Object>();
           for(Map.Entry<String,String> entry : metrics.entrySet()){
               if(newJson.has(entry.getValue())){
                   resultMap.put(entry.getKey(), new JSONObject(newJson.get(entry.getValue()).toString()).get("value"));
                   resultMap.put(groupBy[counter], newJson.get(formulaFields.KEY.field()));
                   newJson.remove(entry.getValue());    
               }
               }
           newJson.remove(formulaFields.DOC_COUNT.field());
           newJson.remove(formulaFields.KEY_AS_STRING.field());
           newJson.remove(formulaFields.KEY.field());
           newJson.remove(formulaFields.BUCKETS.field());
           Iterator<String> dataKeys = newJson.sortedKeys();
           while(dataKeys.hasNext()){
        	   String dataKey = dataKeys.next();
        	   resultMap.put(dataKey, newJson.get(dataKey));
           }
           dataList.add(resultMap);
		}
		
		void iterateInternalObject(JSONObject newJson,JSONArray subJsonArray,String[] groupBy,Integer counter,Object key) throws JSONException{
            JSONArray tempArray = new JSONArray();
            JSONObject dataJson = new JSONObject(newJson.get(groupBy[counter+1]).toString());
            tempArray = new JSONArray(dataJson.get(formulaFields.BUCKETS.field()).toString());
            for(int j=0;j<tempArray.length();j++){
                JSONObject subJson = new JSONObject(tempArray.get(j).toString());
                subJson.put(groupBy[counter], key);
                newJson.remove(groupBy[counter+1]);
                newJson.remove(formulaFields.DOC_COUNT.field());
                newJson.remove(formulaFields.KEY_AS_STRING.field());
                newJson.remove(formulaFields.KEY.field());
                newJson.remove(formulaFields.BUCKETS.field());
                Iterator<String> dataKeys = newJson.sortedKeys();
                while(dataKeys.hasNext()){
                String dataKey = dataKeys.next();
                subJson.put(dataKey, newJson.get(dataKey));
                 }

                subJsonArray.put(subJson);
            }
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
				String[] fields = groupBy.split(COMMA);
				JSONObject json = new JSONObject(resultData);
				json = new JSONObject(json.get(formulaFields.AGGREGATIONS.field()).toString());
				if(hasFilter){
					json = new JSONObject(json.get(formulaFields.FILTERS.field()).toString());
				}
				while(counter < fields.length){
					JSONObject requestJSON = new JSONObject(json.get(formulaFields.FIELD.field()+counter).toString());
				JSONArray jsonArray = new JSONArray(requestJSON.get(formulaFields.BUCKETS.field()).toString());
				JSONArray subJsonArray = new JSONArray();
				boolean hasSubAggregate = false;
				for(int i=0;i<jsonArray.length();i++){
					JSONObject newJson = new JSONObject(jsonArray.get(i).toString());
					Object key=newJson.get(formulaFields.KEY.field());
						if(counter+1 == (fields.length)){
						Map<String,Object> resultMap = new LinkedHashMap<String,Object>();
						boolean processed = false;
						for(Map.Entry<String,String> entry : metrics.entrySet()){
							if(newJson.has(entry.getValue())){
								resultMap.put(entry.getKey(), new JSONObject(newJson.get(entry.getValue()).toString()).get("value"));
								resultMap.put(fields[counter], newJson.get(formulaFields.KEY.field()));
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
						newJson = new JSONObject(newJson.get(formulaFields.FIELD.field()+(counter+1)).toString());
						tempArray = new JSONArray(newJson.get(formulaFields.BUCKETS.field()).toString());
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
					requestJSON.put(formulaFields.BUCKETS.field(), subJsonArray);
					json.put(formulaFields.FIELD.field()+(counter+1), requestJSON);
				}
				
				counter++;
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return dataMap;
		}
		
		public void includeOrder(RequestParamsDTO requestParamsDTO,Map<String,Boolean> validatedData,String fieldName,TermsBuilder termsBuilder,DateHistogramBuilder dateHistogramBuilder,Map<String,String> metricsName){
			
			if(validatedData.get(hasdata.HAS_SORTBY.check())){
				RequestParamsPaginationDTO pagination = requestParamsDTO.getPagination();
				List<RequestParamsSortDTO> orderDatas = pagination.getOrder();
				for(RequestParamsSortDTO orderData : orderDatas){
					if(termsBuilder != null){
						if(fieldName.equalsIgnoreCase(orderData.getSortBy())){
							if(DESC.equalsIgnoreCase(orderData.getSortOrder())){
								
								termsBuilder.order(org.elasticsearch.search.aggregations.bucket.terms.Terms.Order.term(false));
							}else{
								termsBuilder.order(org.elasticsearch.search.aggregations.bucket.terms.Terms.Order.term(true));	
							}
						}
						sortAggregatedValue(termsBuilder,null, requestParamsDTO, orderData,metricsName);
					}else if(dateHistogramBuilder != null){
						if(fieldName.equalsIgnoreCase(orderData.getSortBy())){
							if(DESC.equalsIgnoreCase(orderData.getSortOrder())){
								
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

		public Map<String,Object> fetchFilters(String index,List<Map<String,Object>> dataList){
			Map<String,String> filterFields = new HashMap<String, String>();
			Map<String,Object> filters = new HashMap<String, Object>();
			if(baseConnectionService.getFieldsJoinCache().containsKey(index)){
				filterFields = baseConnectionService.getFieldsJoinCache().get(index);
			}
				for(Map<String,Object> dataMap : dataList){
					Set<String> keySets = filterFields.keySet();
					for(String keySet : keySets){
					for(String key : keySet.split(COMMA)){
						if(dataMap.containsKey(key)){
							buildFilter(dataMap, filters, key);	
						}
					}
					}
				}
				return filters;
		}
		
		private void buildFilter(Map<String,Object> dataMap,Map<String,Object> filters,String key){
			if(!filters.isEmpty() && filters.containsKey(key)){
				Set<Object> filterValue = (Set<Object>) filters.get(key);
				try{
					Set<Object> datas = (Set<Object>) dataMap.get(key);
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
					Set<Object> datas = (Set<Object>) dataMap.get(key);
					for(Object data : datas){
						filterValue.add(data);
					}
				}catch(Exception e){
					filterValue.add(dataMap.get(key));
				}
				filters.put(key, filterValue);
			}
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
		
		public List<Map<String,Object>> customPagination(RequestParamsPaginationDTO requestParamsPaginationDTO,List<Map<String,Object>> data,Map<String,Boolean> validatedData){
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
				
				if((limit+offset) < dataSize && offset < dataSize){
					customizedData = data.subList(offset, offset+limit);
				}else if((limit+offset) >= dataSize &&  offset < dataSize){
					customizedData = data.subList(offset, dataSize);
				}else if((limit+offset) < dataSize &&  offset >= dataSize){
					customizedData = data.subList(0,limit);
				}else if((limit+offset) >= dataSize &&  offset >= dataSize){
					customizedData = data.subList(0,dataSize);
				}
			}else{
				customizedData = data;
			}
			return customizedData;
		}
		
		public List<Map<String,Object>> aggregatePaginate(RequestParamsPaginationDTO requestParamsPaginationDTO,List<Map<String,Object>> data,Map<String,Boolean> validatedData){
			int dataSize = data.size();
			List<Map<String,Object>> customizedData = new ArrayList<Map<String,Object>>();
			if(baseAPIService.checkNull(requestParamsPaginationDTO)){
				int offset = validatedData.get(hasdata.HAS_Offset.check()) ? requestParamsPaginationDTO.getOffset() : 0; 
				int limit = validatedData.get(hasdata.HAS_LIMIT.check()) ? requestParamsPaginationDTO.getLimit() : 10; 
				
				if((limit+offset) < dataSize && offset < dataSize){
					customizedData = data.subList(offset, offset+limit);
				}else if((limit+offset) >= dataSize &&  offset < dataSize){
					customizedData = data.subList(offset, dataSize);
				}else if((limit+offset) < dataSize &&  offset >= dataSize){
					customizedData = data.subList(0,limit);
				}else if((limit+offset) >= dataSize &&  offset >= dataSize){
					customizedData = data.subList(0,dataSize);
				}
			}else{
				customizedData = data;
			}
			return customizedData;
		}
		
		public List<Map<String,Object>> customSort(RequestParamsPaginationDTO requestParamsPaginationDTO,List<Map<String,Object>> data,Map<String,Boolean> validatedData){
			if(baseAPIService.checkNull(requestParamsPaginationDTO)){
				if(validatedData.get(hasdata.HAS_SORTBY.check())){
					List<RequestParamsSortDTO> orderDatas = requestParamsPaginationDTO.getOrder();
					for(RequestParamsSortDTO sortData : orderDatas){
						baseAPIService.sortBy(data, sortData.getSortBy(), sortData.getSortOrder());
					}
				}
				
			}	
			return data;
		}
		
		public List<Map<String,Object>> getData(String fields,String jsonObject){
			List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
			try{
				JSONObject json = new JSONObject(jsonObject);
				json = new JSONObject(json.get(formulaFields.HITS.field()).toString());
			JSONArray hitsArray = new JSONArray(json.get(formulaFields.HITS.field()).toString());
			
			for(int i=0;i<hitsArray.length();i++){
				Map<String,Object> resultMap = new HashMap<String,Object>();
			JSONObject getSourceJson = new JSONObject(hitsArray.get(i).toString());
			getSourceJson = new JSONObject(getSourceJson.get(formulaFields.SOURCE.field()).toString());
			if(baseAPIService.checkNull(fields)){
			for(String field : fields.split(COMMA)){
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
				mainJson = new JSONObject(mainJson.get(formulaFields.HITS.field()).toString());
				JSONArray jsonArray = new JSONArray(mainJson.get(formulaFields.HITS.field()).toString());
				for(int i=0;i<jsonArray.length();i++){
					 mainJson = new JSONObject(jsonArray.get(i).toString());
					 Map<String,Object> dataMap = new HashMap<String,Object>();	 
					 dataMap = gson.fromJson(mainJson.getString(formulaFields.SOURCE.field()),mapType);
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

		public List<Map<String,Object>> getRecords(String[] indices,String data,Map<Integer,String> errorRecord,String dataKey,Map<String,Object> dataMap){
			JSONObject json;
			JSONArray jsonArray = new JSONArray();
			List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
			try {
				json = new JSONObject(data);
				json = new JSONObject(json.get(formulaFields.HITS.field()).toString());
				int totalRows = json.getInt(formulaFields.TOTAL.field());
				
				if(dataMap != null){
				dataMap.put(formulaFields.TOTAL_ROWS.field(), totalRows);
				}
				
				jsonArray = new JSONArray(json.get(formulaFields.HITS.field()).toString());
				if(!formulaFields.FIELDS.field().equalsIgnoreCase(dataKey)){
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
									if(!columnKey.equalsIgnoreCase(FIELD_NAME) && !columnKey.equalsIgnoreCase(DEPENDENT_NAME)){
										if(columnKey.equalsIgnoreCase(fieldJson.getString(dependentColumn.get(DEPENDENT_NAME)))){
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
										if(!columnKey.equalsIgnoreCase(FIELD_NAME) && !columnKey.equalsIgnoreCase(DEPENDENT_NAME)){
											if(columnKey.equalsIgnoreCase(new JSONArray(json.getString(dependentColumn.get(DEPENDENT_NAME))).getString(0))){
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
				json = new JSONObject(json.get(formulaFields.HITS.field()).toString());
				jsonArray = new JSONArray(json.get(formulaFields.HITS.field()).toString());
				if(!formulaFields.FIELDS.field().equalsIgnoreCase(dataKey)){
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
			for(String field : fields.split(COMMA)){
				if(esFields.length() > 0){
					esFields.append(COMMA);
				}
				if(apiFields.containsKey(field)){
					esFields.append(apiFields.get(field));
				}else{
					esFields.append(field);
				}
			}
			return esFields.toString();
		}
		
		public void generateActorProperty(JSONObject activityJsonObject, Map<String, Object> actorAsMap) {
			try {
				actorAsMap.put("objectType", "Agent");
				if (!activityJsonObject.isNull("emailId") && StringUtils.isNotBlank(activityJsonObject.get("emailId").toString())) {
					actorAsMap.put("mbox", "mailto:" + activityJsonObject.get("emailId"));
				} else {
					actorAsMap.put("id", activityJsonObject.get("gooruUId"));
				}
				actorAsMap.put("apiKey", activityJsonObject.get("apiKey"));
				actorAsMap.put("organizationUid", activityJsonObject.get("userOrganizationUId"));
				if (!activityJsonObject.isNull("userIp") && StringUtils.isNotBlank(activityJsonObject.get("userIp").toString())) {
					actorAsMap.put("userIp", activityJsonObject.get("userIp"));
					actorAsMap.put("userAgent", activityJsonObject.get("userAgent"));
				}
			} catch (JSONException e) {
				// TODO Error Handling Required
				e.printStackTrace();
			}
		}
		public void generateVerbProperty(JSONObject activityJsonObject, Map<String, Object> verbAsMap) {
			String verb = null;
			try {
				String eventName = activityJsonObject.get("eventName").toString();
				if (eventName.toString().equalsIgnoreCase("item.create") && activityJsonObject.get("mode").toString().equalsIgnoreCase("copy")) {
					verb = "copied";
				} else if (eventName.toString().equalsIgnoreCase("item.create") && activityJsonObject.get("mode").toString().equalsIgnoreCase("copy")) {
					verb = "moved";
				} else if (eventName.toString().equalsIgnoreCase("item.create")) {
					verb = "created";
				} else if (eventName.toString().endsWith("play")) {
					verb = "studied";
				} else if (eventName.toString().contains("delete")) {
					verb = "deleted";
				} else if (eventName.toString().equalsIgnoreCase("reaction.create")) {
					verb = "reacted";
				} else if (eventName.toString().endsWith("rate") || eventName.toString().endsWith("review")) {
					verb = "reviewed";
				} else if (eventName.toString().endsWith("view")) {
					verb = "viewed";
				} else if (eventName.toString().endsWith("edit")) {
					verb = "edited";
				} else if (eventName.toString().equalsIgnoreCase("comment.create")) {
					verb = "commented";
				} else if (eventName.toString().endsWith("login")) {
					verb = "loggedIn";
				} else if (eventName.toString().endsWith("register")) {
					verb = "registered";
				} else if (eventName.toString().endsWith("load")) {
					verb = "loaded";
				} else if (eventName.toString().equalsIgnoreCase("profile.action")) {
					if (activityJsonObject.get("actionType").toString().endsWith("edit")) {
						verb = "edited";
					} else {
						verb = "visited";
					}
				}
				if (StringUtils.isNotBlank(verb)) {
					verbAsMap.put("id", "www.goorulearning.org/exapi/verbs/" + verb);
					Map<String, Object> displayAsMap = new HashMap<String, Object>(1);
					displayAsMap.put("en-US", verb);
					verbAsMap.put("display", displayAsMap);
				}
			} catch (JSONException e) {
				// TODO Error Handling Required
				e.printStackTrace();
			}
		}
		
		public void generateObjectProperty(JSONObject activityJsonObject, Map<String, Object> objectAsMap) throws JSONException {
			Map<String, Object> definitionAsMap = new HashMap<String, Object>(4);
			Map<String, Object> nameAsMap = new HashMap<String, Object>(1);
			String typeName = null;
			if (!activityJsonObject.isNull("gooruOid") && StringUtils.isNotBlank(activityJsonObject.get("gooruOid").toString())) {
				String objectType = null;
				// TODO Add condition to differentiate Agent/ Statement/ Activity/ StatementRef
				objectType = "Activity";
				objectAsMap.put("objectType", objectType);
				objectAsMap.put("id", activityJsonObject.get("gooruOid"));
				if (!activityJsonObject.isNull("typeName") && StringUtils.isNotBlank(activityJsonObject.get("typeName").toString())) {
					typeName = activityJsonObject.get("typeName").toString();
					if (typeName.matches(RESOURCE_TYPES)) {
						typeName = "resource";
					} else if (typeName.matches(COLLECTION_TYPES)) {
						typeName = "collection";
					} else if (typeName.matches(QUESTION_TYPES)) {
						typeName = "question";
					}
					
				}
			} else if((!activityJsonObject.isNull("userOrganizationUId") && StringUtils.isNotBlank(activityJsonObject.get("userOrganizationUId").toString()) && (!activityJsonObject.isNull("eventName") && StringUtils.isNotBlank(activityJsonObject.get("eventName").toString())))){
				String eventName = activityJsonObject.get("eventName").toString();
				String userOrganizationUId = activityJsonObject.get("userOrganizationUId").toString();
				if (eventName.toString().equalsIgnoreCase("user.login") || eventName.toString().equalsIgnoreCase("user.register")) {
					objectAsMap.put("objectType", "Agent");
					typeName = "application";
					nameAsMap.put("en-US","Gooru Application");
					if (userOrganizationUId.equalsIgnoreCase("4261739e-ccae-11e1-adfb-5404a609bd14")) {
						objectAsMap.put("id", userOrganizationUId);
					}
				}
			}
			if (StringUtils.isNotBlank(typeName)) {
				definitionAsMap.put("type", typeName);
			}
			if(!nameAsMap.isEmpty()) {
				definitionAsMap.put("name", nameAsMap);
			}
			if(!definitionAsMap.isEmpty()) {
				objectAsMap.put("definition", definitionAsMap);
			}
		}
		
		public void generateContextProperty(JSONObject activityJsonObject, Map<String, Object> contextAsMap) {
			try {
				if (!activityJsonObject.isNull("parentGooruId") && StringUtils.isNotBlank(activityJsonObject.get("parentGooruId").toString())) {
					List<Map<String, Object>> parentList = new ArrayList<Map<String, Object>>();
					Map<String, Object> parentAsMap = new HashMap<String, Object>(1);
					parentAsMap.put("id", activityJsonObject.get("parentGooruId").toString());

					if (!activityJsonObject.isNull("parentEventId") && StringUtils.isNotBlank(activityJsonObject.get("parentEventId").toString())) {
						parentAsMap.put("id", activityJsonObject.get("parentEventId").toString());
						parentAsMap.put("objectType", "StatementRef");
						parentList.add(parentAsMap);
					}
					if (parentList.size() > 0) {
						contextAsMap.put("parent", parentList);
					}
				}
			} catch (JSONException e) {
				// TODO Error Handling Required
				e.printStackTrace();
			}
		}

		public void generateResultProperty(JSONObject activityJsonObject, Map<String, Object> resultAsMap) {
			Map<String, Object> responseAsMap = new HashMap<String, Object>(1);
			try {
				String eventName = activityJsonObject.get("eventName").toString();
				if ((eventName.toString().equalsIgnoreCase("item.review") || eventName.toString().equalsIgnoreCase("comment.create"))
						&& (!activityJsonObject.isNull("text") && StringUtils.isNotBlank(activityJsonObject.get("text").toString()))) {
					resultAsMap.put("response", activityJsonObject.get("text"));
				}
				if (eventName.toString().equalsIgnoreCase("item.rate") && (!activityJsonObject.isNull("rate") && StringUtils.isNotBlank(activityJsonObject.get("rate").toString()))) {
					responseAsMap.put("response", activityJsonObject.get("rate"));
				}
				if (eventName.toString().equalsIgnoreCase("reaction.create") && (!activityJsonObject.isNull("reactionType") && StringUtils.isNotBlank(activityJsonObject.get("reactionType").toString()))) {
					responseAsMap.put("response", activityJsonObject.get("reactionType"));
				}
				if (!responseAsMap.isEmpty()) {
					resultAsMap.put("response", responseAsMap);
				}

				if (!activityJsonObject.isNull("totalTimeSpentInMs") && StringUtils.isNotBlank(activityJsonObject.get("totalTimeSpentInMs").toString())) {
					try {
						if (Long.valueOf(activityJsonObject.get("totalTimeSpentInMs").toString()) < 1800000) {
							resultAsMap.put("duration", new Period((long) Long.valueOf(activityJsonObject.get("totalTimeSpentInMs").toString())));
						} else {
							resultAsMap.put("duration", new Period((long) 1800000));
						}
					} catch (NumberFormatException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (JSONException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (!activityJsonObject.isNull("score") && StringUtils.isNotBlank(activityJsonObject.get("score").toString())) {
						Map<String, Object> rawScoreAsMap = new HashMap<String, Object>(1);
						rawScoreAsMap.put("raw", Long.valueOf(activityJsonObject.get("score").toString()));
						resultAsMap.put("score", rawScoreAsMap);
					}
				}
			} catch (JSONException e1) {
				// TODO Error Handling Required
				e1.printStackTrace();
			}
		}
}

 
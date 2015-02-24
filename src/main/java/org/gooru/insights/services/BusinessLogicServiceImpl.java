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
import org.gooru.insights.constants.ErrorConstants;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsFilterFieldsDTO;
import org.gooru.insights.models.RequestParamsPaginationDTO;
import org.gooru.insights.models.RequestParamsSortDTO;
import org.gooru.insights.models.ResponseParamDTO;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

@Service
public class BusinessLogicServiceImpl implements BusinessLogicService{

	private Logger logger = LoggerFactory.getLogger(BusinessLogicServiceImpl.class);
	
	@Autowired
	private BaseConnectionService baseConnectionService;
	
	@Autowired
	private BaseAPIService baseAPIService;

	/**
	 * This function will build the aggregate bucket
	 * @param index This is the source index name
	 * @param RequestParamDTO is the client request
	 * @param searchRequestBuilder is the search query request
	 * @param metricsName is the name of metric functions
	 * @throws unable to build the bucket
	 */
	public void buildBuckets(String index, RequestParamsDTO requestParamsDTO, SearchRequestBuilder searchRequestBuilder, Map<String, String> metricsName) {

		try {
			TermsBuilder termBuilder = null;
			String[] groupBy = requestParamsDTO.getGroupBy().split(APIConstants.COMMA);

			for (int i = groupBy.length - 1; i >= 0; i--) {
				String fieldName = esFields(index, groupBy[i]);
				TermsBuilder tempBuilder = null;
				if (termBuilder != null) {
					tempBuilder = AggregationBuilders.terms(groupBy[i]).field(fieldName);
					tempBuilder.subAggregation(termBuilder);
					termBuilder = tempBuilder;
				} else {
					termBuilder = AggregationBuilders.terms(groupBy[i]).field(fieldName);
				}
				if (i == groupBy.length - 1) {
					bucketAggregation(index, requestParamsDTO, termBuilder, metricsName);
					termBuilder.size(0);
				}
			}
			if (baseAPIService.checkNull(requestParamsDTO.getFilter())) {
				FilterAggregationBuilder filterBuilder = null;
				if (filterBuilder == null) {
					filterBuilder = includeFilterAggregate(index, requestParamsDTO.getFilter());
				}
				if (termBuilder != null) {
					termBuilder.size(0);
					filterBuilder.subAggregation(termBuilder);
				}
				searchRequestBuilder.addAggregation(filterBuilder);
			} else {
				termBuilder.size(0);
				searchRequestBuilder.addAggregation(termBuilder);
			}
		} catch (Exception e) {
			logger.error(ErrorConstants.BUCKET_ERROR.replace(ErrorConstants.REPLACER,ErrorConstants.AGGREGATION_BUCKET )+e);
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
	public void buildGranularityBuckets(String index, RequestParamsDTO requestParamsDTO, SearchRequestBuilder searchRequestBuilder, Map<String, String> metricsName,
			Map<String, Boolean> validatedData) {
		try {

			TermsBuilder termBuilder = null;
			DateHistogramBuilder dateHistogram = null;
			String[] groupBy = requestParamsDTO.getGroupBy().split(APIConstants.COMMA);
			boolean isFirstDateHistogram = false;

			for (int i = groupBy.length -1; i >=0; i--) {

				TermsBuilder tempBuilder = null;
				String groupByName = esFields(index, groupBy[i]);
				if (baseConnectionService.getFieldsDataType().containsKey(groupBy[i])
						&& baseConnectionService.getFieldsDataType().get(groupBy[i]).equalsIgnoreCase(APIConstants.LogicalConstants.DATE.value())) {
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
						bucketAggregation(index, requestParamsDTO, termBuilder, metricsName);
						includeOrder(requestParamsDTO, validatedData, groupBy[i], termBuilder, null, metricsName);
						termBuilder.size(0);
					}
				}
				if (i == groupBy.length - 1 && isFirstDateHistogram) {
					if (dateHistogram != null) {
						granularityBucketAggregation(index, requestParamsDTO, dateHistogram, metricsName);
						includeOrder(requestParamsDTO, validatedData, groupBy[i], null, dateHistogram, metricsName);
					}
				}
			}
			if (baseAPIService.checkNull(requestParamsDTO.getFilter())) {
				FilterAggregationBuilder filterBuilder = null;
				if (filterBuilder == null) {
					filterBuilder = includeFilterAggregate(index, requestParamsDTO.getFilter());
				}
				if (isFirstDateHistogram) {
					filterBuilder.subAggregation(dateHistogram);
				} else {
					termBuilder.size(0);
					filterBuilder.subAggregation(termBuilder);
				}
				searchRequestBuilder.addAggregation(filterBuilder);
			} else {
				termBuilder.size(0);
				searchRequestBuilder.addAggregation(termBuilder);
			}
		} catch (Exception e) {
			logger.error(ErrorConstants.BUCKET_ERROR.replace(ErrorConstants.REPLACER,ErrorConstants.GRANULARITY_BUCKET )+e);
		}
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
					String fieldName = esFields(index, jsonObject.getString(requestValue));
					includeBucketAggregation(termBuilder, jsonObject, jsonObject.getString(APIConstants.FormulaFields.FORMULA.getField()), requestValue, fieldName);
					metricsName.put(jsonObject.getString(APIConstants.FormulaFields.NAME.getField()) != null ? jsonObject.getString(APIConstants.FormulaFields.NAME.getField()) : fieldName,
							requestValue);
				}
			} catch (Exception e) {
				logger.error(ErrorConstants.AGGREGATION_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.AGGREGATION_BUCKET)+e);
			}
		}
	}
	
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
					String fieldName = esFields(index, jsonObject.get(requestValues).toString());
					includeGranularityAggregation(dateHistogramBuilder, jsonObject, jsonObject.getString(APIConstants.FormulaFields.FORMULA.getField()), requestValues, fieldName);
					metricsName.put(jsonObject.getString(APIConstants.FormulaFields.NAME.getField()) != null ? jsonObject.getString(APIConstants.FormulaFields.NAME.getField()) : fieldName,
							requestValues);
				}
			} catch (Exception e) {
				logger.error(ErrorConstants.AGGREGATION_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.GRANULARITY_BUCKET)+e);
			}
		}
	}
	
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
			}
		} catch (Exception e) {
			logger.error(ErrorConstants.AGGREGATOR_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.AGGREGATION_BUCKET)+e);
		} 
	}
	
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
			}
		} catch (Exception e) {
			logger.error(ErrorConstants.AGGREGATOR_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.GRANULARITY_BUCKET)+e);
		} 
		}

	public BoolFilterBuilder customFilter(String index, Map<String, Object> filterData, Set<String> userFilter) {

		BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();

		Set<String> keys = filterData.keySet();
		Map<String, String> supportFilters = baseConnectionService.getFieldsJoinCache().get(index);
		Set<String> supportKeys = supportFilters.keySet();
		String supportKey = APIConstants.EMPTY;
		for (String key : supportKeys) {
			if (baseAPIService.checkNull(supportKey)) {
				supportKey += APIConstants.COMMA;
			}
			supportKey = key;
		}
		for (String key : keys) {
			if (supportKey.contains(key)) {
				userFilter.add(key);
				Set<Object> data = (Set<Object>) filterData.get(key);
				if (!data.isEmpty()) {
					boolFilter.must(FilterBuilders.inFilter(esFields(index, key), baseAPIService.convertSettoArray(data)));
				}
			}
		}
		return boolFilter;
	}

	public BoolFilterBuilder includeBucketFilter(String index, List<RequestParamsFilterDetailDTO> requestParamsFiltersDetailDTO) {

		BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
		if (requestParamsFiltersDetailDTO != null) {
			for (RequestParamsFilterDetailDTO fieldData : requestParamsFiltersDetailDTO) {
				if (fieldData != null) {
					List<RequestParamsFilterFieldsDTO> requestParamsFilterFieldsDTOs = fieldData.getFields();
					AndFilterBuilder andFilter = null;
					OrFilterBuilder orFilter = null;
					NotFilterBuilder notFilter = null;
					for (RequestParamsFilterFieldsDTO fieldsDetails : requestParamsFilterFieldsDTOs) {

						String fieldName = esFields(index, fieldsDetails.getFieldName());
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
	
	private FilterAggregationBuilder includeFilterAggregate(String index, List<RequestParamsFilterDetailDTO> requestParamsFiltersDetailDTO) {
		FilterAggregationBuilder filterBuilder = new FilterAggregationBuilder(APIConstants.EsFilterFields.FILTERS.field());
		if (requestParamsFiltersDetailDTO != null) {
			BoolFilterBuilder boolFilter = includeBucketFilter(index, requestParamsFiltersDetailDTO);
			filterBuilder.filter(boolFilter);
		}
		return filterBuilder;
	}

	private Object checkDataType(String value, String valueType, String dateformat) {

		SimpleDateFormat format = new SimpleDateFormat(APIConstants.DEFAULT_FORMAT);

		if (baseAPIService.checkNull(dateformat)) {
			try {
				format = new SimpleDateFormat(dateformat);
			} catch (Exception e) {

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
				logger.error(ErrorConstants.INVALID_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.DATA_TYPE));
				return value.toString();
			}
		}
		return Integer.valueOf(value);
	}
		
	public String esFields(String index, String fields) {
		Map<String, String> mappingfields = baseConnectionService.getFields().get(index);
		StringBuffer esFields = new StringBuffer();
		for (String field : fields.split(APIConstants.COMMA)) {
			if (esFields.length() > 0) {
				esFields.append(APIConstants.COMMA);
			}
			if (mappingfields.containsKey(field)) {
				esFields.append(mappingfields.get(field));
			} else {
				esFields.append(field);
			}
		}
		return esFields.toString();
	}
		
	private DateHistogramBuilder generateDateHistogram(String granularity, String fieldName, String field) {

		String format = APIConstants.DateFormats.DEFAULT.format();
		if (baseAPIService.checkNull(granularity)) {
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
			} else if (granularity.matches(APIConstants.DateHistory.D_CHECKER.replace())) {
				int days = new Integer(granularity.replaceFirst(APIConstants.DateHistory.D_REPLACER.replace(), APIConstants.EMPTY));
				format = APIConstants.DateFormats.D.format();
				interval = DateHistogram.Interval.days(days);
			} else if (granularity.matches(APIConstants.DateHistory.W_CHECKER.replace())) {
				int weeks = new Integer(granularity.replaceFirst(APIConstants.DateHistory.W_REPLACER.replace(), APIConstants.EMPTY));
				format = APIConstants.DateFormats.W.name();
				interval = DateHistogram.Interval.weeks(weeks);
			} else if (granularity.matches(APIConstants.DateHistory.H_CHECKER.replace())) {
				int hours = new Integer(granularity.replaceFirst(APIConstants.DateHistory.H_REPLACER.replace(), APIConstants.EMPTY));
				format = APIConstants.DateFormats.H.name();
				interval = DateHistogram.Interval.hours(hours);
			} else if (granularity.matches(APIConstants.DateHistory.K_CHECKER.replace())) {
				int minutes = new Integer(granularity.replaceFirst(APIConstants.DateHistory.K_REPLACER.replace(), APIConstants.EMPTY));
				format = APIConstants.DateFormats.K.format();
				interval = DateHistogram.Interval.minutes(minutes);
			} else if (granularity.matches(APIConstants.DateHistory.S_CHECKER.replace())) {
				int seconds = new Integer(granularity.replaceFirst(APIConstants.DateHistory.S_REPLACER.replace(), APIConstants.EMPTY));
				interval = DateHistogram.Interval.seconds(seconds);
			}
			DateHistogramBuilder dateHistogram = AggregationBuilders.dateHistogram(fieldName).field(field).interval(interval).format(format);
			return dateHistogram;
		}
		return null;
	}

	public List<Map<String, Object>> customizeJSON(String[] groupBy, String resultData, Map<String, String> metrics, boolean hasFilter,ResponseParamDTO<Map<String,Object>> responseParamDTO, int limit) {

		List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
		try {
			int counter = 0;
			Integer totalRows = 0;
			JSONObject json = new JSONObject(resultData);
			json = new JSONObject(json.get(APIConstants.FormulaFields.AGGREGATIONS.getField()).toString());
			if (hasFilter) {
				json = new JSONObject(json.get(APIConstants.FormulaFields.FILTERS.getField()).toString());
			}

			while (counter < groupBy.length) {
				if (json.length() > 0) {
					JSONObject requestJSON = new JSONObject(json.get(groupBy[counter]).toString());
					JSONArray jsonArray = new JSONArray(requestJSON.get(APIConstants.FormulaFields.BUCKETS.getField()).toString());
					if (counter == 0) {
						totalRows = jsonArray.length();
						Map<String,Object> dataMap = new HashMap<String, Object>();
						dataMap.put(APIConstants.FormulaFields.TOTAL_ROWS.getField(), totalRows);
						responseParamDTO.setPaginate(dataMap);
					}
					JSONArray subJsonArray = new JSONArray();
					Set<Object> keys = new HashSet<Object>();
					boolean hasSubAggregate = false;

					for (int i = 0; i < jsonArray.length(); i++) {
						JSONObject newJson = new JSONObject(jsonArray.get(i).toString());
						Object key = newJson.get(APIConstants.FormulaFields.KEY.getField());
						keys.add(key);
						if (counter == 0 && limit == i) {
							break;
						}
						if (counter + 1 == (groupBy.length)) {
							fetchMetrics(newJson, dataList, metrics, groupBy, counter);
						} else {
							hasSubAggregate = true;
							iterateInternalObject(newJson, subJsonArray, groupBy, counter, key);
						}
					}

					if (hasSubAggregate) {
						json = new JSONObject();
						requestJSON.put(APIConstants.FormulaFields.BUCKETS.getField(), subJsonArray);
						json.put(groupBy[counter + 1], requestJSON);
					}

				}
				counter++;
			}
		} catch (JSONException e) {
			logger.error(ErrorConstants.INVALID_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.DATA_TYPE));
		}
		return dataList;
	}
		
	private void fetchMetrics(JSONObject newJson, List<Map<String, Object>> dataList, Map<String, String> metrics, String[] groupBy, Integer counter) throws JSONException {
		Map<String, Object> resultMap = new LinkedHashMap<String, Object>();
		for (Map.Entry<String, String> entry : metrics.entrySet()) {
			if (newJson.has(entry.getValue())) {
				resultMap.put(entry.getKey(), new JSONObject(newJson.get(entry.getValue()).toString()).get("value"));
				resultMap.put(groupBy[counter], newJson.get(APIConstants.FormulaFields.KEY.getField()));
				newJson.remove(entry.getValue());
			}
		}
		newJson.remove(APIConstants.FormulaFields.DOC_COUNT.getField());
		newJson.remove(APIConstants.FormulaFields.KEY_AS_STRING.getField());
		newJson.remove(APIConstants.FormulaFields.KEY.getField());
		newJson.remove(APIConstants.FormulaFields.BUCKETS.getField());
		Iterator<String> dataKeys = newJson.sortedKeys();
		while (dataKeys.hasNext()) {
			String dataKey = dataKeys.next();
			resultMap.put(dataKey, newJson.get(dataKey));
		}
		dataList.add(resultMap);
	}
		
	private void iterateInternalObject(JSONObject newJson, JSONArray subJsonArray, String[] groupBy, Integer counter, Object key) throws JSONException {
		JSONArray tempArray = new JSONArray();
		JSONObject dataJson = new JSONObject(newJson.get(groupBy[counter + 1]).toString());
		tempArray = new JSONArray(dataJson.get(APIConstants.FormulaFields.BUCKETS.getField()).toString());
		for (int j = 0; j < tempArray.length(); j++) {
			JSONObject subJson = new JSONObject(tempArray.get(j).toString());
			subJson.put(groupBy[counter], key);
			newJson.remove(groupBy[counter + 1]);
			newJson.remove(APIConstants.FormulaFields.DOC_COUNT.getField());
			newJson.remove(APIConstants.FormulaFields.KEY_AS_STRING.getField());
			newJson.remove(APIConstants.FormulaFields.KEY.getField());
			newJson.remove(APIConstants.FormulaFields.BUCKETS.getField());
			Iterator<String> dataKeys = newJson.sortedKeys();
			while (dataKeys.hasNext()) {
				String dataKey = dataKeys.next();
				subJson.put(dataKey, newJson.get(dataKey));
			}

			subJsonArray.put(subJson);
		}
	}
		
	public List<Map<String, Object>> formDataList(Map<Integer, Map<String, Object>> requestMap) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		for (Map.Entry<Integer, Map<String, Object>> entry : requestMap.entrySet()) {
			resultList.add(entry.getValue());
		}
		return resultList;
	}

		  public Map<Integer,Map<String,Object>> processAggregateJSON(String groupBy,String resultData,Map<String,String> metrics,boolean hasFilter){

			Map<Integer,Map<String,Object>> dataMap = new LinkedHashMap<Integer,Map<String,Object>>();
			try {
				int counter=0;
				String[] fields = groupBy.split(APIConstants.COMMA);
				JSONObject json = new JSONObject(resultData);
				json = new JSONObject(json.get(APIConstants.FormulaFields.AGGREGATIONS.getField()).toString());
				if(hasFilter){
					json = new JSONObject(json.get(APIConstants.FormulaFields.FILTERS.getField()).toString());
				}
				while(counter < fields.length){
					JSONObject requestJSON = new JSONObject(json.get(APIConstants.FormulaFields.FIELD.getField()+counter).toString());
				JSONArray jsonArray = new JSONArray(requestJSON.get(APIConstants.FormulaFields.BUCKETS.getField()).toString());
				JSONArray subJsonArray = new JSONArray();
				boolean hasSubAggregate = false;
				for(int i=0;i<jsonArray.length();i++){
					JSONObject newJson = new JSONObject(jsonArray.get(i).toString());
					Object key=newJson.get(APIConstants.FormulaFields.KEY.getField());
						if(counter+1 == (fields.length)){
						Map<String,Object> resultMap = new LinkedHashMap<String,Object>();
						boolean processed = false;
						for(Map.Entry<String,String> entry : metrics.entrySet()){
							if(newJson.has(entry.getValue())){
								resultMap.put(entry.getKey(), new JSONObject(newJson.get(entry.getValue()).toString()).get("value"));
								resultMap.put(fields[counter], newJson.get(APIConstants.FormulaFields.KEY.getField()));
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
						newJson = new JSONObject(newJson.get(APIConstants.FormulaFields.FIELD.getField()+(counter+1)).toString());
						tempArray = new JSONArray(newJson.get(APIConstants.FormulaFields.BUCKETS.getField()).toString());
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
					requestJSON.put(APIConstants.FormulaFields.BUCKETS.getField(), subJsonArray);
					json.put(APIConstants.FormulaFields.FIELD.getField()+(counter+1), requestJSON);
				}
				
				counter++;
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return dataMap;
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
		
	public List<Map<String, Object>> leftJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, Set<String> keys) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> parentEntry : parent) {
			boolean occured = false;
			Map<String, Object> appended = new HashMap<String, Object>();
			for (Map<String, Object> childEntry : child) {
				boolean validated = false;
				for (String key : keys) {
					if (childEntry.containsKey(key) && parentEntry.containsKey(key)) {
						if (childEntry.get(key).toString().equals(parentEntry.get(key).toString())) {
						} else {
							validated = true;
						}
					} else {
						validated = true;
					}
				}
				if (!validated) {
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

	public Map<String, Object> fetchFilters(String index, List<Map<String, Object>> dataList) {
		Map<String, String> filterFields = new HashMap<String, String>();
		Map<String, Object> filters = new HashMap<String, Object>();
		if (baseConnectionService.getFieldsJoinCache().containsKey(index)) {
			filterFields = baseConnectionService.getFieldsJoinCache().get(index);
		}
		for (Map<String, Object> dataMap : dataList) {
			Set<String> keySets = filterFields.keySet();
			for (String keySet : keySets) {
				for (String key : keySet.split(APIConstants.COMMA)) {
					if (dataMap.containsKey(key)) {
						buildFilter(dataMap, filters, key);
					}
				}
			}
		}
		return filters;
	}
		
	private void buildFilter(Map<String, Object> dataMap, Map<String, Object> filters, String key) {
		if (!filters.isEmpty() && filters.containsKey(key)) {
			Set<Object> filterValue = (Set<Object>) filters.get(key);
			try {
				Set<Object> datas = (Set<Object>) dataMap.get(key);
				for (Object data : datas) {
					filterValue.add(data);
				}
			} catch (Exception e) {
				filterValue.add(dataMap.get(key));
			}
			filters.put(key, filterValue);
		} else {
			Set<Object> filterValue = new HashSet<Object>();
			try {
				Set<Object> datas = (Set<Object>) dataMap.get(key);
				for (Object data : datas) {
					filterValue.add(data);
				}
			} catch (Exception e) {
				filterValue.add(dataMap.get(key));
			}
			filters.put(key, filterValue);
		}
	}
		
	public JSONArray convertJSONArray(List<Map<String, Object>> data) {
		try {
			Gson gson = new Gson();
			Type listType = new TypeToken<List<Map<String, Object>>>() {
			}.getType();
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
				if(validatedData.get(APIConstants.Hasdatas.HAS_SORTBY.check())){
					List<RequestParamsSortDTO> orderDatas = requestParamsPaginationDTO.getOrder();
					for(RequestParamsSortDTO sortData : orderDatas){
						baseAPIService.sortBy(data, sortData.getSortBy(), sortData.getSortOrder());
					}
				}
				int offset = validatedData.get(APIConstants.Hasdatas.HAS_Offset.check()) ? requestParamsPaginationDTO.getOffset() == 0 ? 0 : requestParamsPaginationDTO.getOffset() -1 : 0; 
				int limit = validatedData.get(APIConstants.Hasdatas.HAS_LIMIT.check()) ? requestParamsPaginationDTO.getLimit() == 0 ? 1 : requestParamsPaginationDTO.getLimit() : 10; 
				
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
				int offset = validatedData.get(APIConstants.Hasdatas.HAS_Offset.check()) ? requestParamsPaginationDTO.getOffset() : 0; 
				int limit = validatedData.get(APIConstants.Hasdatas.HAS_LIMIT.check()) ? requestParamsPaginationDTO.getLimit() : 10; 
				
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
				if(validatedData.get(APIConstants.Hasdatas.HAS_SORTBY.check())){
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
				json = new JSONObject(json.get(APIConstants.FormulaFields.HITS.getField()).toString());
			JSONArray hitsArray = new JSONArray(json.get(APIConstants.FormulaFields.HITS.getField()).toString());
			
			for(int i=0;i<hitsArray.length();i++){
				Map<String,Object> resultMap = new HashMap<String,Object>();
			JSONObject getSourceJson = new JSONObject(hitsArray.get(i).toString());
			getSourceJson = new JSONObject(getSourceJson.get(APIConstants.FormulaFields.SOURCE.getField()).toString());
			if(baseAPIService.checkNull(fields)){
			for(String field : fields.split(APIConstants.COMMA)){
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
				mainJson = new JSONObject(mainJson.get(APIConstants.FormulaFields.HITS.getField()).toString());
				JSONArray jsonArray = new JSONArray(mainJson.get(APIConstants.FormulaFields.HITS.getField()).toString());
				for(int i=0;i<jsonArray.length();i++){
					 mainJson = new JSONObject(jsonArray.get(i).toString());
					 Map<String,Object> dataMap = new HashMap<String,Object>();	 
					 dataMap = gson.fromJson(mainJson.getString(APIConstants.FormulaFields.SOURCE.getField()),mapType);
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
		//valid
		public List<Map<String,Object>> getRecords(String index,ResponseParamDTO<Map<String,Object>> responseParamDTO,String data,String dataKey) throws Exception{
			JSONObject json;
			JSONArray jsonArray = new JSONArray();
			List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
			try {
				json = new JSONObject(data);
				json = new JSONObject(json.get(APIConstants.FormulaFields.HITS.getField()).toString());
				int totalRows = json.getInt(APIConstants.FormulaFields.TOTAL.getField());
				
				Map<String,Object> dataMap = new HashMap<String, Object>();
				dataMap.put(APIConstants.FormulaFields.TOTAL_ROWS.getField(), totalRows);
				if(responseParamDTO != null){
				responseParamDTO.setPaginate(dataMap);
				}
				jsonArray = new JSONArray(json.get(APIConstants.FormulaFields.HITS.getField()).toString());
				if(!APIConstants.FormulaFields.FIELDS.getField().equalsIgnoreCase(dataKey)){
				for(int i =0;i< jsonArray.length();i++){
					json = new JSONObject(jsonArray.get(i).toString());
					JSONObject fieldJson = new JSONObject(json.get(dataKey).toString());
					Iterator<String> keys = fieldJson.keys();
					Map<String,Object> resultMap = new HashMap<String, Object>();
					while(keys.hasNext()){
						String key =keys.next();
						if(baseConnectionService.getDependentFieldsCache().containsKey(index)){
							/**
							 * Perform Group concat operation
							 */
							includeGroupConcat(index, key, fieldJson, fieldJson, resultMap);
						}else{
							try{
								JSONArray dataArray = new JSONArray(fieldJson.get(key).toString());
								if(dataArray.length() == 1){	 
									resultMap.put(apiFields(index,key),dataArray.get(0));
								}else{
									Object[] arrayData = new Object[dataArray.length()];
									for(int j=0;j < dataArray.length();j++){
										arrayData[j]=dataArray.get(j);
									}
									resultMap.put(apiFields(index,key),arrayData);
								}
							}catch(Exception e){
								
								resultMap.put(apiFields(index,key), fieldJson.get(key));
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
						 if(baseConnectionService.getDependentFieldsCache().containsKey(index)){
								Map<String,Map<String,String>> dependentKey = baseConnectionService.getDependentFieldsCache().get(index);	
								
								if(dependentKey.containsKey(key)){
									Map<String,String> dependentColumn = dependentKey.get(key);
									Set<String> columnKeys = dependentColumn.keySet();
									for(String columnKey : columnKeys){
										if(!columnKey.equalsIgnoreCase(APIConstants.FIELD_NAME) && !columnKey.equalsIgnoreCase(APIConstants.DEPENDENT_NAME)){
											if(columnKey.equalsIgnoreCase(new JSONArray(json.getString(dependentColumn.get(APIConstants.DEPENDENT_NAME))).getString(0))){
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
							resultMap.put(apiFields(index,key),fieldJsonArray.get(0));
						}else{
							Set<Object> arrayData = new HashSet<Object>();
							for(int j=0;j < fieldJsonArray.length();j++){
								arrayData.add(fieldJsonArray.get(j));
							}
							if(arrayData.size() == 1){
								for(Object dataObject : arrayData){
									resultMap.put(apiFields(index,key),dataObject);
									
								}
							}else{
								
								resultMap.put(apiFields(index,key),arrayData);
							}
						}
								}catch(Exception e){
									resultMap.put(apiFields(index,key), json.get(key));
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
		
		private void includeGroupConcat(String index,String key,JSONObject fieldJson,JSONObject json,Map<String,Object> resultMap) throws Exception{
			Map<String,Map<String,String>> dependentKey = baseConnectionService.getDependentFieldsCache().get(index);	
			if(dependentKey.containsKey(key)){
				Map<String,String> dependentColumn = dependentKey.get(key);
				Set<String> columnKeys = dependentColumn.keySet();
				for(String columnKey : columnKeys){
					if(!columnKey.equalsIgnoreCase(APIConstants.FIELD_NAME) && !columnKey.equalsIgnoreCase(APIConstants.DEPENDENT_NAME)){
						if(columnKey.equalsIgnoreCase(fieldJson.getString(dependentColumn.get(APIConstants.DEPENDENT_NAME)))){
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
		}
		public List<Map<String,Object>> getMultiGetRecords(String[] indices,Map<String,Map<String,String>> comparekey,String data,Map<Integer,String> errorRecord,String dataKey){
			JSONObject json;
			JSONArray jsonArray = new JSONArray();
			List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
			try {
				json = new JSONObject(data);
				json = new JSONObject(json.get(APIConstants.FormulaFields.HITS.getField()).toString());
				jsonArray = new JSONArray(json.get(APIConstants.FormulaFields.HITS.getField()).toString());
				if(!APIConstants.FormulaFields.FIELDS.getField().equalsIgnoreCase(dataKey)){
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
			for(String field : fields.split(APIConstants.COMMA)){
				if(esFields.length() > 0){
					esFields.append(APIConstants.COMMA);
				}
				if(apiFields.containsKey(field)){
					esFields.append(apiFields.get(field));
				}else{
					esFields.append(field);
				}
			}
			return esFields.toString();
		}
		
	public RequestParamsDTO changeDataSourceUserToAnonymousUser(RequestParamsDTO requestParamsDTO) {
		String dataSources = APIConstants.EMPTY;
		for (String dataSource : requestParamsDTO.getDataSource().split(APIConstants.COMMA)) {
			if (dataSources.length() > 0) {
				dataSources += APIConstants.COMMA;
			}
			if (dataSource.matches(APIConstants.USERDATASOURCES)) {
				dataSources += APIConstants.ANONYMOUS_USERDATA_SOURCE;
			} else {
				dataSources += dataSource;
			}
		}
		requestParamsDTO.setDataSource(dataSources);
		return requestParamsDTO;
	}
}

 
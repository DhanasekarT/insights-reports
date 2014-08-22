package org.gooru.insights.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsFilterFieldsDTO;
import org.gooru.insights.models.RequestParamsSortDTO;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;

@Service
public class BaseESServiceImpl implements BaseESService {

	@Autowired
	BaseConnectionService baseConnectionService;

	@Autowired
	BaseAPIService baseAPIService;

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

	public String searchData(RequestParamsDTO requestParamsDTO,
			String[] indices, String[] types, String fields,
			QueryBuilder query, FilterBuilder filters, Integer offset,
			Integer limit, Map<String, String> sort) {
		SearchRequestBuilder searchRequestBuilder = getClient().prepareSearch(
				indices).setSearchType(SearchType.DFS_QUERY_AND_FETCH);
		Map<String, Map<Object, Object>> rangeFilter = new HashMap<String, Map<Object, Object>>();
		if (query != null) {
			searchRequestBuilder.setQuery(query);
		}
		if (filters != null) {
		}
		for (Map.Entry<String, Map<Object, Object>> filterMap : rangeFilter
				.entrySet()) {
			Map<Object, Object> valueMap = new HashMap<Object, Object>();
			valueMap = filterMap.getValue();
			for (Map.Entry<Object, Object> map : valueMap.entrySet()) {
				searchRequestBuilder.setPostFilter(FilterBuilders
						.rangeFilter(filterMap.getKey()).from(map.getKey())
						.to(map.getValue()));
			}
		}
		if (!baseAPIService.checkNull(requestParamsDTO.getGroupBy())) {
			addFilters(requestParamsDTO.getFilter(), searchRequestBuilder);
		}
		if (baseAPIService.checkNull(sort)) {
			for (Map.Entry<String, String> map : sort.entrySet()) {
				searchRequestBuilder.addSort(map.getKey(), (map.getValue()
						.equalsIgnoreCase("ASC") ? SortOrder.ASC
						: SortOrder.DESC));
			}
		}
		if (baseAPIService.checkNull(fields)) {
			for (String field : fields.split(",")) {
				searchRequestBuilder.addField(field);
			}
		}
		// pagination not working,may scroll API will help
		searchRequestBuilder = searchRequestBuilder.setFrom(offset);
		searchRequestBuilder = searchRequestBuilder.setSize(limit);

		if (requestParamsDTO.getGroupBy() != null
				&& requestParamsDTO.getFilter() != null
				&& requestParamsDTO.getFilter().get(0) != null
				&& requestParamsDTO.getFilter().get(0)
						.getLogicalOperatorPrefix() != null) {
			filterAggregateFunction(requestParamsDTO, searchRequestBuilder);
		} else {
			aggregateFunction(requestParamsDTO, searchRequestBuilder);
		}
		return searchRequestBuilder.execute().actionGet().toString();

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
				System.out.println("group By size" + groupBy.length);
				for (int j = groupBy.length - 1; j >= 0; j--) {

					if (j == 0 && groupBy.length > 1) {
						termBuilder = new TermsBuilder(groupBy[0])
								.field(groupBy[0]);
						continue;
					} else {
						termBuilder = new TermsBuilder(groupBy[0])
								.field(groupBy[0]);
					}
					TermsBuilder subTermBuilder = new TermsBuilder(groupBy[j])
							.field(groupBy[j]);
					// subTermBuilder.executionHint(groupBy[j]);
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
												subTermBuilder
														.subAggregation(AggregationBuilders
																.sum(jsonObject
																		.get(aggregateName)
																		.toString())
																.field(jsonObject
																		.get(aggregateName)
																		.toString()));
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
				searchRequestBuilder.addAggregation(termBuilder);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return true;
		}
		return false;
	}

	public boolean filterAggregateFunction(RequestParamsDTO requestParamsDTO,
			SearchRequestBuilder searchRequestBuilder) {
		TermsBuilder termBuilder = null;
		TermsBuilder aggregateBuilder = null;
		boolean includedAggregate = false;
		boolean firstEntry = false;
		boolean singleAggregate = false;
		if (requestParamsDTO.getGroupBy() != null) {
			try {
				String[] groupBy = requestParamsDTO.getGroupBy().split(",");
				for (int j = groupBy.length - 1; j >= 0; j--) {

					if (j == 0 && groupBy.length > 1) {
						termBuilder = new TermsBuilder(groupBy[0])
								.field(groupBy[0]);
						continue;
					} else {
						termBuilder = new TermsBuilder(groupBy[0])
								.field(groupBy[0]);
						singleAggregate = true;
					}
					TermsBuilder subTermBuilder = new TermsBuilder(groupBy[j])
							.field(groupBy[j]);
					if (j == groupBy.length - 2) {
						if (includedAggregate) {
							System.out.println("added");
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
								System.out.println("jsonArray Object"
										+ jsonArray.get(i));
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
												if (singleAggregate) {
													FilterAggregationBuilder mainFilter = addFilters(requestParamsDTO
															.getFilter());
													mainFilter
															.subAggregation(AggregationBuilders
																	.sum(jsonObject
																			.get(aggregateName)
																			.toString())
																	.field(jsonObject
																			.get(aggregateName)
																			.toString()));
													termBuilder
															.subAggregation(mainFilter);
												} else {
													FilterAggregationBuilder mainFilter = addFilters(requestParamsDTO
															.getFilter());
													mainFilter
															.subAggregation(AggregationBuilders
																	.sum(jsonObject
																			.get(aggregateName)
																			.toString())
																	.field(jsonObject
																			.get(aggregateName)
																			.toString()));
													subTermBuilder
															.subAggregation(mainFilter);
												}
											}

										}
									}
								}
							}
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
				searchRequestBuilder.addAggregation(termBuilder);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return true;
		}
		return false;
	}

	public boolean duplicateFilterAggregateFunction(
			RequestParamsDTO requestParamsDTO,
			SearchRequestBuilder searchRequestBuilder) {
		TermsBuilder termBuilder = null;
		TermsBuilder aggregateBuilder = null;
		boolean includedAggregate = false;
		boolean firstEntry = false;
		if (requestParamsDTO.getGroupBy() != null) {
			try {
				String[] groupBy = requestParamsDTO.getGroupBy().split(",");
				for (int j = groupBy.length - 1; j >= 0; j--) {

					if (j == 0 && groupBy.length > 1) {
						termBuilder = new TermsBuilder(groupBy[0])
								.field(groupBy[0]);
						continue;
					} else {
						termBuilder = new TermsBuilder(groupBy[0])
								.field(groupBy[0]);
					}
					TermsBuilder subTermBuilder = new TermsBuilder(groupBy[j])
							.field(groupBy[j]);
					// subTermBuilder.executionHint(groupBy[j]);
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
												FilterAggregationBuilder mainFilter = addFilters(requestParamsDTO
														.getFilter());
												mainFilter
														.subAggregation(AggregationBuilders
																.sum(jsonObject
																		.get(aggregateName)
																		.toString())
																.field(jsonObject
																		.get(aggregateName)
																		.toString()));
												subTermBuilder
														.subAggregation(mainFilter);
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
				searchRequestBuilder.addAggregation(termBuilder);
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
						if (fieldsDetails.getType()
								.equalsIgnoreCase("selector")) {
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

	public void addFilters(
			List<RequestParamsFilterDetailDTO> requestParamsFiltersDetailDTO,
			SearchRequestBuilder searchRequestBuilder) {
		if (requestParamsFiltersDetailDTO != null) {
			for (RequestParamsFilterDetailDTO fieldData : requestParamsFiltersDetailDTO) {
				if (fieldData != null) {
					FilterBuilder subFilter = null;

					System.out.println("fields not empty");
					List<RequestParamsFilterFieldsDTO> requestParamsFilterFieldsDTOs = fieldData
							.getFields();
					BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
					for (RequestParamsFilterFieldsDTO fieldsDetails : requestParamsFilterFieldsDTOs) {
						System.out.println("value" + fieldsDetails.getValue());
						if (fieldsDetails.getType()
								.equalsIgnoreCase("selector")) {
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
								boolFilter.must(FilterBuilders.termFilter(
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
								System.out.println("le"
										+ fieldsDetails.getValue());
								boolFilter.must(FilterBuilders.rangeFilter(
										fieldsDetails.getFieldName()).lte(
										checkDataType(fieldsDetails.getValue(),
												fieldsDetails.getValueType())));
							} else if (fieldsDetails.getOperator()
									.equalsIgnoreCase("ge")) {
								System.out.println("ge"
										+ fieldsDetails.getValue());
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
						} else {

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

	public Object checkDataType(String value, String valueType) {
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
		}
		return Integer.valueOf(value);
	}

	public Client getClient() {
		return baseConnectionService.getClient();
	}
}

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

import org.gooru.insights.builders.utils.InsightsLogger;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.APIConstants.Hasdatas;
import org.gooru.insights.constants.ErrorConstants;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsPaginationDTO;
import org.gooru.insights.models.RequestParamsSortDTO;
import org.gooru.insights.models.ResponseParamDTO;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

@Service
public class BusinessLogicServiceImpl implements BusinessLogicService {

	@Autowired
	private BaseConnectionService baseConnectionService;

	@Autowired
	private BaseAPIService baseAPIService;

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

	public List<Map<String, Object>> formatAggregateKeyValueJson(List<Map<String, Object>> dataMap, String key) throws org.json.JSONException {

		Map<String, List<Map<String, Object>>> resultMap = new LinkedHashMap<String, List<Map<String, Object>>>();
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> map : dataMap) {
			if (map.containsKey(key)) {
				List<Map<String, Object>> tempList = new ArrayList<Map<String, Object>>();
				String jsonKey = map.get(key).toString();
				map.remove(key);
				if (resultMap.containsKey(jsonKey)) {
					tempList.addAll(resultMap.get(jsonKey));
				}
				tempList.add(map);
				resultMap.put(jsonKey, tempList);
			}
		}
		for (Entry<String, List<Map<String, Object>>> entry : resultMap.entrySet()) {
			Map<String, Object> tempMap = new HashMap<String, Object>();
			tempMap.put(entry.getKey(), entry.getValue());
			resultList.add(tempMap);
		}
		return resultList;
	}

	public List<Map<String, Object>> customPagination(RequestParamsPaginationDTO requestParamsPaginationDTO, List<Map<String, Object>> data, Map<String, Boolean> validatedData) {
		int dataSize = data.size();
		List<Map<String, Object>> customizedData = new ArrayList<Map<String, Object>>();
		if (baseAPIService.checkNull(requestParamsPaginationDTO)) {
			if (validatedData.get(APIConstants.Hasdatas.HAS_SORTBY.check())) {
				List<RequestParamsSortDTO> orderDatas = requestParamsPaginationDTO.getOrder();
				for (RequestParamsSortDTO sortData : orderDatas) {
					baseAPIService.sortBy(data, sortData.getSortBy(), sortData.getSortOrder());
				}
			}
			int offset = validatedData.get(APIConstants.Hasdatas.HAS_Offset.check()) ? requestParamsPaginationDTO.getOffset() == 0 ? 0 : requestParamsPaginationDTO.getOffset() - 1 : 0;
			int limit = validatedData.get(APIConstants.Hasdatas.HAS_LIMIT.check()) ? requestParamsPaginationDTO.getLimit() == 0 ? 1 : requestParamsPaginationDTO.getLimit() : 10;

			if ((limit + offset) < dataSize && offset < dataSize) {
				customizedData = data.subList(offset, offset + limit);
			} else if ((limit + offset) >= dataSize && offset < dataSize) {
				customizedData = data.subList(offset, dataSize);
			} else if ((limit + offset) < dataSize && offset >= dataSize) {
				customizedData = data.subList(0, limit);
			} else if ((limit + offset) >= dataSize && offset >= dataSize) {
				customizedData = data.subList(0, dataSize);
			}
		} else {
			customizedData = data;
		}
		return customizedData;
	}

	public List<Map<String, Object>> aggregatePaginate(RequestParamsPaginationDTO requestParamsPaginationDTO, List<Map<String, Object>> data, Map<String, Boolean> validatedData) {
		int dataSize = data.size();
		List<Map<String, Object>> customizedData = new ArrayList<Map<String, Object>>();
		if (baseAPIService.checkNull(requestParamsPaginationDTO)) {
			int offset = validatedData.get(APIConstants.Hasdatas.HAS_Offset.check()) ? requestParamsPaginationDTO.getOffset() : 0;
			int limit = validatedData.get(APIConstants.Hasdatas.HAS_LIMIT.check()) ? requestParamsPaginationDTO.getLimit() : 10;

			if ((limit + offset) < dataSize && offset < dataSize) {
				customizedData = data.subList(offset, offset + limit);
			} else if ((limit + offset) >= dataSize && offset < dataSize) {
				customizedData = data.subList(offset, dataSize);
			} else if ((limit + offset) < dataSize && offset >= dataSize) {
				customizedData = data.subList(0, limit);
			} else if ((limit + offset) >= dataSize && offset >= dataSize) {
				customizedData = data.subList(0, dataSize);
			}
		} else {
			customizedData = data;
		}
		return customizedData;
	}

	public List<Map<String, Object>> customSort(RequestParamsPaginationDTO requestParamsPaginationDTO, List<Map<String, Object>> data, Map<String, Boolean> validatedData) {
		if (baseAPIService.checkNull(requestParamsPaginationDTO)) {
			if (validatedData.get(APIConstants.Hasdatas.HAS_SORTBY.check())) {
				List<RequestParamsSortDTO> orderDatas = requestParamsPaginationDTO.getOrder();
				for (RequestParamsSortDTO sortData : orderDatas) {
					baseAPIService.sortBy(data, sortData.getSortBy(), sortData.getSortOrder());
				}
			}
		}
		return data;
	}

	public List<Map<String, Object>> getSource(String traceId,String result) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		try {
			Gson gson = new Gson();
			Type mapType = new TypeToken<Map<String, Object>>() {
			}.getType();
			JSONObject mainJson = new JSONObject(result);
			mainJson = new JSONObject(mainJson.get(APIConstants.FormulaFields.HITS.getField()).toString());
			JSONArray jsonArray = new JSONArray(mainJson.get(APIConstants.FormulaFields.HITS.getField()).toString());
			for (int i = 0; i < jsonArray.length(); i++) {
				mainJson = new JSONObject(jsonArray.get(i).toString());
				Map<String, Object> dataMap = new HashMap<String, Object>();
				dataMap = gson.fromJson(mainJson.getString(APIConstants.FormulaFields.SOURCE.getField()), mapType);
				resultList.add(dataMap);
			}
		} catch (JSONException e) {
			InsightsLogger.error(traceId,ErrorConstants.INVALID_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.JSON), e);
		}
		return resultList;
	}

	public List<Map<String, Object>> getRecords(String traceId,String index, ResponseParamDTO<Map<String, Object>> responseParamDTO, String data, String dataKey) throws Exception {
		JSONObject json;
		JSONArray jsonArray = new JSONArray();
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		try {
			json = new JSONObject(data);
			json = new JSONObject(json.get(APIConstants.FormulaFields.HITS.getField()).toString());
			int totalRows = json.getInt(APIConstants.FormulaFields.TOTAL.getField());

			Map<String, Object> dataMap = new HashMap<String, Object>();
			dataMap.put(APIConstants.FormulaFields.TOTAL_ROWS.getField(), totalRows);
			if (responseParamDTO != null) {
				responseParamDTO.setPaginate(dataMap);
			}
			jsonArray = new JSONArray(json.get(APIConstants.FormulaFields.HITS.getField()).toString());
			if (!APIConstants.FormulaFields.FIELDS.getField().equalsIgnoreCase(dataKey)) {
				for (int i = 0; i < jsonArray.length(); i++) {
					json = new JSONObject(jsonArray.get(i).toString());
					JSONObject fieldJson = new JSONObject(json.get(dataKey).toString());
					Iterator<String> keys = fieldJson.keys();
					Map<String, Object> resultMap = new HashMap<String, Object>();
					while (keys.hasNext()) {
						String key = keys.next();
						if (baseConnectionService.getDependentFieldsCache().containsKey(index)) {
							/**
							 * Perform Group concat operation
							 */
							includeGroupConcat(index, key, fieldJson, fieldJson, resultMap);
						} else {
							try {
								JSONArray dataArray = new JSONArray(fieldJson.get(key).toString());
								if (dataArray.length() == 1) {
									resultMap.put(apiFields(index, key), dataArray.get(0));
								} else {
									Object[] arrayData = new Object[dataArray.length()];
									for (int j = 0; j < dataArray.length(); j++) {
										arrayData[j] = dataArray.get(j);
									}
									resultMap.put(apiFields(index, key), arrayData);
								}
							} catch (Exception e) {

								resultMap.put(apiFields(index, key), fieldJson.get(key));
							}
						}
					}
					resultList.add(resultMap);
				}
			} else {
				for (int i = 0; i < jsonArray.length(); i++) {
					Map<String, Object> resultMap = new HashMap<String, Object>();
					json = new JSONObject(jsonArray.get(i).toString());
					json = new JSONObject(json.get(dataKey).toString());
					Iterator<String> keys = json.keys();
					while (keys.hasNext()) {
						String key = keys.next();
						if (baseConnectionService.getDependentFieldsCache().containsKey(index)) {
							Map<String, Map<String, String>> dependentKey = baseConnectionService.getDependentFieldsCache().get(index);

							if (dependentKey.containsKey(key)) {
								Map<String, String> dependentColumn = dependentKey.get(key);
								Set<String> columnKeys = dependentColumn.keySet();
								for (String columnKey : columnKeys) {
									if (!columnKey.equalsIgnoreCase(APIConstants.FIELD_NAME) && !columnKey.equalsIgnoreCase(APIConstants.DEPENDENT_NAME)) {
										if (columnKey.equalsIgnoreCase(new JSONArray(json.getString(dependentColumn.get(APIConstants.DEPENDENT_NAME))).getString(0))) {
											try {
												JSONArray dataArray = new JSONArray(json.get(key).toString());
												if (dataArray.length() == 1) {
													resultMap.put(dependentColumn.get(columnKey), dataArray.get(0));
												} else {
													Object[] arrayData = new Object[dataArray.length()];
													for (int j = 0; j < dataArray.length(); j++) {
														arrayData[j] = dataArray.get(j);
													}
													resultMap.put(dependentColumn.get(columnKey), arrayData);
												}
											} catch (Exception e) {

												resultMap.put(dependentColumn.get(columnKey), json.get(key));
											}
										}
									}
								}

							}
						} else {
							try {
								JSONArray fieldJsonArray = new JSONArray(json.get(key).toString());
								if (fieldJsonArray.length() == 1) {
									resultMap.put(apiFields(index, key), fieldJsonArray.get(0));
								} else {
									Set<Object> arrayData = new HashSet<Object>();
									for (int j = 0; j < fieldJsonArray.length(); j++) {
										arrayData.add(fieldJsonArray.get(j));
									}
									if (arrayData.size() == 1) {
										for (Object dataObject : arrayData) {
											resultMap.put(apiFields(index, key), dataObject);

										}
									} else {

										resultMap.put(apiFields(index, key), arrayData);
									}
								}
							} catch (Exception e) {
								resultMap.put(apiFields(index, key), json.get(key));
							}
						}
					}
					resultList.add(resultMap);
				}
			}
			return resultList;
		} catch (JSONException e) {
			InsightsLogger.error(traceId,ErrorConstants.INVALID_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.JSON), e);
		}
		return resultList;
	}

	private void includeGroupConcat(String index, String key, JSONObject fieldJson, JSONObject json, Map<String, Object> resultMap) throws Exception {
		Map<String, Map<String, String>> dependentKey = baseConnectionService.getDependentFieldsCache().get(index);
		if (dependentKey.containsKey(key)) {
			Map<String, String> dependentColumn = dependentKey.get(key);
			Set<String> columnKeys = dependentColumn.keySet();
			for (String columnKey : columnKeys) {
				if (!columnKey.equalsIgnoreCase(APIConstants.FIELD_NAME) && !columnKey.equalsIgnoreCase(APIConstants.DEPENDENT_NAME)) {
					if (columnKey.equalsIgnoreCase(fieldJson.getString(dependentColumn.get(APIConstants.DEPENDENT_NAME)))) {
						try {
							JSONArray dataArray = new JSONArray(json.get(key).toString());
							if (dataArray.length() == 1) {
								resultMap.put(dependentColumn.get(columnKey), dataArray.get(0));
							} else {
								Set<Object> arrayData = new HashSet<Object>();
								for (int j = 0; j < dataArray.length(); j++) {
									arrayData.add(dataArray.get(j));
								}
								resultMap.put(dependentColumn.get(columnKey), arrayData);
							}
						} catch (Exception e) {
							resultMap.put(dependentColumn.get(columnKey), json.get(key));
						}
					}
				}
			}

		}
	}

	public List<Map<String, Object>> getMultiGetRecords(String traceId,String[] indices, Map<String, Map<String, String>> comparekey, String data, Map<Integer, String> errorRecord, String dataKey) {
		JSONObject json;
		JSONArray jsonArray = new JSONArray();
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		try {
			json = new JSONObject(data);
			json = new JSONObject(json.get(APIConstants.FormulaFields.HITS.getField()).toString());
			jsonArray = new JSONArray(json.get(APIConstants.FormulaFields.HITS.getField()).toString());
			if (!APIConstants.FormulaFields.FIELDS.getField().equalsIgnoreCase(dataKey)) {
				for (int i = 0; i < jsonArray.length(); i++) {
					json = new JSONObject(jsonArray.get(i).toString());
					JSONObject fieldJson = new JSONObject(json.get(dataKey).toString());

					Iterator<String> keys = fieldJson.keys();
					Map<String, Object> resultMap = new HashMap<String, Object>();
					while (keys.hasNext()) {
						String key = keys.next();
						if (comparekey.containsKey(key)) {
							Map<String, String> comparable = new HashMap<String, String>();
							comparable = comparekey.get(key);
							for (Map.Entry<String, String> compare : comparable.entrySet()) {

								if (compare.getKey().equalsIgnoreCase(fieldJson.get(key).toString()))
									resultMap.put(compare.getValue(), fieldJson.get(key));
							}
						} else {
							resultMap.put(apiFields(indices[0], key), fieldJson.get(key));
						}
					}
					resultList.add(resultMap);
				}
			} else {
				for (int i = 0; i < jsonArray.length(); i++) {
					Map<String, Object> resultMap = new HashMap<String, Object>();
					json = new JSONObject(jsonArray.get(i).toString());
					json = new JSONObject(json.get(dataKey).toString());
					Iterator<String> keys = json.keys();
					while (keys.hasNext()) {
						String key = keys.next();
						JSONArray fieldJsonArray = new JSONArray(json.get(key).toString());
							if (comparekey.containsKey(key)) {
								Map<String, String> comparable = new HashMap<String, String>();
								comparable = comparekey.get(key);
								for (Map.Entry<String, String> compare : comparable.entrySet()) {
									
									if (compare.getKey().equalsIgnoreCase(fieldJsonArray.get(0).toString())){
										if (fieldJsonArray.length() == 1) {
										resultMap.put(compare.getValue(), fieldJsonArray.getString(0));
										}else{
											resultMap.put(compare.getValue(), fieldJsonArray);
										}
									}
								}
							} else {
								if (fieldJsonArray.length() == 1) {
								resultMap.put(apiFields(indices[0], key), fieldJsonArray.get(0));
								}else{
								resultMap.put(apiFields(indices[0], key), fieldJsonArray);
								}
							}
					}
					resultList.add(resultMap);
				}
			}
			return resultList;
		} catch (JSONException e) {
			InsightsLogger.error(traceId,ErrorConstants.INVALID_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.JSON) ,e);
		}
		return resultList;
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

	public List<Map<String, Object>> customizeJSON(String traceId,String[] groupBy, String resultData, Map<String, String> metrics, Map<String,Boolean> validatedData, ResponseParamDTO<Map<String, Object>> responseParamDTO,
			int limit) {

		List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
		try {
			int counter = 0;
			Integer totalRows = 0;
			JSONObject json = new JSONObject(resultData);
			json = new JSONObject(json.get(APIConstants.FormulaFields.AGGREGATIONS.getField()).toString());
			if (validatedData.get(Hasdatas.HAS_FILTER.check())) {
				json = new JSONObject(json.get(APIConstants.FormulaFields.FILTERS.getField()).toString());
			}

			while (counter < groupBy.length) {
				if (json.length() > 0) {
					JSONObject requestJSON = new JSONObject(json.get(groupBy[counter]).toString());
					JSONArray jsonArray = new JSONArray(requestJSON.get(APIConstants.FormulaFields.BUCKETS.getField()).toString());
					if (counter == 0) {
						totalRows = jsonArray.length();
						Map<String, Object> dataMap = new HashMap<String, Object>();
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
							fetchMetrics(newJson, dataList, metrics, groupBy, counter, validatedData);
						} else {
							hasSubAggregate = true;
							iterateInternalObject(newJson, subJsonArray, groupBy, counter, key,validatedData);
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
			InsightsLogger.error(traceId,ErrorConstants.INVALID_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.DATA_TYPE), e);
		}
		return dataList;
	}

	private void fetchMetrics(JSONObject newJson, List<Map<String, Object>> dataList, Map<String, String> metrics, String[] groupBy, Integer counter,Map<String,Boolean> validatedData) throws JSONException {
		Map<String, Object> resultMap = new LinkedHashMap<String, Object>();
		for (Map.Entry<String, String> entry : metrics.entrySet()) {
			if (newJson.has(entry.getValue())) {
				resultMap.put(entry.getKey(), new JSONObject(newJson.get(entry.getValue()).toString()).get("value"));
				resultMap.put(groupBy[counter], newJson.get(APIConstants.FormulaFields.KEY.getField()));
				newJson.remove(entry.getValue());
			}
		}
		if(validatedData.get(Hasdatas.HAS_RANGE.check())) {
			newJson.remove(APIConstants.FormulaFields.FROM.getField());
			newJson.remove(APIConstants.FormulaFields.TO.getField());
			newJson.remove(APIConstants.FormulaFields.FROM_AS_STRING.getField());
			newJson.remove(APIConstants.FormulaFields.TO_AS_STRING.getField());
			newJson.remove(APIConstants.FormulaFields.KEY.getField());
			newJson.remove(APIConstants.FormulaFields.DOC_COUNT.getField());
			
		} else {
		newJson.remove(APIConstants.FormulaFields.DOC_COUNT.getField());
		newJson.remove(APIConstants.FormulaFields.KEY_AS_STRING.getField());
		newJson.remove(APIConstants.FormulaFields.KEY.getField());
		newJson.remove(APIConstants.FormulaFields.BUCKETS.getField());
		}
		Iterator<String> dataKeys = newJson.sortedKeys();
		while (dataKeys.hasNext()) {
			String dataKey = dataKeys.next();
			resultMap.put(dataKey, newJson.get(dataKey));
		}
		dataList.add(resultMap);
	}

	private void iterateInternalObject(JSONObject newJson, JSONArray subJsonArray, String[] groupBy, Integer counter, Object key,Map<String,Boolean> validatedData) throws JSONException {
		JSONArray tempArray = new JSONArray();
		JSONObject dataJson = new JSONObject(newJson.get(groupBy[counter + 1]).toString());
		tempArray = new JSONArray(dataJson.get(APIConstants.FormulaFields.BUCKETS.getField()).toString());
		for (int j = 0; j < tempArray.length(); j++) {
			JSONObject subJson = new JSONObject(tempArray.get(j).toString());
			subJson.put(groupBy[counter], key);
			if(validatedData.get(Hasdatas.HAS_RANGE.check())) {
				newJson.remove(APIConstants.FormulaFields.FROM.getField());
				newJson.remove(APIConstants.FormulaFields.TO.getField());
				newJson.remove(APIConstants.FormulaFields.FROM_AS_STRING.getField());
				newJson.remove(APIConstants.FormulaFields.TO_AS_STRING.getField());
				newJson.remove(APIConstants.FormulaFields.KEY.getField());
				newJson.remove(APIConstants.FormulaFields.DOC_COUNT.getField());
			} else {
				newJson.remove(groupBy[counter + 1]);
				newJson.remove(APIConstants.FormulaFields.DOC_COUNT.getField());
				newJson.remove(APIConstants.FormulaFields.KEY_AS_STRING.getField());
				newJson.remove(APIConstants.FormulaFields.KEY.getField());
				newJson.remove(APIConstants.FormulaFields.BUCKETS.getField());
			}
			Iterator<String> dataKeys = newJson.sortedKeys();
			while (dataKeys.hasNext()) {
				String dataKey = dataKeys.next();
				subJson.put(dataKey, newJson.get(dataKey));
			}

			subJsonArray.put(subJson);
		}
	}

	/**
	 * This will convert the apiField name to Els field name
	 * 
	 * @param index
	 *            The index fields are belongs to
	 * @param fields
	 *            Request fields from user
	 * @return converted comma separated Els fields
	 */
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

	public String apiFields(String index, String fields) {
		Map<String, String> apiFields = baseConnectionService.getApiFields().get(index);
		StringBuffer esFields = new StringBuffer();
		for (String field : fields.split(APIConstants.COMMA)) {
			if (esFields.length() > 0) {
				esFields.append(APIConstants.COMMA);
			}
			if (apiFields.containsKey(field)) {
				esFields.append(apiFields.get(field));
			} else {
				esFields.append(field);
			}
		}
		return esFields.toString();
	}
}

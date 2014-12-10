package org.gooru.insights.services;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.UUID;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.index.query.FilterBuilder;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.models.RequestParamsCoreDTO;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsFilterFieldsDTO;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import flexjson.JSONDeserializer;
import flexjson.JSONException;

@Service
public class BaseAPIServiceImpl implements BaseAPIService, APIConstants {

	@Autowired
	private BaseConnectionService baseConnectionService;

	@Autowired
	private RedisService redisService;

	public RequestParamsDTO buildRequestParameters(String data) {

		try {
			return data != null ? deserialize(data, RequestParamsDTO.class) : null;
		} catch (Exception e) {
			throw new JSONException();
		}
	}

	public RequestParamsCoreDTO buildRequestParamsCoreDTO(String data) {

		try {
			return data != null ? deserialize(data, RequestParamsCoreDTO.class) : null;
		} catch (Exception e) {
			throw new JSONException();
		}
	}

	public boolean checkNull(String parameter) {

		if (parameter != null && parameter != "" && (!parameter.isEmpty())) {

			return true;

		} else {

			return false;
		}
	}

	public boolean checkNull(Object request) {
		if (request != null) {

			return true;

		} else {

			return false;
		}
	}

	public boolean checkNull(Map<?, ?> request) {

		if (request != null && (!request.isEmpty())) {

			return true;

		} else {

			return false;
		}
	}

	public boolean checkNull(Collection<?> request) {

		if (request != null && (!request.isEmpty())) {

			return true;

		} else {

			return false;
		}
	}

	public boolean checkNull(Integer parameter) {

		if (parameter != null && parameter.SIZE > 0 && (!parameter.toString().isEmpty())) {

			return true;

		} else {

			return false;
		}
	}

	public String assignValue(String parameter) {
		if (parameter != null && parameter != "" && (!parameter.isEmpty())) {

			return parameter;

		} else {

			return null;
		}

	}

	public Integer assignValue(Integer parameter) {

		if (parameter != null && parameter.SIZE > 0 && (!parameter.toString().isEmpty())) {

			return parameter;

		} else {

			return null;
		}
	}

	public <T> T deserialize(String json, Class<T> clazz) {
		try {
			return new JSONDeserializer<T>().use(null, clazz).deserialize(json);
		} catch (Exception e) {
			throw new JSONException();
		}
	}

	public <T> T deserializeTypeRef(String json, TypeReference<T> type) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		return mapper.readValue(json, type);
	}

	public String[] convertStringtoArray(String data) {
		return data.split(",");
	}

	public Object[] convertSettoArray(Set<?> data) {
		return data.toArray(new Object[data.size()]);
	}

	public JSONArray convertListtoJsonArray(List<Map<String, Object>> result) {
		JSONArray jsonArray = new JSONArray();
		for (Map<String, Object> entry : result) {
			jsonArray.put(entry);
		}
		return jsonArray;
	}

	public JSONArray InnerJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String commonKey) {
		JSONArray jsonArray = new JSONArray();
		if (!child.isEmpty() && !parent.isEmpty()) {
			for (Map<String, Object> childEntry : child) {
				Map<String, Object> appended = new HashMap<String, Object>();
				for (Map<String, Object> parentEntry : parent) {
					if (childEntry.containsKey(commonKey) && parentEntry.containsKey(commonKey)) {
						if (childEntry.get(commonKey).equals(parentEntry.get(commonKey))) {
							childEntry.remove(commonKey);
							appended.putAll(childEntry);
							appended.putAll(parentEntry);
							break;
						}
					}
				}
				if (checkNull(appended)) {
					jsonArray.put(appended);
				}
			}
			return jsonArray;
		}
		return jsonArray;
	}

	public JSONArray InnerJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child) {
		JSONArray jsonArray = new JSONArray();
		if (!child.isEmpty() && !parent.isEmpty()) {
			for (Map<String, Object> childEntry : child) {
				Map<String, Object> appended = new HashMap<String, Object>();
				Set<String> keys = childEntry.keySet();
				for (Map<String, Object> parentEntry : parent) {
					boolean valid = true;
					for (String key : keys) {
						if (parentEntry.containsKey(key) && childEntry.containsKey(key) && (!parentEntry.get(key).equals(childEntry.get(key)))) {
							valid = false;
						}
					}
					if (valid) {
						appended.putAll(parentEntry);
						appended.putAll(childEntry);
						break;
					}
				}
				if (checkNull(appended)) {
					jsonArray.put(appended);
				}
			}
		}
		return jsonArray;
	}

	public List<Map<String, Object>> innerJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child) {
		List<Map<String, Object>> resultData = new ArrayList<Map<String, Object>>();
		if (!child.isEmpty() && !parent.isEmpty()) {
			for (Map<String, Object> childEntry : child) {
				Map<String, Object> appended = new HashMap<String, Object>();
				Set<String> keys = childEntry.keySet();
				for (Map<String, Object> parentEntry : parent) {
					boolean valid = true;
					for (String key : keys) {
						if (parentEntry.containsKey(key) && childEntry.containsKey(key) && (!parentEntry.get(key).equals(childEntry.get(key)))) {
							valid = false;
						}
					}
					if (valid) {
						appended.putAll(parentEntry);
						appended.putAll(childEntry);
						break;
					}
				}
				if (checkNull(appended)) {
					resultData.add(appended);
				}
			}
		}
		return resultData;
	}

	@Override
	public String convertArraytoString(String[] datas) {
		StringBuffer result = new StringBuffer();
		for (String data : datas) {
			if (result.length() > 0) {
				result.append(",");
			}
			result.append(data);
		}
		return result.toString();
	}

	public static void main(String args[]) {
		BaseAPIServiceImpl baseAPIService = new BaseAPIServiceImpl();
		double count = 1;
		String data[] = new String[] { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z" };
		List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
		for (int i = 0; i < 26; i++) {
			Map<String, Object> dataMap = new HashMap<String, Object>();
			if (!(i == 2 || i == 4)) {
				dataMap.put("timespent", 1 * count);
			}
			if (!(i == 2 || i == 4)) {
				dataMap.put("views", i);
			}
			dataMap.put("title", data[i]);
			count = count * 100;
			dataList.add(dataMap);
		}

		baseAPIService.sortBy(dataList, "views", "asc");
	}

	public List<Map<String, Object>> sortBy(List<Map<String, Object>> requestData, String sortBy, String sortOrder) {

		if (checkNull(sortBy)) {
			for (final String name : sortBy.split(",")) {
				boolean ascending = false;
				boolean descending = false;
				if (checkNull(sortOrder)) {
					if (sortOrder.equalsIgnoreCase("ASC")) {
						ascending = true;
					} else if (sortOrder.equalsIgnoreCase("DESC")) {
						descending = true;
					} else {
						ascending = true;
					}
				} else {
					ascending = true;
				}
				if (ascending) {
					Collections.sort(requestData, new Comparator<Map<String, Object>>() {
						public int compare(final Map<String, Object> m1, final Map<String, Object> m2) {
							if (m1.containsKey(name) && m2.containsKey(name)) {

								try {
									return ((Integer) m1.get(name)).compareTo((Integer) m2.get(name));
								} catch (Exception e) {
									try {
										return ((Long.valueOf(m1.get(name).toString())).compareTo((Long.valueOf(m2.get(name).toString()))));
									} catch (Exception e1) {
										try {
											return ((Double.valueOf(m1.get(name).toString())).compareTo((Double.valueOf(m2.get(name).toString()))));
										} catch (Exception e3) {
											return ((String) m1.get(name).toString().toLowerCase()).compareTo((String) m2.get(name).toString().toLowerCase());
										}
									}
								}
							}
							return -1;
						}
					});
				}
				if (descending) {

					Collections.sort(requestData, new Comparator<Map<String, Object>>() {
						public int compare(final Map<String, Object> m1, final Map<String, Object> m2) {

							if (m2.containsKey(name)) {
								if (m1.containsKey(name)) {

									try {
										return ((Integer) m2.get(name)).compareTo((Integer) m1.get(name));
									} catch (Exception e3) {
										try {
											return ((Long.valueOf(m2.get(name).toString())).compareTo((Long.valueOf(m1.get(name).toString()))));
										} catch (Exception e1) {
											try {
												return ((Double.valueOf(m2.get(name).toString())).compareTo((Double.valueOf(m1.get(name).toString()))));
											} catch (Exception e) {
												return ((String) m2.get(name).toString().toLowerCase()).compareTo((String) m1.get(name).toString().toLowerCase());
											}
										}
									}
								} else {
									return 1;
								}
							} else {
								return -1;
							}
						}
					});

				}
			}
		}
		return requestData;
	}

	public JSONArray formatKeyValueJson(List<Map<String, Object>> dataMap, String key) throws org.json.JSONException {

		JSONArray jsonArray = new JSONArray();
		JSONObject json = new JSONObject();
		Map<String, String> resultMap = new HashMap<String, String>();
		Gson gson = new Gson();
		for (Map<String, Object> map : dataMap) {
			if (map.containsKey(key)) {
				String jsonKey = map.get(key).toString();
				map.remove(key);
				json.accumulate(jsonKey, map);
			}
		}
		resultMap = gson.fromJson(json.toString(), resultMap.getClass());
		Map<String, Object> Treedata = new TreeMap<String, Object>(resultMap);
		for (Map.Entry<String, Object> entry : Treedata.entrySet()) {
			JSONObject resultJson = new JSONObject();
			resultJson.put(entry.getKey(), entry.getValue());
			jsonArray.put(resultJson);
		}
		return jsonArray;
	}

	public Map<String, Boolean> validateData(RequestParamsDTO requestParamsDTO) {
		Map<String, Boolean> processedData = new HashMap<String, Boolean>();
		processedData.put("hasFields", false);
		processedData.put("hasDataSource", false);
		processedData.put("hasGroupBy", false);
		processedData.put("hasIntervals", false);
		processedData.put("hasFilter", false);
		processedData.put("hasAggregate", false);
		processedData.put("hasLimit", false);
		processedData.put("hasOffset", false);
		processedData.put("hasSortBy", false);
		processedData.put("hasSortOrder", false);
		processedData.put("hasGranularity", false);
		processedData.put("hasPagination", false);
		if (checkNull(requestParamsDTO.getFields())) {
			processedData.put("hasFields", true);
		}
		if (checkNull(requestParamsDTO.getDataSource())) {
			// System.out.println("has dataSource"+requestParamsDTO.getDataSource());
			processedData.put("hasDataSource", true);
		}
		if (checkNull(requestParamsDTO.getGroupBy())) {
			processedData.put("hasGroupBy", true);
		}
		if (checkNull(requestParamsDTO.getIntervals())) {
			processedData.put("hasIntervals", true);
		}
		if (checkNull(requestParamsDTO.getGranularity())) {
			processedData.put("hasGranularity", true);
		}
		if (checkNull(requestParamsDTO.getFilter()) && checkNull(requestParamsDTO.getFilter().get(0)) && checkNull(requestParamsDTO.getFilter().get(0).getLogicalOperatorPrefix())
				&& checkNull(requestParamsDTO.getFilter().get(0).getFields()) && checkNull(requestParamsDTO.getFilter().get(0).getFields().get(0))) {
			processedData.put("hasFilter", true);
		}
		if (checkNull(requestParamsDTO.getAggregations()) && processedData.get("hasGroupBy")) {
			processedData.put("hasAggregate", true);
		}
		if (checkNull(requestParamsDTO.getPagination())) {
			processedData.put("hasPagination", true);
			if (checkNull(requestParamsDTO.getPagination().getLimit())) {
				processedData.put("hasLimit", true);
			}
			if (checkNull(requestParamsDTO.getPagination().getOffset())) {
				processedData.put("hasOffset", true);
			}
			if (checkNull(requestParamsDTO.getPagination().getOrder())) {
				if (checkNull(requestParamsDTO.getPagination().getOrder().get(0)))
					if (checkNull(requestParamsDTO.getPagination().getOrder().get(0).getSortBy())) {
						processedData.put("hasSortBy", true);
					}
				if (checkNull(requestParamsDTO.getPagination().getOrder().get(0).getSortOrder())) {
					processedData.put("hasSortOrder", true);
				}
			}
		}
		return processedData;
	}

	public String[] getIndices(String names) {
		String[] indices = new String[names.split(",").length];
		String[] requestNames = names.split(",");
		for (int i = 0; i < indices.length; i++) {
			if (baseConnectionService.getIndexMap().containsKey(requestNames[i]))
				indices[i] = baseConnectionService.getIndexMap().get(requestNames[i]);
		}
		return indices;
	}

	public String convertTimeMstoISO(Object milliseconds) {

		DateFormat ISO_8601_DATE_TIME = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZZ");
		ISO_8601_DATE_TIME.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date date = new Date(Long.valueOf(milliseconds.toString()));
		return ISO_8601_DATE_TIME.format(date);
	}

	@Override
	public RequestParamsDTO validateUserRole(RequestParamsDTO requestParamsDTO, Map<String, Object> userMap, Map<Integer, String> errorMap) {
		String userRole = userMap.containsKey("userRoleSetString") ? userMap.get("userRoleSetString").toString() : null;
		String gooruUId = userMap.containsKey("gooruUId") ? userMap.get("gooruUId").toString() : null;

		if (userRole != null && (userRole.contains("ROLE_GOORU_ADMIN") || userRole.contains("superadmin") || userRole.contains("Organization_Admin") || userRole.contains("Content_Admin"))) {
			System.out.print("Do Nothing..");
		} else {
			for (RequestParamsFilterDetailDTO fieldData : requestParamsDTO.getFilter()) {
				for (RequestParamsFilterFieldsDTO fieldsDetails : fieldData.getFields()) {
					if (fieldsDetails.getFieldName() != null && fieldsDetails.getFieldName().equalsIgnoreCase("gooruUId") || fieldsDetails.getFieldName().equalsIgnoreCase("creatorUid")) {
						if (fieldsDetails.getValue().equalsIgnoreCase(gooruUId)) {
							errorMap.put(403, "Sorry! You don't have access to see PII info.");
						}
					}
				}
			}

			if ((requestParamsDTO.getGroupBy() != null && (requestParamsDTO.getGroupBy().contains("creatorUid") || requestParamsDTO.getGroupBy().contains("gooruUId")))
					|| (requestParamsDTO.getFields() != null && (requestParamsDTO.getFields().contains("userName") || requestParamsDTO.getFields().contains("gooruUId") || requestParamsDTO.getFields()
							.contains("creatorUid")))) {
				errorMap.put(403, "Sorry! You don't have access to see PII info.");
			}
		}

		return requestParamsDTO;
	}

	public boolean clearQuery(String id) {

		try {
			for (String requestId : id.split(",")) {
				if (redisService.hasRedisKey(requestId)) {
					String queryId = redisService.getRedisValue(requestId);
					redisService.removeRedisKey(requestId);
					if (redisService.hasRedisKey(queryId)) {
						redisService.removeRedisKey(CACHE_PREFIX_ID + SEPARATOR + queryId);
						redisService.removeRedisKey(queryId);
						System.out.println(" requestId " + requestId + " queryid " + queryId);
					}
				}
			}
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	public boolean clearQuerys(String[] id) {

		return redisService.removeRedisKeys(id);
	}

	public String putRedisCache(String query, JSONArray jsonArray) {

		UUID queryId = UUID.randomUUID();

		if (redisService.hasRedisKey(CACHE_PREFIX_ID + SEPARATOR + query.trim())) {
			return redisService.getRedisValue(CACHE_PREFIX_ID + SEPARATOR + query.trim());
		}

		redisService.putRedisStringValue(queryId.toString(), query.trim());
		redisService.putRedisStringValue(query.trim(), jsonArray.toString());
		redisService.putRedisStringValue(CACHE_PREFIX_ID + SEPARATOR + query.trim(), queryId.toString());

		System.out.println("new Id created " + queryId);
		return queryId.toString();

	}
}

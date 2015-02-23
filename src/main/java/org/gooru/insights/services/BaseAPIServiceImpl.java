package org.gooru.insights.services;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.gooru.insights.builders.utils.ExcludeNullTransformer;
import org.gooru.insights.builders.utils.MessageHandler;
import org.gooru.insights.builders.utils.RedisService;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.ErrorConstants;
import org.gooru.insights.constants.ResponseParamDTO;
import org.gooru.insights.exception.handlers.AccessDeniedException;
import org.gooru.insights.exception.handlers.BadRequestException;
import org.gooru.insights.exception.handlers.ReportGenerationException;
import org.gooru.insights.models.RequestParamsCoreDTO;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsFilterFieldsDTO;
import org.gooru.insights.models.RequestParamsSortDTO;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;

import flexjson.JSONDeserializer;
import flexjson.JSONException;
import flexjson.JSONSerializer;

@Service
public class BaseAPIServiceImpl {
	
	Logger logger = LoggerFactory.getLogger(BaseAPIServiceImpl.class); 

	@Autowired
	private BaseConnectionService baseConnectionService;

	@Autowired
	private RedisService redisService;

	@Autowired
	private ValidateUserPermissionService validateUserPermissionService;
	
	public RequestParamsDTO buildRequestParameters(String data) {

		try {
			return data != null ? deserialize(data, RequestParamsDTO.class) : null;
		} catch (Exception e) {
			throw new BadRequestException(ErrorConstants.E102);
		}
	}
	/**
	 * 
	 * @param data
	 * @return
	 */
	public RequestParamsCoreDTO buildRequestParamsCoreDTO(String data) {

		try {
			return data != null ? deserialize(data, RequestParamsCoreDTO.class) : null;
		} catch (Exception e) {
			throw new JSONException();
		}
	}

	public boolean checkNull(String parameter) {

		if(StringUtils.isBlank(parameter)){
			return false;
		}else {

			return true;
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
	
	public Set<String> convertStringtoSet(String inputDatas) {
		Set<String> outDatas = new HashSet<String>();
		for(String inputData : inputDatas.split(",")){
			outDatas.add(inputData);
		}
		return outDatas;
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
	
	public String convertCollectiontoString(Collection<String> datas) {
		StringBuffer result = new StringBuffer();
		for (String data : datas) {
			if (result.length() > 0) {
				result.append(",");
			}
			result.append(data);
		}
		return result.toString();
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

	public static void main(String[] args){
		String data="abc";
		//abc,bca,cba,acb,cab,bac,
		StringBuffer result = new StringBuffer();
		int counter =0;
		int replacer=0;
		while((data.length() * data.length()) != counter){
		for(int i=replacer;i< data.length();i++){
			result.append(data.charAt(i));
			if(replacer >= data.length()){
				i =0;
			}
		}
		if(result.length() > 0){
			result.append(",");
		}
		replacer++;
		counter++;
		}
		System.out.println(result.toString());
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
	
	public boolean validateRequest(RequestParamsDTO requestParamsDTO,Map<String, Boolean> processedData){
		
		return false;
	}
	 
	public Map<String, Boolean> checkPoint(RequestParamsDTO requestParamsDTO){
		Map<String, Boolean> processedData = new HashMap<String, Boolean>();
		processedData.put("hasFields", false);
		processedData.put("hasDataSource", false);
		processedData.put("hasGroupBy", false);
		processedData.put("hasFilter", false);
		processedData.put("hasAggregate", false);
		processedData.put("hasLimit", false);
		processedData.put("hasOffset", false);
		processedData.put("hasSortBy", false);
		processedData.put("hasSortOrder", false);
		processedData.put("hasGranularity", false);
		processedData.put("hasPagination", false);
		Set<String> fieldData = new HashSet<String>();
		
		/**
		 * DataSource should not be null and it should have valid dataSource.
		 */
		if (checkNull(requestParamsDTO.getDataSource())) {
			for(String dataSource : requestParamsDTO.getDataSource().split(",")){
			if(!baseConnectionService.getIndexMap().containsKey(dataSource)){
				throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103,new String[]{APIConstants.DATA_SOURCE,dataSource}));
			}else{
				if(baseConnectionService.getFields().containsKey(baseConnectionService.getIndexMap().get(dataSource))){	
					fieldData.addAll(baseConnectionService.getFields().get(baseConnectionService.getIndexMap().get(dataSource)).keySet());
				}
				}
			}
			processedData.put("hasDataSource", true);
		}else{
			throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100,APIConstants.DATA_SOURCE));
		}
		
		/**
		 * Aggregation mandatory fields need to be present and it's field name should be in database acceptable field
		 */
		if(checkNull(requestParamsDTO.getAggregations())){
			for(Map<String,String> aggregate : requestParamsDTO.getAggregations()){
				if(!aggregate.containsKey("requestValues") && !aggregate.containsKey("name") && !aggregate.containsKey("formula") && !aggregate.containsKey(aggregate.get("requestValues"))){
					throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100,APIConstants.AGGREGATE_ATTRIBUTE));
				}else{
					if(!fieldData.contains(aggregate.get(aggregate.get("requestValues")))){
						throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103,new String[]{APIConstants.AGGREGATE_ATTRIBUTE,aggregate.get(aggregate.get("requestValues"))}));
					}
					fieldData.add(aggregate.get("name"));
				}
			}
			processedData.put("hasAggregate", true);
		}
		
		/**
		 * fields should not be EMPTY and should be a valid field specified in data source
		 */
		if (checkNull(requestParamsDTO.getFields())) {
			StringBuffer errorField = new StringBuffer();
			for(String field : requestParamsDTO.getFields().split(APIConstants.COMMA)){
				if(!fieldData.contains(field)){
					errorField.append(field);
					if(errorField.length() > 0){
						errorField.append(APIConstants.COMMA);
					}
				}
			}
			if(errorField.length() > 0){
				throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103, new String[]{APIConstants.FIELDS,errorField.toString()}));
			}
			processedData.put("hasFields", true);
		}else{
			throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100,APIConstants.FIELDS));
		}
		
		/**
		 * If the aggregation is given then groupBy should not be EMPTY and vice versa.
		 */
		if (checkNull(requestParamsDTO.getGroupBy())) {
			if(!processedData.get("hasAggregate")){
				throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100,APIConstants.AGGREGATE_ATTRIBUTE));
			}
			StringBuffer errorField = new StringBuffer();
			for(String field : requestParamsDTO.getGroupBy().split(APIConstants.COMMA)){
				if(!fieldData.contains(field)){
					errorField.append(field);
				}
				if(errorField.length() > 0){
					errorField.append(APIConstants.COMMA);
				}
			}
			if(errorField.length() > 0){
				throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103, new String[]{APIConstants.GROUP_BY,errorField.toString()}));
			}
			processedData.put("hasGroupBy", true);
		}else if(processedData.get("hasAggregate")){
			throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100, APIConstants.GROUP_BY));
		}
		
		/**
		 * granularity should be a valid acceptable field 
		 */
		if (checkNull(requestParamsDTO.getGranularity())) {
			boolean isValid = false;
			for(String granularity : APIConstants.GRANULARITY){
				if(requestParamsDTO.getGranularity().endsWith(granularity)){
					isValid = true;
					break;
				}
			}
			if(!isValid){
			throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103, new String[]{APIConstants.GRANULARITY_NAME,requestParamsDTO.getGranularity()}));
			}
			processedData.put("hasGranularity", true);
		}
		
		/**
		 * Filter mandatory fields should not be EMPTY and it should be acceptable field
		 */
		if (checkNull(requestParamsDTO.getFilter()) && checkNull(requestParamsDTO.getFilter().get(0))) {
			for(RequestParamsFilterDetailDTO logicalOperations : requestParamsDTO.getFilter()){
				
				if(!checkNull(logicalOperations.getLogicalOperatorPrefix())){
					throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100,APIConstants.LOGICAL_OPERATOR));
				}
				if(!baseConnectionService.getLogicalOperations().contains(logicalOperations.getLogicalOperatorPrefix())){
					throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103,new String[]{APIConstants.LOGICAL_OPERATOR,logicalOperations.getLogicalOperatorPrefix()}));
				}
				
				for(RequestParamsFilterFieldsDTO filters : logicalOperations.getFields()){

					if(!checkNull(filters.getFieldName()) || !checkNull(filters.getOperator()) ||!checkNull(filters.getValueType()) || !checkNull(filters.getValue()) || !checkNull(filters.getType())){
						throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100,APIConstants.FILTERS));
					}
					if(!filters.getType().equalsIgnoreCase(APIConstants.SELECTOR)){
						throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103,new String[]{APIConstants.FILTERS,filters.getType()}));
					}
					if(!fieldData.contains(filters.getFieldName())){
						throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103,new String[]{APIConstants.FILTERS,filters.getFieldName()}));
					}
					if(!baseConnectionService.getEsOperations().contains(filters.getOperator())){
						throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103,new String[]{APIConstants.FILTERS,filters.getOperator()}));
					}
				}
			}
			processedData.put("hasFilter", true);
		}

		/**
		 * Check for pagination and the sortBy field should not be EMPTY and it should be valid field.
		 */
		if (checkNull(requestParamsDTO.getPagination())) {
			processedData.put("hasPagination", true);
			if (checkNull(requestParamsDTO.getPagination().getLimit())) {
				processedData.put("hasLimit", true);
			}
			if (checkNull(requestParamsDTO.getPagination().getOffset())) {
				processedData.put("hasOffset", true);
			}
			if (checkNull(requestParamsDTO.getPagination().getOrder())) {
				for(RequestParamsSortDTO orderData : requestParamsDTO.getPagination().getOrder()){
					
					if(!checkNull(orderData.getSortBy())){
						throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100,APIConstants.SORT_BY));
					}
					
					if(!fieldData.contains(orderData.getSortBy())){
						throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103,new String[]{APIConstants.SORT_BY,orderData.getSortBy()}));
					}
					
					if (checkNull(orderData.getSortOrder())) {
						processedData.put("hasSortOrder", true);
					}
					processedData.put("hasSortBy", true);
				}
			}
		}
		return processedData;
	}

	public String[] getIndices(String names) {
		String[] indices = new String[names.split(",").length];
		int index = 0;
		for (String name : names.split(",")) {
			if (baseConnectionService.getIndexMap().containsKey(name)){
				indices[index] = baseConnectionService.getIndexMap().get(name);
				index++;
			}
		}
		return indices;
	}

	public String convertTimeMstoISO(Object milliseconds) {

		DateFormat ISO_8601_DATE_TIME = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZZ");
		ISO_8601_DATE_TIME.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date date = new Date(Long.valueOf(milliseconds.toString()));
		return ISO_8601_DATE_TIME.format(date);
	}

	public RequestParamsDTO validateUserRole(RequestParamsDTO requestParamsDTO, Map<String, Object> userMap, Map<Integer, String> errorMap) {

		String gooruUId = userMap.containsKey(APIConstants.GOORUUID) ? userMap.get(APIConstants.GOORUUID).toString() : null;

		Map<String, Set<String>> partyPermissions = (Map<String, Set<String>>) userMap.get(APIConstants.PERMISSIONS);
		logger.info(APIConstants.GOORUUID+APIConstants.SEPARATOR+gooruUId);
		logger.info(APIConstants.PERMISSIONS+APIConstants.SEPARATOR+partyPermissions);
		
		if(!StringUtils.isBlank(validateUserPermissionService.getRoleBasedParty(partyPermissions,APIConstants.AP_ALL_PARTY_ALL_DATA))){
			return requestParamsDTO;
		}

		Map<String, Object> userFilters = validateUserPermissionService.getUserFilters(gooruUId);
		Map<String, Object> userFiltersAndValues = validateUserPermissionService.getUserFiltersAndValues(requestParamsDTO.getFilter());
		Set<String> userFilterOrgValues = (Set<String>) userFiltersAndValues.get("orgFilters");
		Set<String> userFilterUserValues = (Set<String>) userFiltersAndValues.get("userFilters");

		String partyAlldataPerm = validateUserPermissionService.getRoleBasedParty(partyPermissions,APIConstants.AP_PARTY_ALL_DATA);
		
		if(!StringUtils.isBlank(partyAlldataPerm) && userFilterOrgValues.isEmpty()){			
			validateUserPermissionService.addSystemContentUserOrgFilter(requestParamsDTO.getFilter(), partyAlldataPerm);
		}
		if(!StringUtils.isBlank(partyAlldataPerm) && !userFilterOrgValues.isEmpty()){			
			for(String org : userFilterOrgValues){
				if(!partyAlldataPerm.contains(org)){
					throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E108));
				}
			}		
			return requestParamsDTO;
		}
		
		Map<String, Object> orgFilters = new HashMap<String, Object>();
		
		for(Entry<String, Set<String>> e : partyPermissions.entrySet()){
			if(e.getValue().contains(APIConstants.AP_ALL_PARTY_ALL_DATA)){
				return requestParamsDTO;
			}else if(e.getValue().contains(APIConstants.AP_PARTY_ALL_DATA)){
				orgFilters.put(e.getKey(), e.getValue());
			}
		}
		if(userFilterOrgValues.isEmpty() && !orgFilters.isEmpty()){
			return requestParamsDTO;
		}
		
		if (!validateUserPermissionService.checkIfFieldValueMatch(userFilters, userFiltersAndValues, errorMap).isEmpty()) {
			if(errorMap.containsKey(403)){
				return validateUserPermissionService.userPreValidation(requestParamsDTO, userFilterUserValues, partyPermissions);
			}else{
				errorMap.clear();
				return requestParamsDTO;
			}
		}

		if (partyPermissions.isEmpty() && (requestParamsDTO.getDataSource().matches(APIConstants.USERDATASOURCES)|| (requestParamsDTO.getDataSource().matches(APIConstants.ACTIVITYDATASOURCES) 
				&& !StringUtils.isBlank(requestParamsDTO.getGroupBy()) && requestParamsDTO.getGroupBy().matches(APIConstants.USERFILTERPARAM)))) {
				errorMap.put(403,MessageHandler.getMessage(ErrorConstants.E104, ErrorConstants.E_PII));
				return requestParamsDTO;
		}
		if (partyPermissions.isEmpty() && (requestParamsDTO.getDataSource().matches(APIConstants.ACTIVITYDATASOURCES) && StringUtils.isBlank(requestParamsDTO.getGroupBy()))) {
			errorMap.put(403,MessageHandler.getMessage(ErrorConstants.E104, ErrorConstants.E_RAW));
			return requestParamsDTO;
		}

		if (!userFilterOrgValues.isEmpty()) {
			validateUserPermissionService.validateOrganization(requestParamsDTO, partyPermissions, errorMap, userFilterOrgValues);
		} else {
			String allowedParty = validateUserPermissionService.getAllowedParties(requestParamsDTO, partyPermissions);
			if (!StringUtils.isBlank(allowedParty)) {
				if(requestParamsDTO.getDataSource().matches(APIConstants.USERDATASOURCES)){
					validateUserPermissionService.addSystemUserOrgFilter(requestParamsDTO.getFilter(), allowedParty);
				}else{
					validateUserPermissionService.addSystemContentUserOrgFilter(requestParamsDTO.getFilter(), allowedParty);
				}
			} else {
				errorMap.put(403, ErrorConstants.E108);
				return requestParamsDTO;
			}
		}

		JSONSerializer serializer = new JSONSerializer();
		serializer.transform(new ExcludeNullTransformer(), void.class).exclude("*.class");
		logger.info(APIConstants.NEW_QUERY+serializer.deepSerialize(requestParamsDTO));
		return requestParamsDTO;
	}


	public boolean clearQuery(String id) {
		boolean status = false;
		if (id != null && !id.isEmpty()) {
			for (String requestId : id.split(",")) {
				if (redisService.hasRedisKey(requestId)) {
					String queryId = redisService.getRedisValue(requestId);
					redisService.removeRedisKey(requestId);
					if (redisService.hasRedisKey(queryId)) {
						redisService.removeRedisKey(APIConstants.CACHE_PREFIX_ID + APIConstants.SEPARATOR + queryId);
						status = redisService.removeRedisKey(queryId);
						System.out.println(" requestId " + requestId + " queryid " + queryId);
					}
				}
			}
		} else {
			status = redisService.removeRedisKeys();
		}
		return status;
	}

	public String getQuery(String prefix, String id) {
		if (redisService.hasRedisKey(prefix + id)) {
			if (redisService.hasRedisKey(prefix + redisService.getRedisValue(prefix + id))) {
				return redisService.getRedisValue(prefix + redisService.getRedisValue(prefix + id));
			}
		}
		return null;
	}

	public boolean insertKey(String data) {
		try {
			JSONObject jsonObject = new JSONObject(data);
			Iterator<String> itr = jsonObject.keys();
			while (itr.hasNext()) {
				String key = itr.next();
				redisService.putRedisRawValue(key, jsonObject.getString(key));
			}
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	public boolean clearQuerys(String[] id) {

		return redisService.removeRedisKeys(id);
	}

	public boolean hasKey(String id) {

		return redisService.hasRedisKey(id);
	}

	public String getKey(String id) {
		return redisService.getRedisValue(id);
	}

	public String getRedisRawValue(String key) {
		return redisService.getRedisRawValue(key);
	}

	public Set<String> getKeys() {
		return redisService.getKeys();
	}

	public <M> String putRedisCache(String query, Map<String, Object> userMap, ResponseParamDTO<M> responseParamDTO) {

		UUID queryId = UUID.randomUUID();

		String KEY_PREFIX = APIConstants.EMPTY;
		if (userMap.containsKey(APIConstants.GOORUUID) && userMap.get(APIConstants.GOORUUID) != null) {
			KEY_PREFIX += userMap.get(APIConstants.GOORUUID) + APIConstants.SEPARATOR;
		}
		if (redisService.hasRedisKey(APIConstants.CACHE_PREFIX_ID + APIConstants.SEPARATOR + KEY_PREFIX + query.trim())) {
			return redisService.getRedisValue(APIConstants.CACHE_PREFIX_ID + APIConstants.SEPARATOR + KEY_PREFIX + query.trim());
		}
		redisService.putRedisStringValue(KEY_PREFIX + queryId.toString(), query.trim());
		redisService.putRedisStringValue(KEY_PREFIX + query.trim(), new JSONSerializer().exclude(APIConstants.EXCLUDE_CLASSES).serialize(responseParamDTO));
		redisService.putRedisStringValue(APIConstants.CACHE_PREFIX_ID + APIConstants.SEPARATOR + KEY_PREFIX + query.trim(), queryId.toString());

		System.out.println("new Id created " + queryId);
		return queryId.toString();

	}

	public Map<String, Object> getRequestFieldNameValueInMap(HttpServletRequest request, String prefix) {
	     Map<String, Object> requestFieldNameValue = new HashMap<String, Object>();
         Enumeration paramNames = request.getParameterNames();
         while (paramNames.hasMoreElements()) {
                 String paramName = (String) paramNames.nextElement();
                 if (paramName.startsWith(prefix+".")) {
                	 requestFieldNameValue.put(paramName.replace(prefix+".", ""), request.getParameter(paramName));
                 }
         }
         return requestFieldNameValue;
	}

	public void saveQuery(RequestParamsDTO requestParamsDTO, ResponseParamDTO<Map<String,Object>> responseParamDTO, String data, Map<String, Object> dataMap, Map<String, Object> userMap){
		try {
		if (requestParamsDTO.isSaveQuery() != null) {
			if (requestParamsDTO.isSaveQuery()) {
				String queryId = putRedisCache(data,userMap, responseParamDTO);
				dataMap.put("queryId", queryId);
			}
		}
		} catch (Exception e) {
			throw new ReportGenerationException(ErrorConstants.REDIS_MESSAGE.replace(ErrorConstants.REPLACER, ErrorConstants.INSERT));
		}
	}
}

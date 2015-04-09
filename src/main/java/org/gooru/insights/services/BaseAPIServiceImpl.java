package org.gooru.insights.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.gooru.insights.builders.utils.ExcludeNullTransformer;
import org.gooru.insights.builders.utils.MessageHandler;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.APIConstants.Hasdatas;
import org.gooru.insights.constants.ErrorConstants;
import org.gooru.insights.exception.handlers.AccessDeniedException;
import org.gooru.insights.exception.handlers.BadRequestException;
import org.gooru.insights.models.RequestParamsCoreDTO;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsFilterFieldsDTO;
import org.gooru.insights.models.RequestParamsRangeDTO;
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
public class BaseAPIServiceImpl implements BaseAPIService {
	
	private static final Logger logger = LoggerFactory.getLogger(BaseAPIServiceImpl.class); 

	@Autowired
	private BaseConnectionService baseConnectionService;

	@Autowired
	private ValidateUserPermissionService validateUserPermissionService;
	
	@Autowired
	private BusinessLogicService businessLogicService;
	
	public RequestParamsDTO buildRequestParameters(String data) {
		try {
			return data != null ? deserialize(data, RequestParamsDTO.class) : null;
		} catch (Exception e) {
			throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E102));
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
		return data.split(APIConstants.COMMA);
	}
	
	public Set<String> convertStringtoSet(String inputDatas) {
		Set<String> outDatas = new HashSet<String>();
		for(String inputData : inputDatas.split(APIConstants.COMMA)){
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
				result.append(APIConstants.COMMA);
			}
			result.append(data);
		}
		return result.toString();
	}
	
	public String convertCollectiontoString(Collection<String> datas) {
		StringBuffer result = new StringBuffer();
		for (String data : datas) {
			if (result.length() > 0) {
				result.append(APIConstants.COMMA);
			}
			result.append(data);
		}
		return result.toString();
	}

	public List<Map<String, Object>> sortBy(List<Map<String, Object>> requestData, String sortBy, String sortOrder) {

		if (checkNull(sortBy)) {
			for (final String name : sortBy.split(",")) {
				boolean descending = false;
				if (checkNull(sortOrder) && sortOrder.equalsIgnoreCase("DESC")) {
					descending = true;
				}
				if (!descending) {
					Collections.sort(requestData, new Comparator<Map<String, Object>>() {
						public int compare(final Map<String, Object> m1, final Map<String, Object> m2) {
							if (m1.containsKey(name) && m2.containsKey(name)) {
								return compareTo(m1, m2, name);
							}
							return 1;
						}
					});
				}else{
					Collections.sort(requestData, new Comparator<Map<String, Object>>() {
						public int compare(final Map<String, Object> m1, final Map<String, Object> m2) {
							if (m2.containsKey(name)) {
								if (m1.containsKey(name)) {
									return compareTo(m2, m1, name);
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
	
	private int compareTo(Map<String,Object> m1,Map<String,Object> m2,String name){
		if (m1.get(name) instanceof String) {
			return ((String) m1.get(name).toString().toLowerCase()).compareTo((String) m2.get(name).toString().toLowerCase());
		} else if (m1.get(name) instanceof Double) {
			return (Double.valueOf(m1.get(name).toString())).compareTo(Double.valueOf(m2.get(name).toString()));
		} else if (m1.get(name) instanceof Long) {
			return (Long.valueOf(m1.get(name).toString())).compareTo(Long.valueOf(m2.get(name).toString()));
		} else if (m1.get(name) instanceof Integer) {
			return (Integer.valueOf(m1.get(name).toString())).compareTo(Integer.valueOf(m2.get(name).toString()));
		} 
		return 0;
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
	
	public Map<String, Boolean> checkPoint(RequestParamsDTO requestParamsDTO){
		Map<String, Boolean> processedData = new HashMap<String, Boolean>();
		processedData.put(Hasdatas.HAS_FEILDS.check(), false);
		processedData.put(Hasdatas.HAS_DATASOURCE.check(), false);
		processedData.put(Hasdatas.HAS_GROUPBY.check(), false);
		processedData.put(Hasdatas.HAS_FILTER.check(), false);
		processedData.put(Hasdatas.HAS_AGGREGATE.check(), false);
		processedData.put(Hasdatas.HAS_RANGE.check(), false);
		processedData.put(Hasdatas.HAS_LIMIT.check(), false);
		processedData.put(Hasdatas.HAS_Offset.check(), false);
		processedData.put(Hasdatas.HAS_SORTBY.check(), false);
		processedData.put(Hasdatas.HAS_SORTORDER.check(), false);
		processedData.put(Hasdatas.HAS_GRANULARITY.check(), false);
		processedData.put(Hasdatas.HAS_PAGINATION.check(), false);
		Set<String> fieldData = new HashSet<String>();
		
		/**
		 * DataSource should not be null and it should have valid dataSource.
		 */
		if (checkNull(requestParamsDTO.getDataSource())) {
			for(String dataSource : requestParamsDTO.getDataSource().split(APIConstants.COMMA)){
			boolean validDataSource = false;
				for(String indexName : baseConnectionService.getIndexMap().keySet()){
			if(indexName.toUpperCase().equalsIgnoreCase(dataSource)){
				validDataSource = true;
				if(baseConnectionService.getFields().containsKey(baseConnectionService.getIndexMap().get(indexName))){	
					fieldData.addAll(baseConnectionService.getFields().get(baseConnectionService.getIndexMap().get(indexName)).keySet());
				}
			}
			}
				if(!validDataSource){
					throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103,new String[]{APIConstants.DATA_SOURCE,dataSource}));
				}
			}
			processedData.put(Hasdatas.HAS_DATASOURCE.check(), true);
		}else{
			throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100,APIConstants.DATA_SOURCE));
		}
		
		/**
		 * Aggregation mandatory fields need to be present and it's field name should be in database acceptable field
		 */
		if(checkNull(requestParamsDTO.getAggregations())){
			for(Map<String,String> aggregate : requestParamsDTO.getAggregations()){
				if(!aggregate.containsKey(APIConstants.FormulaFields.REQUEST_VALUES.getField()) || !aggregate.containsKey(APIConstants.FormulaFields.NAME.getField()) || !checkNull(aggregate.get(APIConstants.FormulaFields.NAME.getField())) || !aggregate.containsKey(APIConstants.FormulaFields.FORMULA.getField()) || !checkNull(aggregate.get(APIConstants.FormulaFields.FORMULA.getField())) || !aggregate.containsKey(aggregate.get(APIConstants.FormulaFields.REQUEST_VALUES.getField())) || !checkNull(aggregate.get(aggregate.get(APIConstants.FormulaFields.REQUEST_VALUES.getField())))){
					throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100,APIConstants.AGGREGATE_ATTRIBUTE));
				}else{
					if(!fieldData.contains(aggregate.get(aggregate.get(APIConstants.FormulaFields.REQUEST_VALUES.getField())))){
						throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103,new String[]{APIConstants.AGGREGATE_ATTRIBUTE,aggregate.get(aggregate.get(APIConstants.FormulaFields.REQUEST_VALUES.getField()))}));
					}
					if(!baseConnectionService.getFormulaOperations().contains(aggregate.get(APIConstants.FormulaFields.FORMULA.getField()).toUpperCase())){
						throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103,new String[]{APIConstants.AGGREGATE_ATTRIBUTE,aggregate.get(APIConstants.FormulaFields.FORMULA.getField())}));
					}
					fieldData.add(aggregate.get(APIConstants.FormulaFields.NAME.getField()));
				}
			}
			processedData.put(APIConstants.Hasdatas.HAS_AGGREGATE.check(), true);
		}
		
		/**
		 * fields should not be EMPTY and should be a valid field specified in data source
		 */
		if (checkNull(requestParamsDTO.getFields())) {
			StringBuffer errorField = new StringBuffer();
			for(String field : requestParamsDTO.getFields().split(APIConstants.COMMA)){
				if(!fieldData.contains(field)){
					if(errorField.length() > 0){
						errorField.append(APIConstants.COMMA);
					}
					errorField.append(field);
				}
			}
			if(errorField.length() > 0){
				throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103, new String[]{APIConstants.FIELDS,errorField.toString()}));
			}
			processedData.put(APIConstants.Hasdatas.HAS_FEILDS.check(), true);
		}else{
			throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100,APIConstants.FIELDS));
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
			processedData.put(APIConstants.Hasdatas.HAS_GRANULARITY.check(), true);
		}
		
		/**
		 * If the aggregation is given then groupBy should not be EMPTY and vice versa.
		 */
		if (checkNull(requestParamsDTO.getGroupBy())) {
			if(!processedData.get(APIConstants.Hasdatas.HAS_AGGREGATE.check())){
				throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100,APIConstants.AGGREGATE_ATTRIBUTE));
			}
			StringBuffer errorField = new StringBuffer();
			for(String field : requestParamsDTO.getGroupBy().split(APIConstants.COMMA)){
				if(!fieldData.contains(field)){
					if(errorField.length() > 0){
						errorField.append(APIConstants.COMMA);
					}
					errorField.append(field);
				}
			}
			if(errorField.length() > 0){
				throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103, new String[]{APIConstants.GROUP_BY,errorField.toString()}));
			}
			processedData.put(APIConstants.Hasdatas.HAS_GROUPBY.check(), true);
		}else if(processedData.get(APIConstants.Hasdatas.HAS_AGGREGATE.check()) || processedData.get(APIConstants.Hasdatas.HAS_GRANULARITY.check())){
			throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100, APIConstants.GROUP_BY));
		}
		/**
		 * Range filter validation.Here groupBy field shouldn't be empty
		 */
		if(checkNull(requestParamsDTO.getRanges())) {
			for(RequestParamsRangeDTO ranges : requestParamsDTO.getRanges()) {
				if(!checkNull(ranges.getFrom()) && !checkNull(ranges.getTo())) {
					throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100,APIConstants.RANGE_ATTRIBUTE));				
				} 
			}
			if(requestParamsDTO.getGroupBy().split(APIConstants.SEPARATOR).length > 1) {
				throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E109,APIConstants.MULTIPLE_GROUPBY));
			}
			processedData.put(APIConstants.Hasdatas.HAS_RANGE.check(), true);
		}
		
		/**
		 * Filter mandatory fields should not be EMPTY and it should be acceptable field
		 */
		if (checkNull(requestParamsDTO.getFilter()) && checkNull(requestParamsDTO.getFilter().get(0))) {
			for(RequestParamsFilterDetailDTO logicalOperations : requestParamsDTO.getFilter()){
				
				if(!checkNull(logicalOperations.getLogicalOperatorPrefix())){
					throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100,APIConstants.LOGICAL_OPERATOR));
				}
				if(!baseConnectionService.getLogicalOperations().contains(logicalOperations.getLogicalOperatorPrefix().toUpperCase())){
					throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103,new String[]{APIConstants.LOGICAL_OPERATOR,logicalOperations.getLogicalOperatorPrefix()}));
				}
				if(!checkNull(logicalOperations.getFields())){
					throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100,APIConstants.FILTER_FIELDS));
				}
				for(RequestParamsFilterFieldsDTO filters : logicalOperations.getFields()){

					if(!checkNull(filters.getFieldName()) || !checkNull(filters.getOperator()) || !checkNull(filters.getValueType()) || !checkNull(filters.getValue()) || !checkNull(filters.getType())){
						throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E100,APIConstants.FILTERS));
					}
					if(!baseConnectionService.getDataTypes().contains(filters.getValueType().toUpperCase())){
						throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103,new String[]{APIConstants.FILTERS,filters.getValueType()}));
					}else{
						/** future validation for date field for range 
						 *
						 *if(APIConstants.DataTypes.DATE.dataType().equalsIgnoreCase(filters.getValueType())){
						 *} 
						 */
					}
					if(!filters.getType().equalsIgnoreCase(APIConstants.SELECTOR)){
						throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103,new String[]{APIConstants.FILTERS,filters.getType()}));
					}
					if(!fieldData.contains(filters.getFieldName())){
						throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103,new String[]{APIConstants.FILTERS,filters.getFieldName()}));
					}
					if(!baseConnectionService.getEsOperations().contains(filters.getOperator().toUpperCase())){
						throw new BadRequestException(MessageHandler.getMessage(ErrorConstants.E103,new String[]{APIConstants.FILTERS,filters.getOperator()}));
					}
				}
			}
			processedData.put(APIConstants.Hasdatas.HAS_FILTER.check(), true);
		}

		/**
		 * Check for pagination and the sortBy field should not be EMPTY and it should be valid field.
		 */
		if (checkNull(requestParamsDTO.getPagination())) {
			processedData.put(APIConstants.Hasdatas.HAS_PAGINATION.check(), true);
			if (checkNull(requestParamsDTO.getPagination().getLimit())) {
				processedData.put(APIConstants.Hasdatas.HAS_LIMIT.check(), true);
			}
			if (checkNull(requestParamsDTO.getPagination().getOffset())) {
				processedData.put(APIConstants.Hasdatas.HAS_Offset.check(), true);
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
						processedData.put(APIConstants.Hasdatas.HAS_SORTORDER.check(), true);
					}
					processedData.put(APIConstants.Hasdatas.HAS_SORTBY.check(), true);
				}
			}
		}
		return processedData;
	}

	public String[] getIndices(String names) {
		String[] indices = new String[names.split(APIConstants.COMMA).length];
		int index = 0;
		for (String name : names.split(APIConstants.COMMA)) {
			if (baseConnectionService.getIndexMap().containsKey(name)){
				indices[index] = baseConnectionService.getIndexMap().get(name);
				index++;
			}
		}
		return indices;
	}

	public RequestParamsDTO validateUserRole(String traceId,RequestParamsDTO requestParamsDTO, Map<String, Object> userMap) {
		
		String gooruUId = userMap.containsKey(APIConstants.GOORUUID) ? userMap.get(APIConstants.GOORUUID).toString() : null;

		Map<Integer,String> errorMap = new HashMap<Integer, String>();
		Map<String, Set<String>> partyPermissions = (Map<String, Set<String>>) userMap.get(APIConstants.PERMISSIONS);
		logger.info(APIConstants.GOORUUID+APIConstants.SEPARATOR+gooruUId);
		logger.info(APIConstants.PERMISSIONS+APIConstants.SEPARATOR+partyPermissions);
		
		if(!StringUtils.isBlank(validateUserPermissionService.getRoleBasedParty(traceId,partyPermissions,APIConstants.AP_ALL_PARTY_ALL_DATA))){
			return requestParamsDTO;
		}

		Map<String, Object> userFilters = validateUserPermissionService.getUserFilters(gooruUId);
		Map<String, Object> userFiltersAndValues = validateUserPermissionService.getUserFiltersAndValues(requestParamsDTO.getFilter());
		Set<String> userFilterOrgValues = (Set<String>) userFiltersAndValues.get("orgFilters");
		Set<String> userFilterUserValues = (Set<String>) userFiltersAndValues.get("userFilters");

		String partyAlldataPerm = validateUserPermissionService.getRoleBasedParty(traceId,partyPermissions,APIConstants.AP_PARTY_ALL_DATA);
		
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
//			throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E104, ErrorConstants.E_PII));
			return businessLogicService.changeDataSourceUserToAnonymousUser(requestParamsDTO);
		}
		if (partyPermissions.isEmpty() && (requestParamsDTO.getDataSource().matches(APIConstants.ACTIVITYDATASOURCES) && StringUtils.isBlank(requestParamsDTO.getGroupBy()))) {
			errorMap.put(403,MessageHandler.getMessage(ErrorConstants.E104, ErrorConstants.E_RAW));
			throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E104, ErrorConstants.E_RAW));
		}

		if (!userFilterOrgValues.isEmpty()) {
			validateUserPermissionService.validateOrganization(requestParamsDTO, partyPermissions, errorMap, userFilterOrgValues);
		} else {
			String allowedParty = validateUserPermissionService.getAllowedParties(traceId,requestParamsDTO, partyPermissions);
			if (!StringUtils.isBlank(allowedParty)) {
				if(requestParamsDTO.getDataSource().matches(APIConstants.USERDATASOURCES)){
					validateUserPermissionService.addSystemUserOrgFilter(requestParamsDTO.getFilter(), allowedParty);
				}else{
					validateUserPermissionService.addSystemContentUserOrgFilter(requestParamsDTO.getFilter(), allowedParty);
				}
			} else {
				throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E108));
			}
		}

		JSONSerializer serializer = new JSONSerializer();
		serializer.transform(new ExcludeNullTransformer(), void.class).exclude(APIConstants.EXCLUDE_CLASSES);
		logger.info(APIConstants.NEW_QUERY+serializer.deepSerialize(requestParamsDTO));
		return requestParamsDTO;
	}

	public Map<String, Object> getRequestFieldNameValueInMap(HttpServletRequest request, String prefix) {
	     Map<String, Object> requestFieldNameValue = new HashMap<String, Object>();
         Enumeration paramNames = request.getParameterNames();
         while (paramNames.hasMoreElements()) {
                 String paramName = (String) paramNames.nextElement();
                 if (paramName.startsWith(prefix+APIConstants.DOT)) {
                	 requestFieldNameValue.put(paramName.replace(prefix+APIConstants.DOT, APIConstants.EMPTY), request.getParameter(paramName));
                 }
         }
         return requestFieldNameValue;
	}
}

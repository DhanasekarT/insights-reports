package org.gooru.insights.services;

import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.CassandraConstants.columnFamilies;
import org.gooru.insights.constants.CassandraConstants.keyspaces;
import org.gooru.insights.constants.ErrorCodes;
import org.gooru.insights.models.RequestParamsCoreDTO;
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
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

import flexjson.JSONSerializer;

@Service
public class ItemServiceImpl implements ItemService, APIConstants,ErrorCodes {

	@Autowired
	BaseAPIService baseAPIService;

	@Autowired
	BaseESService esService;

	@Autowired
	BusinessLogicService businessLogicService;

	@Autowired
	BaseConnectionService baseConnectionService;

	@Autowired
	BaseCassandraService baseCassandraService;

	@Autowired
	ExcelBuilderService excelBuilderService;
	
	@Autowired
	CSVBuilderService csvBuilderService;
	
	JSONSerializer serializer = new JSONSerializer();
	
	public JSONArray processApi(String data, Map<String, Object> dataMap, Map<Integer, String> errorMap) {

		List<Map<String, Object>> resultData = new ArrayList<Map<String, Object>>();
		try {
			RequestParamsCoreDTO requestParamsCoreDTO = baseAPIService.buildRequestParamsCoreDTO(data);

			if (baseAPIService.checkNull(requestParamsCoreDTO.getRequestParamsDTO())) {
				List<RequestParamsDTO> requestParamsDTOs = requestParamsCoreDTO.getRequestParamsDTO();

				String previousAPIKey = null;
				for (RequestParamsDTO api : requestParamsDTOs) {
					if (!baseAPIService.checkNull(api)) {
						continue;
					}
					List<Map<String, Object>> tempData = new ArrayList<Map<String, Object>>();
					tempData = getData(api, dataMap, errorMap);
					if (baseAPIService.checkNull(previousAPIKey)) {
						resultData = businessLogicService.leftJoin(resultData, tempData, previousAPIKey, api.getApiJoinKey());
					}
				}

				if (baseAPIService.checkNull(requestParamsCoreDTO.getCoreKey())) {
					resultData = businessLogicService.formatAggregateKeyValueJson(resultData, requestParamsCoreDTO.getCoreKey());
				}

			} else {
				return new JSONArray();
			}

			return businessLogicService.buildAggregateJSON(resultData);
		} catch (Exception e) {
			e.printStackTrace();
			errorMap.put(400, E1014);
			return new JSONArray();
		}

	}

	public List<Map<String, Object>> getData(RequestParamsDTO requestParamsDTO, Map<String, Object> dataMap, Map<Integer, String> errorMap) {

		Map<String, Boolean> validatedData = baseAPIService.validateData(requestParamsDTO);

		if (!validatedData.get(hasdata.HAS_DATASOURCE.check())) {
			errorMap.put(400, E1016);
			return new ArrayList<Map<String, Object>>();
		}

		String[] indices = baseAPIService.getIndices(requestParamsDTO.getDataSource().toLowerCase());
		List<Map<String, Object>> resultList = esService.generateQuery(requestParamsDTO, indices, validatedData, dataMap, errorMap);
		return resultList;
	}
	
	public JSONArray getExportReportArray(HttpServletRequest request,String reportType, Map<String, Object> dataMap, Map<String, Object> userMap, Map<Integer, String> errorMap) {
		RequestParamsDTO systemRequestParamsDTO = null;
		
		 Map<String, Object> filtersMap = new HashMap<String, Object>();
         Enumeration paramNames = request.getParameterNames();
         while (paramNames.hasMoreElements()) {
                 String paramName = (String) paramNames.nextElement();
                 filtersMap.put(paramName, request.getParameter(paramName));
         }
         
		if(filtersMap.isEmpty() || !filtersMap.containsKey(START_DATE) || !filtersMap.containsKey(START_DATE) ||
				((filtersMap.containsKey(START_DATE) && StringUtils.isBlank(filtersMap.get(START_DATE).toString())) 
				&& (filtersMap.containsKey(END_DATE) && StringUtils.isBlank(filtersMap.get(END_DATE).toString())))){
			errorMap.put(400, E1030);
			return new JSONArray();
		}
		
		Column<String> val = baseCassandraService.readColumnValue(keyspaces.INSIGHTS.keyspace(), columnFamilies.QUERY_REPORTS.columnFamily(), DI_REPORTS,reportType);
		
		if(val == null){
			errorMap.put(400, E1018);
			return new JSONArray();
		}
		
		ColumnList<String> columns = baseCassandraService.read(keyspaces.INSIGHTS.keyspace(), columnFamilies.QUERY_REPORTS.columnFamily(), val.getStringValue());
		
		systemRequestParamsDTO = baseAPIService.buildRequestParameters(columns.getStringValue("query", null));
		
		for(RequestParamsFilterDetailDTO systemFieldsDTO : systemRequestParamsDTO.getFilter()) {
			List<RequestParamsFilterFieldsDTO> systemFields = systemFieldsDTO.getFields();
			systemFields.clear();
			for (String key : filtersMap.keySet()) {
				RequestParamsFilterFieldsDTO systemfieldsDetails = new RequestParamsFilterFieldsDTO();
				if (!key.matches(PAGINATION_PARAMS)) {
					systemfieldsDetails.setFieldName(key);
					systemfieldsDetails.setOperator("in");
					systemfieldsDetails.setValueType("String");
				} else if (key.equalsIgnoreCase("startDate")) {
					systemfieldsDetails.setFieldName("eventTime");
					systemfieldsDetails.setOperator("ge");
					systemfieldsDetails.setValueType("Date");
					systemfieldsDetails.setFormat("yyyy-MM-dd");
				} else if (key.equalsIgnoreCase("endDate")) {
					systemfieldsDetails.setFieldName("eventTime");
					systemfieldsDetails.setOperator("le");
					systemfieldsDetails.setValueType("Date");
					systemfieldsDetails.setFormat("yyyy-MM-dd");
				}
				systemfieldsDetails.setType("selector");
				systemfieldsDetails.setValue(filtersMap.get(key).toString());

				systemFields.add(systemfieldsDetails);
			}
			systemFieldsDTO.setFields(systemFields);
		}
		

		if(!filtersMap.isEmpty()){
			if(filtersMap.containsKey("limit")){
				systemRequestParamsDTO.getPagination().setLimit(Integer.valueOf(""+filtersMap.get("limit")));
			}
			if(filtersMap.containsKey("offset")){
				systemRequestParamsDTO.getPagination().setOffset(Integer.valueOf(""+filtersMap.get("offset")));
			}
			if(filtersMap.containsKey("sortOrder")){
				for(RequestParamsSortDTO requestParamsSortDTO :   systemRequestParamsDTO.getPagination().getOrder()){
					requestParamsSortDTO.setSortOrder(filtersMap.get("sortOrder").toString());
				}
			}
		}
		
		
		System.out.print("\n Old Object : " + columns.getStringValue("query", null)+ "\n\n");

		serializer.transform(new ExcludeNullTransformer(), void.class).exclude("*.class");
		
		String datas = serializer.deepSerialize(systemRequestParamsDTO);
		
		System.out.print("\n newObject : " + datas);

		
		if(columns.getStringValue("query", null) != null){			
			return generateQuery(datas, dataMap, userMap, errorMap);
		}
		
		return new JSONArray();
	}

	public JSONArray getPartyReport(HttpServletRequest request,String reportType, Map<String, Object> dataMap, Map<String, Object> userMap, Map<Integer, String> errorMap) {
		RequestParamsDTO systemRequestParamsDTO = null;
		boolean isMerged = false;

		Map<String,Object> filtersMap = baseAPIService.getRequestFieldNameValueInMap(request, "f");
		Map<String,Object> paginationMap = baseAPIService.getRequestFieldNameValueInMap(request, "p");
		
		if(filtersMap.isEmpty()){
			errorMap.put(400, E1015);
			return new JSONArray();
		}
		
		Column<String> val = baseCassandraService.readColumnValue(keyspaces.INSIGHTS.keyspace(), columnFamilies.QUERY_REPORTS.columnFamily(), DI_REPORTS,reportType);
		
		if(val == null){
			errorMap.put(400, E1018);
			return new JSONArray();
		}
		
		ColumnList<String> columns = baseCassandraService.read(keyspaces.INSIGHTS.keyspace(), columnFamilies.QUERY_REPORTS.columnFamily(), val.getStringValue());
		
		systemRequestParamsDTO = baseAPIService.buildRequestParameters(columns.getStringValue("query", null));
		for(RequestParamsFilterDetailDTO systemFieldData : systemRequestParamsDTO.getFilter()) {
			for(RequestParamsFilterFieldsDTO systemfieldsDetails : systemFieldData.getFields()) {
				if(filtersMap.containsKey(systemfieldsDetails.getFieldName())){
					isMerged = true;
					String[] values = filtersMap.get(systemfieldsDetails.getFieldName()).toString().split(",");
					systemfieldsDetails.setValue(filtersMap.get(systemfieldsDetails.getFieldName()).toString());
					if(values.length > 1){
						systemfieldsDetails.setOperator("in");
					}
				}
			}
		}
		if(!isMerged){
			errorMap.put(400, E1017);
			return new JSONArray();
		}

		if(!paginationMap.isEmpty()){
			if(paginationMap.containsKey("limit")){
				systemRequestParamsDTO.getPagination().setLimit(Integer.valueOf(""+paginationMap.get("limit")));
			}
			if(paginationMap.containsKey("offset")){
				systemRequestParamsDTO.getPagination().setOffset(Integer.valueOf(""+paginationMap.get("offset")));
			}
			if(paginationMap.containsKey("sortOrder")){
				for(RequestParamsSortDTO requestParamsSortDTO :   systemRequestParamsDTO.getPagination().getOrder()){
					requestParamsSortDTO.setSortOrder(paginationMap.get("sortOrder").toString());
				}
			}
		}
		
		
		System.out.print("\n Old Object : " + columns.getStringValue("query", null)+ "\n\n");

		serializer.transform(new ExcludeNullTransformer(), void.class).exclude("*.class");
		
		String datas = serializer.deepSerialize(systemRequestParamsDTO);
		
		System.out.print("\n newObject : " + datas);

		
		if(columns.getStringValue("query", null) != null){			
			return generateQuery(datas, dataMap, userMap, errorMap);
		}
		
		return new JSONArray();
	}
	public List<Map<String, Object>> generateReportFile(JSONArray activityArray, Map<String, Object> dataMap, Map<Integer, String> errorData) {
		List<Map<String, Object>> activityList = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> filesMap = new ArrayList<Map<String, Object>>();
		try {

			// ReportData is generated here
			getReportDataList(activityArray, activityList, errorData);

			Map<String, Object> files = new HashMap<String, Object>();
			String fileName = null;
			fileName = csvBuilderService.generateCSVMapReport(activityList, fileName + "_" + MINUTE_DATE_FORMATTER.format(new Date()) + ".csv");
			if (fileName != null) {
				files.put("file", fileName);
				filesMap.add(files);
				return filesMap;
			} else {
				errorData.put(204, "Content is unavailable for your request.");
				return filesMap;
			}
		} catch (Exception e) {
			// TODO Add Error Handling
			//e.printStackTrace();
			errorData.put(500, "At this time, we are unable to process your request. Please try again by changing your request or contact developer");
			return filesMap;
		}
	}
	
	public void getReportDataList(JSONArray activityArray, List<Map<String, Object>> activityList, Map<Integer, String> errorAsMap) throws JSONException, Exception {

		if (activityArray.length() > 0) {
			for (int index = 0; index < activityArray.length(); index++) {
				JSONObject activityJsonObject = activityArray.getJSONObject(index);
				if (!activityJsonObject.isNull("eventId") && StringUtils.isNotBlank(activityJsonObject.get("eventId").toString())) {
					Map<String, Object> activityAsMap = new HashMap<String, Object>();
					if (activityJsonObject.get("eventName").toString().matches(XAPI_SUPPORTED_EVENTS)) {
						/* Unique Activity Id */
						activityAsMap.put("id", activityJsonObject.get("eventId"));

						/* Actor Property starts here */
						Map<String, Object> actorAsMap = new HashMap<String, Object>(1);
						if ((!activityJsonObject.isNull("gooruUId") && StringUtils.isNotBlank(activityJsonObject.get("gooruUId").toString()))) {
							businessLogicService.generateActorProperty(activityJsonObject, actorAsMap, errorAsMap);
							if (!actorAsMap.isEmpty()) {
								activityAsMap.put("actor", actorAsMap);
							}
						}
						/* Verb Property starts here */
						Map<String, Object> verbAsMap = new HashMap<String, Object>();
						if (!activityJsonObject.isNull("eventName") && StringUtils.isNotBlank(activityJsonObject.get("eventName").toString())) {
							businessLogicService.generateVerbProperty(activityJsonObject, verbAsMap, errorAsMap);
							if (!verbAsMap.isEmpty()) {
								activityAsMap.put("verb", verbAsMap);
							}
						}
						/* Object Property starts here */
						Map<String, Object> objectAsMap = new HashMap<String, Object>();
						businessLogicService.generateObjectProperty(activityJsonObject, objectAsMap, errorAsMap);
						if (!objectAsMap.isEmpty()) {
							activityAsMap.put("object", objectAsMap);
						}
						/* Context Property starts here */
						Map<String, Object> contextAsMap = new HashMap<String, Object>();
						businessLogicService.generateContextProperty(activityJsonObject, contextAsMap, errorAsMap);
						if (!contextAsMap.isEmpty()) {
							Map<String, Object> contextActivitiesMap = new HashMap<String, Object>();
							contextActivitiesMap.put("contextActivities", contextAsMap);
							activityAsMap.put("context", contextActivitiesMap);
						}
						/* Result Property starts here */
						Map<String, Object> resultAsMap = new HashMap<String, Object>();
						businessLogicService.generateResultProperty(activityJsonObject, resultAsMap, errorAsMap);
						if (!resultAsMap.isEmpty()) {
							activityAsMap.put("result", resultAsMap);
						}
						String eventTime = null;
						if (!activityJsonObject.isNull("eventTime") && StringUtils.isNotBlank(activityJsonObject.get("eventTime").toString())) {
							eventTime = activityJsonObject.get("eventTime").toString();
						} else if (!activityJsonObject.isNull("startTime") && StringUtils.isNotBlank(activityJsonObject.get("startTime").toString())) {
							eventTime = activityJsonObject.get("startTime").toString();
						}
						activityAsMap.put("timestamp", eventTime);
						activityAsMap.put("stored", eventTime);

						/*
						 * Map<String, Object> responseAsMap = (Map<String, Object>) mapper.readValue(activityArray.toString, new
						 * TypeReference<Map<String, Object>>() { });
						 */
						if (!objectAsMap.isEmpty() && !actorAsMap.isEmpty() && !verbAsMap.isEmpty() && !activityAsMap.isEmpty()) {
							activityList.add(activityAsMap);
						}
					}
				}
			}
		}
	}
	
	public JSONArray generateQuery(String data, Map<String, Object> messageData, Map<String, Object> userMap, Map<Integer, String> errorData) {
		
		RequestParamsDTO requestParamsDTO = null;
		try {
			requestParamsDTO = baseAPIService.buildRequestParameters(data);
		} catch (Exception e) {
			errorData.put(400, E1014);
			return new JSONArray();
		}
		
		//Map<String, Boolean> checkPoint = baseAPIService.validateData(requestParamsDTO);
		Map<String, Boolean> checkPoint = new HashMap<String, Boolean>();
		
		if (!baseAPIService.checkPoint(requestParamsDTO, checkPoint, errorData)) {
			return new JSONArray();
		}

		/*
		 * Additional filters are added based on user authentication
		 */
		requestParamsDTO = baseAPIService.validateUserRole(requestParamsDTO, userMap, errorData);

		if (!errorData.isEmpty()) {
			return new JSONArray();
		}

		String[] indices = baseAPIService.getIndices(requestParamsDTO.getDataSource().toLowerCase());
		List<Map<String, Object>> resultList = esService.generateQuery(requestParamsDTO, indices, checkPoint, messageData, errorData);
		
		try {
			JSONArray jsonArray = businessLogicService.buildAggregateJSON(resultList);
			
			/*
			 * save data to redis
			 */
			baseAPIService.saveQuery(requestParamsDTO,jsonArray,data,messageData,userMap);
			
			return jsonArray;
		} catch (JSONException e) {
			e.printStackTrace();
			return new JSONArray();
		}

	}

	public Boolean clearQuery(String id) {
		return baseAPIService.clearQuery(id);
	}

	public JSONArray getQuery(String prefix,String id,Map<String,Object> dataMap) {
		String result = baseAPIService.getQuery(prefix,id);
		try {
			JSONObject jsonObject = new JSONObject(result);
			Map<String,Object> messageMap = new Gson().fromJson(jsonObject.getString("message"), dataMap.getClass());
			dataMap.putAll(messageMap);
			return new JSONArray(jsonObject.getString("data"));
		} catch (Exception e) {
			return new JSONArray();
		}
	}

	
	
	public JSONArray getCacheData(String prefix,String id) {

		JSONArray resultArray = new JSONArray();
		try {
			if (id != null && !id.isEmpty()) {
				for (String requestId : id.split(",")) {
					JSONArray jsonArray = new JSONArray();
					do {
						JSONObject jsonObject = new JSONObject();
						jsonObject.put(requestId, baseAPIService.getKey(prefix+requestId) != null ? baseAPIService.getKey(prefix+requestId) : "");
						requestId = baseAPIService.getKey(prefix+requestId);
						jsonArray.put(jsonObject);
					} while (baseAPIService.hasKey(prefix+requestId));
					resultArray.put(jsonArray);
				}
			} else {
				Set<String> keyIds = baseAPIService.getKeys();
				Set<String> customizedKey = new HashSet<String>();
				for (String keyId : keyIds) {
					if (keyId.contains(CACHE_PREFIX + SEPARATOR + CACHE_PREFIX_ID+prefix)) {
						customizedKey.add(keyId.replaceAll(CACHE_PREFIX + SEPARATOR + CACHE_PREFIX_ID + SEPARATOR+prefix, ""));
					}else{
					customizedKey.add(keyId.replaceAll(CACHE_PREFIX + SEPARATOR+prefix, ""));
					}
				}
				for (String requestId : customizedKey) {
						JSONObject jsonObject = new JSONObject();
						jsonObject.put(requestId, baseAPIService.getKey(prefix+requestId) != null ? baseAPIService.getKey(prefix+requestId) : "");
						requestId = baseAPIService.getKey(prefix+requestId);
						resultArray.put(jsonObject);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultArray;
	}

	public Map<Integer,String> manageReports(String action,String reportName,String data,Map<Integer,String> errorMap){
		if(action.equalsIgnoreCase("add")){
			Column<String> val = baseCassandraService.readColumnValue(keyspaces.INSIGHTS.keyspace(), columnFamilies.QUERY_REPORTS.columnFamily(), DI_REPORTS,reportName);
			
			if(val == null || (val !=null && StringUtils.isBlank(val.getStringValue()))){
				try {
					RequestParamsDTO requestParamsDTO = baseAPIService.buildRequestParameters(data);
				} catch (Exception e) {
					errorMap.put(400,E1014);
					return errorMap;
				}	
				
				UUID reportId = UUID.randomUUID();
	
				baseCassandraService.saveStringValue(keyspaces.INSIGHTS.keyspace(), columnFamilies.QUERY_REPORTS.columnFamily(), DI_REPORTS, reportName, reportId.toString());
				baseCassandraService.saveStringValue(keyspaces.INSIGHTS.keyspace(), columnFamilies.QUERY_REPORTS.columnFamily(), reportId.toString(), "query", data);
				
				errorMap.put(200,E1019);
				return errorMap;
			}else{
				errorMap.put(403,E1020);
			}
		}
		else if(action.equalsIgnoreCase("update")){
			Column<String> val = baseCassandraService.readColumnValue(keyspaces.INSIGHTS.keyspace(), columnFamilies.QUERY_REPORTS.columnFamily(), DI_REPORTS,reportName);
			
			if(val !=null && !StringUtils.isBlank(val.getStringValue())){
				try {
					RequestParamsDTO requestParamsDTO = baseAPIService.buildRequestParameters(data);
				} catch (Exception e) {
					errorMap.put(400,E1014);
					return errorMap;
				}	
				
				baseCassandraService.saveStringValue(keyspaces.INSIGHTS.keyspace(), columnFamilies.QUERY_REPORTS.columnFamily(), val.getStringValue(), "query", data);
				
				errorMap.put(200,E1019);
				return errorMap;
			}else{
				errorMap.put(403,E1020);
			}
		}
				
		return errorMap;	
	}

	public boolean insertKey(String data){
		return baseAPIService.insertKey(data);
	}
	
	public boolean clearDataCache() {
		return baseConnectionService.clearDataCache();
	}

	public void clearConnectionCache() {
		baseConnectionService.clearConnectionCache();
	}

	public Map<String, Object> getUserObject(String sessionToken, Map<Integer, String> errorMap) {
		return baseConnectionService.getUserObject(sessionToken, errorMap);
	}

	public Map<String, Object> getUserObjectData(String sessionToken, Map<Integer, String> errorMap) {
		return baseConnectionService.getUserObjectData(sessionToken, errorMap);
	}
	
}

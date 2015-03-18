package org.gooru.insights.services;

import java.net.InetAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
import org.codehaus.jackson.map.ObjectMapper;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.CassandraConstants.columnFamilies;
import org.gooru.insights.constants.CassandraConstants.keyspaces;
import org.gooru.insights.constants.ErrorCodes;
import org.gooru.insights.constants.TypeConverter;
import org.gooru.insights.models.RequestParamsCoreDTO;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsFilterDetailDTO;
import org.gooru.insights.models.RequestParamsFilterFieldsDTO;
import org.gooru.insights.models.RequestParamsSortDTO;
import org.gooru.insights.models.ServerLocation;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

	@Autowired
	MailService mailService;

	@Autowired
	GeoLocationService geoLocationService;
	
	JSONSerializer serializer = new JSONSerializer();

	private int EXPORT_ROW_LIMIT = 100;

	 private static final Logger logger = LoggerFactory.getLogger(ItemServiceImpl.class);
	 
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
	
	public void calculateScore(HttpServletRequest request,String reportType, Map<String, Object> dataMap,Map<String, Object> userMap, Map<Integer, String> errorMap,String eventId) {
		System.out.print("\nProcessing ...");
		long start = System.currentTimeMillis();;
		RequestParamsDTO systemRequestParamsDTO = null;
		
		Column<String> val = baseCassandraService.readColumnValue(keyspaces.INSIGHTS.keyspace(), columnFamilies.QUERY_REPORTS.columnFamily(), DI_REPORTS,reportType);
		
		ColumnList<String> columns = baseCassandraService.read(keyspaces.INSIGHTS.keyspace(), columnFamilies.QUERY_REPORTS.columnFamily(), val.getStringValue());
		
		systemRequestParamsDTO = baseAPIService.buildRequestParameters(columns.getStringValue("query", null));
		
		serializer.transform(new ExcludeNullTransformer(), void.class).exclude("*.class");
		
		resourceEventing(systemRequestParamsDTO, eventId);

		Map<String, Boolean> checkPoint = baseAPIService.validateData(systemRequestParamsDTO);
		String[] indices = baseAPIService.getIndices(systemRequestParamsDTO.getDataSource().toLowerCase());
		
		String datas = serializer.deepSerialize(systemRequestParamsDTO);
		
		System.out.print("\n newObject" + datas);
		
		JSONArray resultSet = null;		
		
		try {
			List<Map<String, Object>> resultList = esService.generateQuery(systemRequestParamsDTO, indices, checkPoint, dataMap, errorMap);
			resultSet = businessLogicService.buildAggregateJSON(resultList);
			int totalRows = (Integer) dataMap.get("totalRows");
			System.out.print("\n totalRows : " + totalRows);

			for (int index = 0; index < resultSet.length(); index++) {
				JSONObject activityJsonObject = resultSet.getJSONObject(index);
				System.out.print("\n attemptStatus : " + activityJsonObject.get("attemptStatus").toString());
				System.out.print("\n gooruOid : " + activityJsonObject.get("gooruOid").toString());
				System.out.print("\n attemptCount : " + activityJsonObject.get("attemptCount").toString());
				int newScore = 0;
				if(activityJsonObject.get("attemptStatus") != null){
					String attempStatus = activityJsonObject.get("attemptStatus").toString();
					int[] attempStatusArray =  convertStringToIntArray(attempStatus);
					if(attempStatusArray.length > 0){
						newScore = attempStatusArray[(attempStatusArray.length - 1)];
					}
				}
				System.out.print("\n newScore for Question : " + newScore);
				if(newScore > 0 && activityJsonObject.get("gooruOid") != null){
					baseCassandraService.saveIntegerValue(keyspaces.INSIGHTS.keyspace(), columnFamilies.ASSESSMENT_SCORE.columnFamily(), eventId, activityJsonObject.get("gooruOid").toString(), newScore);
				}
			}
			
			ColumnList<String> assessmentList = baseCassandraService.read(keyspaces.INSIGHTS.keyspace(), columnFamilies.ASSESSMENT_SCORE.columnFamily(), eventId);
			int assScore = 0;
			
			for(Column<String> question : assessmentList){
				assScore = (assScore+question.getIntegerValue());
			}
			
			System.out.print("\n Assessment Score : " + assScore);
			
			baseCassandraService.saveIntegerValue(keyspaces.INSIGHTS.keyspace(), columnFamilies.ASSESSMENT_SCORE.columnFamily(), eventId, "newAssScore", assScore);
			
			System.out.print("\n Indexing event.. : ");
			
			esService.singeColumnUpdate("prod", "event_logger_info_20141231", "event_detail", eventId, "new_score", assScore);
		
			long stop = System.currentTimeMillis();;
			
			System.out.print("\n Time take to complete process: " + (stop-start));
			
			
		} catch (Exception e) {
			errorMap.put(500, "At this time, we are unable to process your request. Please try again by changing your request or contact developer");
		}			
		
		
	}

	public int[] convertStringToIntArray(String value){

		String[] items = value.replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\"", "").split(",");

		int[] results = new int[items.length];

		for (int i = 0; i < items.length; i++) {
			try {
				results[i] = Integer.parseInt(items[i]);
			} catch (NumberFormatException nfe) {};
		}	
		
		return results;
	}
	
	public void resourceEventing(RequestParamsDTO systemRequestParamsDTO1, String id) {

		for (RequestParamsFilterDetailDTO systemFieldsDTO : systemRequestParamsDTO1.getFilter()) {
			List<RequestParamsFilterFieldsDTO> systemFields = new ArrayList<RequestParamsFilterFieldsDTO>();
			RequestParamsFilterFieldsDTO systemfieldsDetails = null;
			systemfieldsDetails = new RequestParamsFilterFieldsDTO();
			systemfieldsDetails.setFieldName("parentEventId");
			systemfieldsDetails.setOperator("eq");
			systemfieldsDetails.setValueType("String");
			systemfieldsDetails.setType("selector");
			systemfieldsDetails.setValue(id);
			systemFields.add(systemfieldsDetails);
			systemFieldsDTO.setFields(systemFields);
		}
	}
	public void getExportReportArray(HttpServletRequest request,String reportType, Map<String, Object> dataMap, Map<String, Object> userMap, Map<Integer, String> errorMap,String emailId,String fileName) {
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
		}
		
		Column<String> val = baseCassandraService.readColumnValue(keyspaces.INSIGHTS.keyspace(), columnFamilies.QUERY_REPORTS.columnFamily(), DI_REPORTS,reportType);
		
		if(val == null){
			errorMap.put(400, E1018);
		}
		
		ColumnList<String> columns = baseCassandraService.read(keyspaces.INSIGHTS.keyspace(), columnFamilies.QUERY_REPORTS.columnFamily(), val.getStringValue());
		
		systemRequestParamsDTO = baseAPIService.buildRequestParameters(columns.getStringValue("query", null));
		
		Map<String, Boolean> checkPoint = baseAPIService.validateData(systemRequestParamsDTO);
		systemRequestParamsDTO = baseAPIService.validateUserRole(systemRequestParamsDTO, userMap, errorMap);
		String[] indices = baseAPIService.getIndices(systemRequestParamsDTO.getDataSource().toLowerCase());
		
		for(RequestParamsFilterDetailDTO systemFieldsDTO : systemRequestParamsDTO.getFilter()) {
			List<RequestParamsFilterFieldsDTO> systemFields = new ArrayList<RequestParamsFilterFieldsDTO>();
			for (String key : filtersMap.keySet()) {
				RequestParamsFilterFieldsDTO systemfieldsDetails = null;
				if (!key.matches(PAGINATION_PARAMS)) {
					systemfieldsDetails = new RequestParamsFilterFieldsDTO();
					systemfieldsDetails.setFieldName(key);
					systemfieldsDetails.setOperator("in");
					systemfieldsDetails.setValueType("String");
					systemfieldsDetails.setType("selector");
					systemfieldsDetails.setValue(filtersMap.get(key).toString());
					systemFields.add(systemfieldsDetails);
				} else if (key.equalsIgnoreCase("startDate")) {
					systemfieldsDetails = new RequestParamsFilterFieldsDTO();
					systemfieldsDetails.setFieldName("eventTime");
					systemfieldsDetails.setOperator("ge");
					systemfieldsDetails.setValueType("Date");
					systemfieldsDetails.setFormat("yyyy-MM-dd");
					systemfieldsDetails.setType("selector");
					systemfieldsDetails.setValue(filtersMap.get(key).toString());
					systemFields.add(systemfieldsDetails);
				} else if (key.equalsIgnoreCase("endDate")) {
					systemfieldsDetails = new RequestParamsFilterFieldsDTO();
					systemfieldsDetails.setFieldName("eventTime");
					systemfieldsDetails.setOperator("le");
					systemfieldsDetails.setValueType("Date");
					systemfieldsDetails.setFormat("yyyy-MM-dd");
					systemfieldsDetails.setType("selector");
					systemfieldsDetails.setValue(filtersMap.get(key).toString());
					systemFields.add(systemfieldsDetails);
				}
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
				

		serializer.transform(new ExcludeNullTransformer(), void.class).exclude("*.class");
		
		String datas = serializer.deepSerialize(systemRequestParamsDTO);
		
		System.out.print("\n newObject : " + datas);

		JSONArray resultSet = null;
		if(StringUtils.isBlank(fileName)){
			fileName = "activity" + "_" + MINUTE_DATE_FORMATTER.format(new Date()) + ".csv";
		}else{
			fileName = fileName+".csv";
		}
		String resultFileName = "http://www.goorulearning.org/insights/api/v2/report/"+fileName;
		if (columns.getStringValue("query", null) != null) {
			try {
			resultSet = generateQuery(datas, dataMap, userMap, errorMap);
			generateReportFile(reportType, resultSet, dataMap, errorMap,fileName,true);
			int totalRows = (Integer) dataMap.get("totalRows");
			System.out.print("totalRows : " + totalRows);
				if (!filtersMap.containsKey("limit") && totalRows > EXPORT_ROW_LIMIT) {
					for (int offset = EXPORT_ROW_LIMIT; offset <= totalRows;) {
						systemRequestParamsDTO.getPagination().setOffset(Integer.valueOf("" + offset));
						//JSONArray array = generateQuery(serializer.deepSerialize(systemRequestParamsDTO), dataMap, userMap, errorMap);
						List<Map<String, Object>> resultList = esService.generateQuery(systemRequestParamsDTO, indices, checkPoint, dataMap, errorMap);						
						JSONArray array = businessLogicService.buildAggregateJSON(resultList);
						
						generateReportFile(reportType, array, dataMap, errorMap,fileName,false);
						offset += EXPORT_ROW_LIMIT;
						Thread.sleep(EXPORT_ROW_LIMIT);
						System.out.print("\nOffset: " + offset);
					}
				}
			
				if (totalRows > 0) {
					mailService.sendMail(emailId, "xAPI - Formatted report", "Please download the attachement ", resultFileName);
				}else{
					mailService.sendMail(emailId, "xAPI - Formatted report", "Oops!,We don't see any records for you request.");
				}
				} catch (Exception e) {
					errorMap.put(500, "At this time, we are unable to process your request. Please try again by changing your request or contact developer");
				}			
		}
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

		serializer.transform(new ExcludeNullTransformer(), void.class).exclude("*.class");
		
		String datas = serializer.deepSerialize(systemRequestParamsDTO);
		
		System.out.print("\n newObject : " + datas);

		
		if(columns.getStringValue("query", null) != null){			
			return generateQuery(datas, dataMap, userMap, errorMap);
		}
		
		return new JSONArray();
	}
	public String generateReportFile(String reportType, JSONArray activityArray, Map<String, Object> dataMap, Map<Integer, String> errorData,String fileName,boolean isNewFile) {
		try {
			List<Map<String, Object>> activityList = new ArrayList<Map<String, Object>>();
			// ReportData is generated here
			if(reportType.equalsIgnoreCase("xapi")) {
				getReportDataList(activityArray, activityList, errorData);
				fileName = csvBuilderService.generateCSVMapReport(activityList, fileName,isNewFile);
			} else if(reportType.equalsIgnoreCase("xapi-edx-hybrid")) {
				getXAPIEdxHybridDataList(activityArray, activityList, errorData);
				fileName = csvBuilderService.generateCSVReportPipeSeperatedValues(activityList, fileName, isNewFile);
			}
			return fileName;
		} catch (Exception e) {
			errorData.put(500, "At this time, we are unable to process your request. Please try again by changing your request or contact developer");
			return null;
		}
	}
	
	public void getReportDataList(JSONArray activityArray, List<Map<String, Object>> activityList, Map<Integer, String> errorAsMap) throws JSONException, Exception {

		if (activityArray.length() > 0) {
			/*System.out.println("activityArray Length :" +activityArray.length());
			int skippedCount = 0;*/
			for (int index = 0; index < activityArray.length(); index++) {
				JSONObject activityJsonObject = activityArray.getJSONObject(index);
				if (!activityJsonObject.isNull("eventId") && StringUtils.isNotBlank(activityJsonObject.get("eventId").toString())) {
					Map<String, Object> activityAsMap = new HashMap<String, Object>();
					if (activityJsonObject.get("eventName").toString().matches(XAPI_SUPPORTED_EVENTS)) {
						/* Unique Activity Id */
						activityAsMap.put("id", activityJsonObject.get("eventId"));

						/* Actor Property starts here */
						Map<String, Object> actorAsMap = new HashMap<String, Object>(1);
						businessLogicService.generateActorProperty(activityJsonObject, actorAsMap, errorAsMap);
						if (!actorAsMap.isEmpty()) {
							activityAsMap.put("actor", actorAsMap);
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

						if (!objectAsMap.isEmpty() && !actorAsMap.isEmpty() && !verbAsMap.isEmpty() && !activityAsMap.isEmpty()) {
							activityList.add(activityAsMap);
						}/* else {
							skippedCount++;
							System.out.println("Skipped eventId >> "+activityJsonObject.get("eventId").toString() + "eventName :"+activityJsonObject.get("eventName").toString());
						}*/
					}/* else {
						skippedCount++;
						System.out.println("Skipped eventId >> "+activityJsonObject.get("eventId").toString() + "eventName :"+activityJsonObject.get("eventName").toString());
					}*/
				}
			}
			/*if(skippedCount > 0) {
				System.out.println("skippedCount >> "+skippedCount);
			}*/
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
		
		Map<String, Boolean> checkPoint = baseAPIService.validateData(requestParamsDTO);
		/*Map<String, Boolean> checkPoint = new HashMap<String, Boolean>();
		
		if (!baseAPIService.checkPoint(requestParamsDTO, checkPoint, errorData)) {
			return new JSONArray();
		}*/

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
	
	public List<Map<String, Object>> getXAPIEdxHybridDataList(JSONArray activityArray, List<Map<String, Object>> activityList, Map<Integer, String> errorAsMap) throws JSONException, Exception {

		if (activityArray.length() > 0) {
			/*System.out.println("activityArray Length :" +activityArray.length());
			int skippedCount = 0;*/
			String currentEventTime = null;
			String nextEventTime = null;
			String currentSessionToken = null;
			String nextSessionToken = null;
			Long secsToNext = 0L;
			for (int index = 0; index < activityArray.length(); index++) {
				JSONObject activityJsonObject = activityArray.getJSONObject(index);
				if (!activityJsonObject.isNull("eventId") && StringUtils.isNotBlank(activityJsonObject.get("eventId").toString())) {
					try{
						Map<String, Object> activityAsMap = new HashMap<String, Object>();
						if (activityJsonObject.get("eventName").toString().matches(XAPI_SUPPORTED_EVENTS)) {
							System.out.println("Processing Activity..");
							/* Unique Activity Id */
							activityAsMap.put("id", activityJsonObject.get("eventId"));
							
							/* Time of Activity */
							if (!activityJsonObject.isNull("eventTime") && StringUtils.isNotBlank(activityJsonObject.get("eventTime").toString())) {
								currentEventTime = activityJsonObject.get("eventTime").toString();
							} else if (!activityJsonObject.isNull("startTime") && StringUtils.isNotBlank(activityJsonObject.get("startTime").toString())) {
								currentEventTime = activityJsonObject.get("startTime").toString();
							}
							activityAsMap.put("time", currentEventTime);
							
							/* secsToNext Activity */
							if (!activityJsonObject.isNull("sessionToken") && StringUtils.isNotBlank(activityJsonObject.get("sessionToken").toString())) {
								currentSessionToken = activityJsonObject.get("sessionToken").toString();
							}
							if((index+1) < activityArray.length()) {
								if((!activityArray.getJSONObject(index+1).isNull("eventTime") && StringUtils.isNotBlank(activityJsonObject.get("eventTime").toString()))
										|| (StringUtils.isNotBlank(activityJsonObject.get("startTime").toString()) && StringUtils.isNotBlank(activityJsonObject.get("startTime").toString()))) {
									if (!activityJsonObject.isNull("eventTime") && StringUtils.isNotBlank(activityJsonObject.get("eventTime").toString())) {
										nextEventTime = activityArray.getJSONObject(index + 1).get("startTime").toString();
									} else if (!activityJsonObject.isNull("startTime") && StringUtils.isNotBlank(activityJsonObject.get("startTime").toString())) {
										nextEventTime = activityArray.getJSONObject(index + 1).get("startTime").toString();
									}
								}
								//System.out.println(activityArray.getJSONObject(index+1).get("eventTime"));
								if (!activityArray.getJSONObject(index+1).isNull("sessionToken") && StringUtils.isNotBlank(activityArray.getJSONObject(index+1).get("sessionToken").toString())) {
									nextSessionToken = activityArray.getJSONObject(index+1).get("sessionToken").toString();
								}
							}
							
							if (currentEventTime != null && nextEventTime != null && nextSessionToken != null && currentSessionToken != null && currentSessionToken.equalsIgnoreCase(nextSessionToken)) {
								SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
								try {
									secsToNext = (formatter.parse(nextEventTime).getTime() - formatter.parse(currentEventTime).getTime()) / 1000;
								} catch (ParseException e) {
									e.printStackTrace();
								}
							}
							activityAsMap.put("secs_to_next", secsToNext.longValue());

							/* Actor property */
							String mailId = null;
							if (!activityJsonObject.isNull("emailId") && StringUtils.isNotBlank(activityJsonObject.get("emailId").toString())) {
								mailId = activityJsonObject.get("emailId").toString();
							} else if (!activityJsonObject.isNull("gooruUId") && StringUtils.isNotBlank(activityJsonObject.get("gooruUId").toString())
									&& activityJsonObject.get("gooruUId").toString().equalsIgnoreCase("ANONYMOUS")) {
								mailId = "Anonymous@goorulearning.org";
							} else {
								mailId = UUID.randomUUID() + "@goorulearning.org";
							}
							activityAsMap.put("actor", mailId);
							
							String eventType = null;
							/* Verb property */
							Map<String, Object> verbAsMap = new HashMap<String, Object>();
							String verb = null;
							businessLogicService.generateVerbProperty(activityJsonObject, verbAsMap, errorAsMap);
							if(!verbAsMap.isEmpty()) {
								verb = verbAsMap.get("id").toString().substring(42);
								activityAsMap.put("verb", verb);
								if(verb.equalsIgnoreCase("experienced")) {
									eventType = "play";
								}
							}
							
							/* object_name property */
							String id = null;
							if ((!activityJsonObject.isNull("gooruOid") && StringUtils.isNotBlank(activityJsonObject.get("gooruOid").toString()))
									|| (!activityJsonObject.isNull("gooru_oid") && StringUtils.isNotBlank(activityJsonObject.get("gooru_oid").toString()))) {
								if (!activityJsonObject.isNull("gooruOid") && StringUtils.isNotBlank(activityJsonObject.get("gooruOid").toString())) {
									id = activityJsonObject.get("gooruOid").toString();
								} else if (!activityJsonObject.isNull("gooru_oid") && StringUtils.isNotBlank(activityJsonObject.get("gooru_oid").toString())) {
									id = activityJsonObject.get("gooru_oid").toString();
								}
							}
							activityAsMap.put("object_name", id);
							
							/* object_type property */
							String typeName = null;
							String type = null;
							
							if (!activityJsonObject.isNull("typeName") && StringUtils.isNotBlank(activityJsonObject.get("typeName").toString())) {
								typeName = activityJsonObject.get("typeName").toString();
								if (typeName.matches(RESOURCE_TYPES) || typeName.matches(QUESTION_TYPES)) {
									type = "resource";
								} else if (typeName.matches(COLLECTION_TYPES)) {
									type = "collection";
								}
							} else {
								typeName = "NA";
							}
							activityAsMap.put("object_type", typeName);

							/* agent property */
							String agent = null;
							if (!activityJsonObject.isNull("userAgent") && StringUtils.isNotBlank(activityJsonObject.get("userAgent").toString())) {
								agent = activityJsonObject.get("userAgent").toString();
							} else if (!activityJsonObject.isNull("user_agent") && StringUtils.isNotBlank(activityJsonObject.get("user_agent").toString())){
								agent = activityJsonObject.get("user_agent").toString();
							} else {
								agent = "NA";
							}
							activityAsMap.put("agent", agent);

							// activityAsMap.put("event_type", activityJsonObject.get("TYPE"));
							// "http://qa.goorulearning.org/#students-view&pageSize=5&id=cfed4718-ee28-44ad-82be-206dec5c9c8f&pageNum=0&pos=1"
							// "http://qa.goorulearning.org/#teach&pageSize=5&classpageid=e7249ce2-b7c8-4e1d-b31f-c54f0cff9765&pageNum=0&pos=1";
							
							/* page property */
							if (type != null) {
								activityAsMap.put("page", "http://www.goorulearning.org/#" + type + "-play&id=" + id + "&pn=" + type);
							} else {
								activityAsMap.put("page", "NA");
							}
							
							/* ip property */
							String userIp = null;
							String stateCode = null;
							String countryCode = null;
							String hostName = null;
							if (!activityJsonObject.isNull("userIp") && StringUtils.isNotBlank(activityJsonObject.get("userIp").toString())) {
								userIp = activityJsonObject.get("userIp").toString();
							} else if (!activityJsonObject.isNull("user_ip") && StringUtils.isNotBlank(activityJsonObject.get("user_ip").toString())) {
								userIp = activityJsonObject.get("user_ip").toString();
							}
							if (userIp != null && !userIp.equalsIgnoreCase("127.0.0.1")) {
								try {
									/*ServerLocation location = geoLocationService.getLocation(userIp);
									stateCode = location.getRegion().split("[\\(\\)]")[0];
									countryCode = location.getCountryCode().split("[\\(\\)]")[0];*/
									InetAddress inetAddress = InetAddress.getByName(userIp);
									hostName = inetAddress.getCanonicalHostName();
									
									//activityAsMap.put("userIp", userIp);
								} catch (Exception e) {
									e.printStackTrace();
									hostName = "UNRES";
									/*stateCode = "UNRES";
									countryCode = "UNRES";*/
								}
								
							} else {
								hostName = "UNRES";
								/*stateCode = "UNRES";
								countryCode = "UNRES";*/
							}
							activityAsMap.put("ip", hostName != null && !hostName.equalsIgnoreCase(userIp) ? hostName : "UNRES");
							//activityAsMap.put("state_code", stateCode!= null ? stateCode : "NA");
							//activityAsMap.put("country_code", countryCode != null ? countryCode : "NA");
							
							/* result, meta & event property */
							String eventName = activityJsonObject.get("eventName").toString();
							Map<String, Object> metaAsMap = new HashMap<String, Object>(3);
							Map<String, Object> correctMap = new HashMap<String, Object>(4);
							Map<String, Object> correctMapObject = new HashMap<String, Object>(1);
							Map<String, Object> eventAsMap = new HashMap<String, Object>();
							if ((!activityJsonObject.isNull("score") && StringUtils.isNotBlank(activityJsonObject.get("score").toString()))
									|| (!activityJsonObject.isNull("newScore") && StringUtils.isNotBlank(activityJsonObject.get("newScore").toString()))
									&& activityJsonObject.get("eventName").toString().endsWith("play")) {
								String resultString = null;
								String hint = null;
								String hintMode = null;
								int attemptCount = 0;
								if (!activityJsonObject.isNull("resourceTypeId")
										&& StringUtils.isNotBlank(activityJsonObject.get("resourceTypeId").toString())
										&& (Integer.valueOf(activityJsonObject.get("resourceTypeId").toString()) == 1002 || Integer.valueOf(activityJsonObject.get("resourceTypeId").toString()) == 1020)) {
									Map<String, Object> rawScoreAsMap = new HashMap<String, Object>(3);
									Integer score = 0;
									score = Integer.valueOf(activityJsonObject.get("score").toString());

									if ((eventName.toString().equalsIgnoreCase("resource.play") || eventName.toString().equalsIgnoreCase("collection.resource.play"))) {
										eventType = "problem_check";
										if (!activityJsonObject.isNull("attemptCount") && StringUtils.isNotBlank(activityJsonObject.get("attemptCount").toString())) {
											int[] attemptStatus = TypeConverter.stringToIntArray(activityJsonObject.get("attemptStatus").toString());
											attemptCount = Integer.valueOf(activityJsonObject.get("attemptCount").toString());
											if (attemptStatus.length > 1) {
												int recentAttempt = attemptCount;
												if (recentAttempt != 0) {
													recentAttempt = recentAttempt - 1;
												}
												score = attemptStatus[recentAttempt];
											}
										} else if (score >= 1) {
											score = 1;
										}
										resultString = score > 0 ? "correct" : "incorrect";
										
										if (!activityJsonObject.isNull("hints") && StringUtils.isNotBlank(activityJsonObject.get("hints").toString()) && !activityJsonObject.get("hints").toString().equalsIgnoreCase("{}")) {
											hint = activityJsonObject.get("hints").toString();
											hintMode = "on_request";
										} else {
											hint = null;
											hintMode = "None";
										}
										
										correctMap.put("correctness", resultString);
										correctMap.put("hint", hint);
										correctMap.put("hint_mode", hintMode);
										
										if (!correctMap.isEmpty()) {
											correctMapObject.put(id, correctMap);
										}
										eventAsMap.put("grade", score);
										eventAsMap.put("max_grade", 1);
										eventAsMap.put("done", "true");
										eventAsMap.put("problem_id", id);
										eventAsMap.put("attempts", attemptCount);
										eventAsMap.put("success", resultString);
										activityAsMap.put("result", resultString);
										rawScoreAsMap.put("min", 0);
										rawScoreAsMap.put("max", 1);
									}
									
									if ((!activityJsonObject.isNull("questionCount") && StringUtils.isNotBlank(activityJsonObject.get("questionCount").toString()))
											&& Integer.valueOf(activityJsonObject.get("questionCount").toString()) != 0 && eventName.toString().equalsIgnoreCase("collection.play")) {
										Integer questionCount = Integer.valueOf(activityJsonObject.get("questionCount").toString()) > 0 ? Integer.valueOf(activityJsonObject.get("questionCount")
												.toString()) : 0;
										if (questionCount >= score) {
											rawScoreAsMap.put("min", 0);
											rawScoreAsMap.put("max", questionCount);
										}
									}
									if (eventName.toString().equalsIgnoreCase("collection.play")
											&& (!activityJsonObject.isNull("newScore") && StringUtils.isNotBlank(activityJsonObject.get("newScore").toString()))) {
										score = Integer.valueOf(activityJsonObject.get("newScore").toString());
									}
									rawScoreAsMap.put("raw", score);
									metaAsMap.put("score", rawScoreAsMap);
									if (eventAsMap.isEmpty()) {
										eventAsMap.put("id", id);
									}
									if (eventName.toString().endsWith("play")) {
										if (!activityJsonObject.isNull("type") && StringUtils.isNotBlank(activityJsonObject.get("type").toString())
												&& activityJsonObject.get("type").toString().equalsIgnoreCase("stop")) {
											metaAsMap.put("completion", Boolean.valueOf("true"));
											eventAsMap.put("done", "true");
										} else {
											metaAsMap.put("completion", Boolean.valueOf("false"));
											eventAsMap.put("done", "false");
										}
									}

								}
							}
							if (!activityJsonObject.isNull("parentGooruId") && StringUtils.isNotBlank(activityJsonObject.get("parentGooruId").toString())) {
								metaAsMap.put("parent", activityJsonObject.get("parentGooruId").toString());
							}
							if (!activityJsonObject.isNull("sessionToken") && StringUtils.isNotBlank(activityJsonObject.get("sessionToken").toString())) {
								metaAsMap.put("session_token", activityJsonObject.get("sessionToken").toString());
							}
							if(!activityAsMap.containsKey("result")) {
								activityAsMap.put("result", "");
							}
							activityAsMap.put("event_type", eventType != null ? eventType : "");

							if (!metaAsMap.isEmpty()) {
								ObjectMapper objectMapper = new ObjectMapper();
								activityAsMap.put("meta", objectMapper.writeValueAsString(metaAsMap));
							} else {
								activityAsMap.put("meta", "NA");
							}

							if (eventAsMap.isEmpty()) {
								eventAsMap.put("id", id);
							}
							if (!correctMapObject.isEmpty()) {
								eventAsMap.put("correct_map", correctMapObject);
							}
							if(!eventAsMap.isEmpty()) {
								ObjectMapper objectMapper = new ObjectMapper();
								activityAsMap.put("event", objectMapper.writeValueAsString(eventAsMap));
							} else {
								activityAsMap.put("event", "NA");
							}
							
							if (!activityAsMap.isEmpty() && !verbAsMap.isEmpty()) {
								activityList.add(activityAsMap);
							}
							
						}
					
					} catch(Exception e) {
						e.printStackTrace();
					}
				}
			}
			//System.out.println("activityList >>" +activityList);
		}
		return activityList;
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

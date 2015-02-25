package org.gooru.insights.services;

import java.util.ArrayList;
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
import org.joda.time.Period;
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
	
	private static final String RESOURCE_TYPES = ResourceType.PRESENTATION.getType()+"|"+ResourceType.AUDIO.getType()+"|"+ResourceType.IMAGE.getType()+"|"+ResourceType.VIDEO.getType()+"|"+ResourceType.RESOURCE.getType()+"|"+ResourceType.ANIMATION_KMZ.getType()+"|"+ResourceType.ANIMATION_SWF.getType()+"|"+ResourceType.TEXTBOOK.getType()+"|"+ResourceType.VIMEO_VIDEO.getType()+"|"+ResourceType.HANDOUTS.getType()+"|"+ResourceType.EXAM.getType();
	
	private static final String QUESTION_TYPES = ResourceType.ASSESSMENT_QUESTION.getType()+"|"+ResourceType.QB_QUESTION.getType()+"|"+ResourceType.QUESTION.getType();
	
	private static final String COLLECTION_TYPES = ResourceType.SCOLLECTION.getType()+"|"+ResourceType.CLASSPAGE.getType()+"|"+ResourceType.CLASSPLAN.getType()+"|"+ResourceType.STUDYSHELF.getType()+"|"+ResourceType.CLASSBOOK.getType();
	
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

	public List<Map<String, Object>> generateReportFile(JSONArray activityArray) {

		try {
			List<Map<String, Object>> activityList = new ArrayList<Map<String, Object>>();

			for (int index = 0; index < activityArray.length(); index++) {
				Map<String, Object> activityAsMap = new HashMap<String, Object>();
				JSONObject activityJsonObject = activityArray.getJSONObject(index);
				
				/* Unique Activity Id*/
				activityAsMap.put("id", activityJsonObject.get("eventId"));

				/* Actor Property starts here*/
				if ((!activityJsonObject.isNull("gooruUId") && StringUtils.isNotBlank(activityJsonObject.get("gooruUId").toString()))) {
					Map<String, Object> actorAsMap = new HashMap<String, Object>(1);
					actorAsMap.put("objectType", "Agent");
					if(!activityJsonObject.isNull("emailId") && StringUtils.isNotBlank(activityJsonObject.get("emailId").toString())) {
						actorAsMap.put("mbox", "mailto:"+activityJsonObject.get("emailId"));
					} else {
						actorAsMap.put("id", activityJsonObject.get("gooruUId"));
					}
					actorAsMap.put("apiKey", activityJsonObject.get("apiKey"));
					actorAsMap.put("organizationUid", activityJsonObject.get("userOrganizationUId"));
					if (!activityJsonObject.isNull("userIp") && StringUtils.isNotBlank(activityJsonObject.get("userIp").toString())) {
						actorAsMap.put("userIp", activityJsonObject.get("userIp"));
						actorAsMap.put("userAgent", activityJsonObject.get("userAgent"));
					}
					activityAsMap.put("actor", actorAsMap);
				}
				
				/* Object Property starts here*/
				if (!activityJsonObject.isNull("gooruOid") && StringUtils.isNotBlank(activityJsonObject.get("gooruOid").toString())) {
					Map<String, Object> objectAsMap = new HashMap<String, Object>(1);
					String objectType = null;
					
					// TODO Add condition to differentiate Agent/ Statement/ Activity/ StatementRef
					objectType = "Activity";
					objectAsMap.put("objectType", objectType);
					objectAsMap.put("id", activityJsonObject.get("gooruOid"));
					if (!activityJsonObject.isNull("typeName") && StringUtils.isNotBlank(activityJsonObject.get("typeName").toString())) {
						Map<String, Object> definitionAsMap = new HashMap<String, Object>(4);
						String typeName = activityJsonObject.get("typeName").toString();
						if (typeName.matches(RESOURCE_TYPES)) {
							typeName = "resource";
						} else if (typeName.matches(COLLECTION_TYPES)) {
							typeName = "collection";
						} else if (typeName.matches(QUESTION_TYPES)) {
							typeName = "question";
						}
						if (StringUtils.isNotBlank(typeName)) {
							definitionAsMap.put("type", typeName);
							objectAsMap.put("definition", definitionAsMap);
						}
					}
					activityAsMap.put("object", objectAsMap);

				}
				/* Verb Property starts here*/
				if (!activityJsonObject.isNull("eventName") && StringUtils.isNotBlank(activityJsonObject.get("eventName").toString())) {
					Map<String, Object> verbAsMap = new HashMap<String, Object>();
					String verb = null;
					verb = getVerb(activityJsonObject);
					if (StringUtils.isNotBlank(verb)) {
						verbAsMap.put("id", "www.goorulearning.org/exapi/verbs/" + verb);
						Map<String, Object> displayAsMap = new HashMap<String, Object>();
						displayAsMap.put("en-US", verb);
						verbAsMap.put("display", displayAsMap);
						activityAsMap.put("verb", verbAsMap);
					}
				}
				/* Context Property starts here*/
				Map<String, Object> contextActivitiesMap = new HashMap<String, Object>();
				Map<String, Object> contextAsMap = new HashMap<String, Object>();
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
						contextActivitiesMap.put("contextActivities", contextAsMap);
					}
				}
				if(!contextActivitiesMap.isEmpty()) {
					activityAsMap.put("context", contextActivitiesMap);
				}
				
				/* Result Property starts here*/
				Map<String, Object> resultMap = new HashMap<String, Object>();
				if((activityJsonObject.get("eventName").toString().equalsIgnoreCase("item.review") || activityJsonObject.get("eventName").toString().equalsIgnoreCase("comment.create")) && (!activityJsonObject.isNull("text") && StringUtils.isNotBlank(activityJsonObject.get("text").toString()))){
					resultMap.put("response", activityJsonObject.get("text"));
				}
				if (activityJsonObject.get("eventName").toString().equalsIgnoreCase("item.rate") && (!activityJsonObject.isNull("rate") && StringUtils.isNotBlank(activityJsonObject.get("rate").toString()))) {
					Map<String, Object> responseAsMap = new HashMap<String, Object>(1);
					responseAsMap.put("response", activityJsonObject.get("rate"));
					resultMap.put("response", responseAsMap);
				}
				if (!activityJsonObject.isNull("totalTimeSpentInMs") && StringUtils.isNotBlank(activityJsonObject.get("totalTimeSpentInMs").toString())) {
					if(Long.valueOf(activityJsonObject.get("totalTimeSpentInMs").toString()) < 1800000) {
						resultMap.put("duration", new Period((long)Long.valueOf(activityJsonObject.get("totalTimeSpentInMs").toString())));
					} else {
						resultMap.put("duration", new Period((long)1800000));
					}
					if (!activityJsonObject.isNull("score") && StringUtils.isNotBlank(activityJsonObject.get("score").toString())) {
						Map<String, Object> rawScoreAsMap = new HashMap<String, Object>(1);
						rawScoreAsMap.put("raw", Long.valueOf(activityJsonObject.get("score").toString()));
						resultMap.put("score", rawScoreAsMap);
					}
				}
				if(!resultMap.isEmpty()) {
					activityAsMap.put("result", resultMap);
				}

				activityAsMap.put("timestamp", activityJsonObject.get("eventTime"));
				activityAsMap.put("stored", activityJsonObject.get("eventTime"));

				/*
				 * Map<String, Object> responseAsMap = (Map<String, Object>) mapper.readValue(activityArray.toString, new TypeReference<Map<String,
				 * Object>>() { });
				 */
				activityList.add(activityAsMap);
			}

			Map<String, Object> files = new HashMap<String, Object>();
			String fileName = null;
			fileName = csvBuilderService.generateCSVMapReport(activityList, fileName);

			files.put("file", fileName);
			List<Map<String, Object>> classGrade = new ArrayList<Map<String, Object>>();

			classGrade.add(files);

			return classGrade;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public String getVerb(JSONObject activityJsonObject) {
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
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return verb;
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

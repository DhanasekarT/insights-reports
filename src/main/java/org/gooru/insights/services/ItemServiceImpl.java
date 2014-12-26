package org.gooru.insights.services;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

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
				System.out.println("combined " + resultData);
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
		List<Map<String, Object>> resultList = esService.itemSearch(requestParamsDTO, indices, validatedData, dataMap, errorMap);
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
		
		ColumnList<String> columns = baseCassandraService.read(keyspaces.INSIGHTS.keyspace(), columnFamilies.QUERY_REPORTS.columnFamily(), reportType);
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
		
		JSONSerializer serializer = new JSONSerializer();
		serializer.transform(new ExcludeNullTransformer(), void.class).exclude("*.class");
		String datas = serializer.deepSerialize(systemRequestParamsDTO);
		System.out.print("\n newObject : " + datas);

		
		if(columns.getStringValue("query", null) != null){			
			return getEventDetail(datas, dataMap, userMap, errorMap);
		}
		
		return new JSONArray();
	}
	
	public JSONArray getEventDetail(String data, Map<String, Object> dataMap, Map<String, Object> userMap, Map<Integer, String> errorMap) {
		RequestParamsDTO requestParamsDTO = null;

		try {
			requestParamsDTO = baseAPIService.buildRequestParameters(data);
		} catch (Exception e) {
			e.printStackTrace();
			errorMap.put(400, E1014);
			return new JSONArray();
		}
		Map<String, Boolean> validatedData = baseAPIService.validateData(requestParamsDTO);

		if (!validatedData.get(hasdata.HAS_DATASOURCE.check())) {
			errorMap.put(400,E1016);
			return new JSONArray();
		}

		requestParamsDTO = baseAPIService.validateUserRole(requestParamsDTO, userMap, errorMap);

		if (!errorMap.isEmpty()) {
			return new JSONArray();
		}


		String[] indices = baseAPIService.getIndices(requestParamsDTO.getDataSource().toLowerCase());
		List<Map<String, Object>> resultList = esService.itemSearch(requestParamsDTO, indices, validatedData, dataMap, errorMap);

		try {
			JSONArray jsonArray = businessLogicService.buildAggregateJSON(resultList);

			if (requestParamsDTO.isSaveQuery() != null) {
				if (requestParamsDTO.isSaveQuery()) {
					JSONObject jsonObject = new JSONObject();
					jsonObject.put("data",jsonArray.toString());
					jsonObject.put("message",dataMap);
					String queryId = baseAPIService.putRedisCache(data,userMap, jsonObject);
					dataMap.put("queryId", queryId);
				}
			}
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

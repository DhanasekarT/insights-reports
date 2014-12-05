package org.gooru.insights.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.APIConstants.hasdata;
import org.gooru.insights.constants.ESConstants.esIndices;
import org.gooru.insights.constants.ESConstants.esSources;
import org.gooru.insights.constants.ESConstants.esTypes;
import org.gooru.insights.models.RequestParamsCoreDTO;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsSortDTO;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ItemServiceImpl implements ItemService,APIConstants {

	@Autowired
	BaseAPIService baseAPIService;
	
	@Autowired
	BaseESService esService;
	
	@Autowired
	BusinessLogicService businessLogicService;
	
	@Autowired
	BaseConnectionService baseConnectionService;
	
	public JSONArray processApi(String data,Map<String,Object> dataMap,Map<Integer,String> errorMap){
	
		List<Map<String,Object>> resultData = new ArrayList<Map<String,Object>>();
		try{
		RequestParamsCoreDTO requestParamsCoreDTO  = baseAPIService.buildRequestParamsCoreDTO(data);
		
		if(baseAPIService.checkNull(requestParamsCoreDTO.getRequestParamsDTO())){
		List<RequestParamsDTO> requestParamsDTOs = requestParamsCoreDTO.getRequestParamsDTO();
		
		String previousAPIKey =null;
		for(RequestParamsDTO api : requestParamsDTOs){
			if(!baseAPIService.checkNull(api)){
				continue;
			}
			List<Map<String,Object>> tempData = new ArrayList<Map<String,Object>>();
			tempData = getData(api, dataMap, errorMap);
			if(baseAPIService.checkNull(previousAPIKey)){
			resultData = businessLogicService.leftJoin(resultData, tempData, previousAPIKey, api.getApiJoinKey());
			}
		}
		System.out.println("combined "+ resultData);
		if(baseAPIService.checkNull(requestParamsCoreDTO.getCoreKey())){
			resultData = businessLogicService.formatAggregateKeyValueJson(resultData, requestParamsCoreDTO.getCoreKey());
		}
		
		}else{
			return new JSONArray();	
		}
		
		return businessLogicService.buildAggregateJSON(resultData);
		}catch(Exception e){
			e.printStackTrace();
			errorMap.put(500, "Invalid JSON format");
			return new JSONArray();
		}
		
	}
	
	public List<Map<String,Object>> getData(RequestParamsDTO requestParamsDTO,Map<String,Object> dataMap,Map<Integer,String> errorMap){

		Map<String,Boolean> validatedData = baseAPIService.validateData(requestParamsDTO);

		if(!validatedData.get(hasdata.HAS_DATASOURCE.check())){
			errorMap.put(400, "should provide the data source to be fetched");
			return new ArrayList<Map<String,Object>>();
		}

		String[] indices = baseAPIService.getIndices(requestParamsDTO.getDataSource().toLowerCase());
		List<Map<String, Object>> resultList = esService.itemSearch(requestParamsDTO,indices,validatedData,dataMap,errorMap);
		return resultList;
	}
	
	public JSONArray getEventDetail(String data,Map<String,Object> dataMap,Map<String,Object> userMap, Map<Integer,String> errorMap){
		RequestParamsDTO requestParamsDTO = null;
		
		try{
		requestParamsDTO = baseAPIService.buildRequestParameters(data);
		}catch(Exception e){
			e.printStackTrace();
			errorMap.put(500, "Invalid JSON format");
			return new JSONArray();
		}
		
		Map<String,Boolean> validatedData = baseAPIService.validateData(requestParamsDTO);

		
		if(!validatedData.get(hasdata.HAS_DATASOURCE.check())){
			errorMap.put(400, "should provide the data source to be fetched");
			return new JSONArray();
		}

		requestParamsDTO = baseAPIService.validateUserRole(requestParamsDTO, userMap, errorMap);
		
		if(!errorMap.isEmpty()){
			return new JSONArray();
	    }
		
		
		String[] indices = baseAPIService.getIndices(requestParamsDTO.getDataSource().toLowerCase());
		List<Map<String,Object>> resultList =  esService.itemSearch(requestParamsDTO,indices,validatedData,dataMap,errorMap);
		
		try {
			JSONArray jsonArray = businessLogicService.buildAggregateJSON(resultList);
			
			if(requestParamsDTO.isSaveQuery()){
				String queryId = baseAPIService.putRedisCache(data, jsonArray);
				dataMap.put("queryId", queryId);
			}
			return jsonArray;
		} catch (JSONException e) {
			e.printStackTrace();
			return new JSONArray();
		}
		
	}

	public Boolean clearQuery(String id){
		return baseAPIService.clearQuery(id);
	}
	public boolean clearDataCache(){
		return baseConnectionService.clearDataCache();
	}
	
	public void clearConnectionCache(){
		baseConnectionService.clearConnectionCache();
	}
	
	public  Map<String,Object> getUserObject(String sessionToken ,Map<Integer,String> errorMap){	
		return baseConnectionService.getUserObject(sessionToken, errorMap);
	}
	
}

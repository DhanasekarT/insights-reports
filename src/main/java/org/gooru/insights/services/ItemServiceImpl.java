package org.gooru.insights.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.gooru.insights.constants.ESConstants.esIndices;
import org.gooru.insights.constants.ESConstants.esTypes;
import org.gooru.insights.models.RequestParamsDTO;
import org.gooru.insights.models.RequestParamsSortDTO;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ItemServiceImpl implements ItemService {

	@Autowired
	BaseAPIService baseAPIService;
	
	@Autowired
	BaseESService esService;
	
	Map<String,String> indexMap = new HashMap<String,String>();

	ItemServiceImpl(){
	indexMap.put("rawdata", "event_logger_insights");
	}
	
	public String getEventDetail(String data,Map<String,String> dataRecord,Map<Integer,String> errorMap){
		RequestParamsDTO requestParamsDTO = null;
		Map<String,String> sort = new HashMap<String, String>();
		FilterBuilder filterBuilder = null;
		Integer offset =0;
		Integer limit =10;
		String dataKey="_source";
		try{
			System.out.println("input json"+data);
		requestParamsDTO = baseAPIService.buildRequestParameters(data);
		}catch(Exception e){
			errorMap.put(500, "Not a Valid JSON ");
			e.printStackTrace();
			System.out.println("error");
//			return new JSONArray();
		}
		if(!baseAPIService.checkNull(requestParamsDTO.getDataSource())){
			errorMap.put(400, "should provide the data source to be fetched");
//			return new JSONArray();
		}
		if(baseAPIService.checkNull(requestParamsDTO.getFields())){
			dataKey="fields";
		}
		if(requestParamsDTO.getFilters() != null){
        filterBuilder = FilterBuilders.termsFilter("eventName", requestParamsDTO.getFilters().getEventName());
		}
		System.out.println("validate");
		if(requestParamsDTO.getPagination() != null){
			if(baseAPIService.checkNull(requestParamsDTO.getPagination().getOffset())){
			offset = requestParamsDTO.getPagination().getOffset();
			}
			if(baseAPIService.checkNull(requestParamsDTO.getPagination().getLimit())){
			limit = requestParamsDTO.getPagination().getLimit();
			}
			if(baseAPIService.checkNull(requestParamsDTO.getPagination().getOrder())){
			List<RequestParamsSortDTO> requestParamsSortDTO = requestParamsDTO.getPagination().getOrder();
			for(RequestParamsSortDTO sortData : requestParamsSortDTO){
				if(baseAPIService.checkNull(sortData.getSortBy())){
					sort.put(sortData.getSortBy(), baseAPIService.checkNull(sortData.getSortOrder()) ? checkSort(sortData.getSortOrder()) : "ASC");
				}
			}
			}
			
		}
//		return getRecords(esService.searchData(requestParamsDTO,baseAPIService.convertStringtoArray(indexMap.get(requestParamsDTO.getDataSource().toLowerCase())),baseAPIService.convertStringtoArray(esTypes.EVENT_DETAIL.esType()),requestParamsDTO.getFields(), null, filterBuilder,offset,limit,sort),dataRecord,errorMap,dataKey);
		return esService.searchData(requestParamsDTO,baseAPIService.convertStringtoArray(indexMap.get(requestParamsDTO.getDataSource().toLowerCase())),baseAPIService.convertStringtoArray(esTypes.EVENT_DETAIL.esType()),requestParamsDTO.getFields(), null, filterBuilder,offset,limit,sort);
		
	}

	public JSONArray getRecords(String data,Map<String,String> dataRecord,Map<Integer,String> errorRecord,String dataKey){
		JSONObject json;
		JSONArray jsonArray = new JSONArray();
		JSONArray resultJsonArray = new JSONArray();
		try {
			json = new JSONObject(data);
			json = new JSONObject(json.get("hits").toString());
			dataRecord.put("totalRows", json.get("total").toString());
			jsonArray = new JSONArray(json.get("hits").toString());
			if(!dataKey.equalsIgnoreCase("fields")){
			for(int i =0;i< jsonArray.length();i++){
				json = new JSONObject(jsonArray.get(i).toString());
				resultJsonArray.put(json.get(dataKey));
			}
			}else{
				for(int i =0;i< jsonArray.length();i++){
					JSONObject resultJson = new JSONObject();
				json = new JSONObject(jsonArray.get(i).toString());
				json = new JSONObject(json.get(dataKey).toString());
				 Iterator<String> keys =json.keys();
				 while(keys.hasNext()){
					 String key = keys.next();
						 resultJson.put(key,new JSONArray(json.get(key).toString()).get(0));
				 }
				 resultJsonArray.put(resultJson);
			}
			}
			return resultJsonArray;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return resultJsonArray;
	}
	
	public String checkSort(String sortOrder){
		if("ASC".equalsIgnoreCase(sortOrder)){
			return "ASC";
		}else if("DESC".equalsIgnoreCase(sortOrder)){
			return "DESC";
		}else{
			return "ASC";
		}
	}
	
	public String getClasspageCollectionDetail(String data) {
		return null;
	}
}

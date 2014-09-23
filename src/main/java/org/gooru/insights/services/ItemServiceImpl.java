package org.gooru.insights.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.ESConstants.esIndices;
import org.gooru.insights.constants.ESConstants.esSources;
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
public class ItemServiceImpl implements ItemService,APIConstants {

	@Autowired
	BaseAPIService baseAPIService;
	
	@Autowired
	BaseESService esService;
	
	@Autowired
	UpdatedService updatedService;
	
	Map<String,String> indexMap = new HashMap<String,String>();

	ItemServiceImpl(){
	indexMap.put("rawdata", "event_logger");
	indexMap.put("content", "content_catalog");
	}
	
	public JSONArray getEventDetail(String data,Map<String,String> dataRecord,Map<Integer,String> errorMap){
		RequestParamsDTO requestParamsDTO = null;
		Map<String,String> sort = new HashMap<String, String>();
		FilterBuilder filterBuilder = null;
		Integer offset =0;
		Integer limit =10;
		
		try{
		requestParamsDTO = baseAPIService.buildRequestParameters(data);
		}catch(Exception e){
			e.printStackTrace();
			errorMap.put(500, "Invalid JSON format");
			return new JSONArray();
		}
		Map<String,Boolean> validatedData = validateData(requestParamsDTO);
		if(!validatedData.get(hasdata.HAS_DATASOURCE.check())){
			errorMap.put(400, "should provide the data source to be fetched");
			return new JSONArray();
		}
			if(validatedData.get(hasdata.HAS_Offset.check())){
			offset = requestParamsDTO.getPagination().getOffset();
			}
			if(validatedData.get(hasdata.HAS_LIMIT.check())){
			limit = requestParamsDTO.getPagination().getLimit();
			}
			if(validatedData.get(hasdata.HAS_SORTBY.check())){
			List<RequestParamsSortDTO> requestParamsSortDTO = requestParamsDTO.getPagination().getOrder();
			for(RequestParamsSortDTO sortData : requestParamsSortDTO){
				if(validatedData.get(hasdata.HAS_SORTBY.check())){
					sort.put(sortData.getSortBy(), baseAPIService.checkNull(sortData.getSortOrder()) ? checkSort(sortData.getSortOrder()) : "ASC");
				}
			}
			}
			String[] indices = getIndices(requestParamsDTO.getDataSource().toLowerCase());
			
		return esService.searchData(requestParamsDTO,indices,baseAPIService.convertStringtoArray(esTypes.EVENT_DETAIL.esType()),requestParamsDTO.getFields(), null, filterBuilder,offset,limit,sort,validatedData,dataRecord,errorMap);
		
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
	
	public Map<String,Boolean> validateData(RequestParamsDTO requestParamsDTO){
		Map<String,Boolean> processedData = new HashMap<String,Boolean>();
		processedData.put("hasFields", false);
		processedData.put("hasDataSource",false);
		processedData.put("hasGroupBy",false);
		processedData.put("hasIntervals",false);
		processedData.put("hasFilter",false);
		processedData.put("hasAggregate",false);
		processedData.put("hasLimit",false);
		processedData.put("hasOffset",false);
		processedData.put("hasSortBy",false);
		processedData.put("hasSortOrder",false);
		processedData.put("hasGranularity",false);
		if(baseAPIService.checkNull(requestParamsDTO.getFields())){
			processedData.put("hasFields", true);
		}
		if(baseAPIService.checkNull(requestParamsDTO.getDataSource())){
			System.out.println("has dataSource"+requestParamsDTO.getDataSource());
			processedData.put("hasDataSource",true);
		}
		if(baseAPIService.checkNull(requestParamsDTO.getGroupBy())){
			processedData.put("hasGroupBy",true);
		}
		if(baseAPIService.checkNull(requestParamsDTO.getIntervals())){
			processedData.put("hasIntervals",true);
		}
		if(baseAPIService.checkNull(requestParamsDTO.getGranularity())){
			processedData.put("hasGranularity",true);
		}
		if(baseAPIService.checkNull(requestParamsDTO.getFilter()) && baseAPIService.checkNull(requestParamsDTO.getFilter().get(0)) && baseAPIService.checkNull(requestParamsDTO.getFilter().get(0).getLogicalOperatorPrefix()) && baseAPIService.checkNull(requestParamsDTO.getFilter().get(0).getFields()) && baseAPIService.checkNull(requestParamsDTO.getFilter().get(0).getFields().get(0))){
			processedData.put("hasFilter",true);
		}
		if(baseAPIService.checkNull(requestParamsDTO.getAggregations()) && processedData.get("hasGroupBy")){
			processedData.put("hasAggregate",true);	
		}
		if(baseAPIService.checkNull(requestParamsDTO.getPagination())){
			if(baseAPIService.checkNull(requestParamsDTO.getPagination().getLimit())){
				processedData.put("hasLimit",true);
			}
			if(baseAPIService.checkNull(requestParamsDTO.getPagination().getOffset())){
				processedData.put("hasOffset",true);
			}
			if(baseAPIService.checkNull(requestParamsDTO.getPagination().getOrder())){
				if(baseAPIService.checkNull(requestParamsDTO.getPagination().getOrder().get(0)))
					if(baseAPIService.checkNull(requestParamsDTO.getPagination().getOrder().get(0).getSortBy())){
						processedData.put("hasSortBy",true);
					}
					if(baseAPIService.checkNull(requestParamsDTO.getPagination().getOrder().get(0).getSortOrder())){
					processedData.put("hasSortOrder",true);
					}
			}
		}
		return processedData;
	}
	public String getClasspageCollectionDetail(String data) {
		return null;
	}
	
	public String[] getIndices(String names){
		String[] indices = new String[names.split(",").length];
		String[] requestNames = names.split(",");
		for(int i =0;i<indices.length;i++){
			if(indexMap.containsKey(requestNames[i])){
				indices[i] = indexMap.get(requestNames[i]);
			}
		}
		return indices;
	}
}

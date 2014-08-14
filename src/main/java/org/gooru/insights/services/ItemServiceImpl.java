package org.gooru.insights.services;

import java.util.HashMap;
import java.util.Map;

import org.gooru.insights.constants.ESConstants.esIndices;
import org.gooru.insights.constants.ESConstants.esTypes;
import org.gooru.insights.models.RequestParamsDTO;
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
	
	public String TestSearch(String data,Map<Integer,String> errorMap){
		RequestParamsDTO requestParamsDTO;
		try{
		requestParamsDTO = baseAPIService.buildRequestParameters(data);
		}catch(Exception e){
			errorMap.put(400, "Not a Valid JSON ");
			return null;
		}
		return esService.searchData(baseAPIService.convertStringtoArray(indexMap.get(esIndices.RAW_DATA.esIndex())),baseAPIService.convertStringtoArray(esTypes.EVENT_DETAIL.esType()),requestParamsDTO.getFields(), null, null);
	}

	public String getClasspageCollectionDetail(String data) {
		return null;
	}
}

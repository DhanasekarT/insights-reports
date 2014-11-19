package org.gooru.insights.services;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

public interface ItemService {

	JSONArray getEventDetail(String data,Map<String,Object> dataMap,Map<Integer,String> errorMap);
	
	JSONArray processApi(String data,Map<String,Object> dataMap,Map<Integer,String> errorMap);
	
}

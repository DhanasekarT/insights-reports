package org.gooru.insights.services;

import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

public interface ItemService {

	String getClasspageCollectionDetail(String data);
	
	JSONArray getEventDetail(String data,Map<String,Object> dataMap,Map<Integer,String> errorMap);
}

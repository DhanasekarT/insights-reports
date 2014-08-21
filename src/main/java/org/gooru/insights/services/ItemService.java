package org.gooru.insights.services;

import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

public interface ItemService {

	String getClasspageCollectionDetail(String data);
	
	String getEventDetail(String data,Map<String,String> dataMap,Map<Integer,String> errorMap);
}

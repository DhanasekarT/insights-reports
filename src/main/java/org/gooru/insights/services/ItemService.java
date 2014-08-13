package org.gooru.insights.services;

import java.util.Map;

public interface ItemService {

	String getClasspageCollectionDetail(String data);
	
	String TestSearch(String data,Map<Integer,String> errorMap);
}

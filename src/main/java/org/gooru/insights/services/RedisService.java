package org.gooru.insights.services;

import java.util.Map;
import java.util.Set;

import org.gooru.insights.models.ResponseParamDTO;

public interface RedisService {
	
	boolean hasKey(String key);
	
	String getValue(String key);
	
	void putLongValue(String key,Long value);
	
	boolean removeKey(String key);
	
	boolean removeKeys();
	
	boolean removeKeys(String[] key);
	
	String putStringValue(String key,String value);
	
	boolean clearQuery(String id);

	String getQuery(String prefix, String id);

	boolean insertKey(String data);
	
	Set<String> getKeys();
	
	String getDirectValue(String key);
	
	String putDirectValue(String key,String value);
	
	public <M> String putCache(String traceId,String query, Map<String, Object> userMap, ResponseParamDTO<M> responseParamDTO);

}

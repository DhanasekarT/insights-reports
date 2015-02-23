package org.gooru.insights.builders.utils;

import java.util.Set;

public interface RedisService {
	
	boolean hasRedisKey(String key);
	
	String getRedisValue(String key);
	
	void putRedisLongValue(String key,Long value);
	
	boolean removeRedisKey(String key);
	
	boolean removeRedisKeys();
	
	boolean removeRedisKeys(String[] key);
	
	String putRedisStringValue(String key,String value);
	
	Set<String> getKeys();
	
	String getRedisRawValue(String key);
	
	String putRedisRawValue(String key,String value);
	
}

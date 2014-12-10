package org.gooru.insights.services;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.gooru.insights.constants.APIConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

@Component
public class RedisServiceImpl implements APIConstants,RedisService {

	final StringRedisSerializer STRING_SERIALIZER = new StringRedisSerializer();
	
	final LongSerializer LONG_SERIALIZER  = LongSerializer.INSTANCE;
	
	@Autowired(required = false)
	private RedisTemplate<String, Long> redisLongTemplate = new RedisTemplate() ;
	
	@Autowired(required = false)
	private  RedisTemplate<String, String> redisStringTemplate;
	
	@PostConstruct
	void init(){
		
		setRedisStringSerializerTemplate();
		setRedisLongSerializerTemplate();
	}
	public void setRedisLongSerializerTemplate(){
		redisLongTemplate.setKeySerializer(STRING_SERIALIZER);
		redisLongTemplate.setValueSerializer(LONG_SERIALIZER);
	}
	
	public void setRedisStringSerializerTemplate(){
		redisStringTemplate.setKeySerializer(STRING_SERIALIZER);
		redisStringTemplate.setValueSerializer(STRING_SERIALIZER);
	}
	
	public ValueOperations<String, Long> redisLongOperation(){
		return redisLongTemplate.opsForValue();
	}
	
	public ValueOperations<String, String> redisStringOperation(){
		return redisStringTemplate.opsForValue();
	}
	
	public void putRedisLongValue(String key,Long value){
		redisLongOperation().set(CACHE_PREFIX+SEPARATOR+key, value);
	}
	
	public String putRedisStringValue(String key,String value){
		if(!redisStringOperation().setIfAbsent(CACHE_PREFIX+SEPARATOR+key, value)){
			return redisStringOperation().get(CACHE_PREFIX+SEPARATOR+key);
		}
		return null;
	}
	
	public boolean hasRedisKey(String key){
		if(redisStringTemplate.hasKey(CACHE_PREFIX+SEPARATOR+key)){
			return true;
		}else{
			return false;
		}
	}
	
	public String getRedisValue(String key){
			return redisStringOperation().get(CACHE_PREFIX+SEPARATOR+key);
	}
	
	public String getRedisRawValue(String key){
		return redisStringOperation().get(key);
	}
	
	
	public boolean removeRedisKey(String key){
		try{
		redisStringTemplate.delete(CACHE_PREFIX+SEPARATOR+key);
		return true;
		}catch(Exception e){
			return false;
		}
	}
	
	public boolean removeRedisKeys(){
		try{
			Set<String> keys = redisStringTemplate.keys(CACHE_PREFIX+SEPARATOR+WILD_CARD);
			Iterator<String> itr = keys.iterator();
			while(itr.hasNext()){
		redisStringTemplate.delete(itr.next());
			}
		return true;
		}catch(Exception e){
			System.out.println("failed on remove ");
			return false;
		}
	}
	
	public Set<String> getKeys(){
		return redisStringTemplate.keys(CACHE_PREFIX+SEPARATOR+WILD_CARD);
	}
	public boolean removeRedisKeys(String[] key){
		try{
		redisStringTemplate.delete(CACHE_PREFIX+SEPARATOR+key);
		return true;
		}catch(Exception e){
			return false;
		}
	}
	
	public void putRedisLongValue(String key,Long value,Long ttl,String timeFormat){
		redisLongOperation().set(CACHE_PREFIX+SEPARATOR+key, value,ttl,checkTime(timeFormat));
	}
	
	public void putRedisStringValue(String key,String value,Long ttl,String timeFormat){
		redisStringOperation().set(CACHE_PREFIX+SEPARATOR+key, value,ttl,checkTime(timeFormat));
	}
	
	public TimeUnit checkTime(String timeFormat) {
		if (timeFormat != null && !timeFormat.isEmpty()) {
			if ("HOUR".equalsIgnoreCase(timeFormat)) {
				return TimeUnit.HOURS;
			} else if ("MINUTE".equalsIgnoreCase(timeFormat)) {
				return TimeUnit.MINUTES;
			} else if ("SECOND".equalsIgnoreCase(timeFormat)) {
				return TimeUnit.SECONDS;
			} else if ("MILLISECOND".equalsIgnoreCase(timeFormat)) {
				return TimeUnit.MILLISECONDS;
			} else if ("DAY".equalsIgnoreCase(timeFormat)) {
				return TimeUnit.DAYS;
			} else if ("NANOSECOND".equalsIgnoreCase(timeFormat)) {
				return TimeUnit.NANOSECONDS;
			} else {
				return TimeUnit.SECONDS;
			}
		} else {
			return TimeUnit.SECONDS;
		}
	}
	
	public RedisTemplate<String, String> getRedisStringOperationTemplate(){
		return redisStringTemplate;
	}
	

	

	private enum LongSerializer implements RedisSerializer<Long> {

	INSTANCE;

	@Override
	public byte[] serialize(Long aLong) throws SerializationException {
		if (null != aLong) {
			return aLong.toString().getBytes();
		} else {
			return new byte[0];
		}
	}

	@Override
	public Long deserialize(byte[] bytes) throws SerializationException {
		if (bytes != null && bytes.length > 0) {
			return Long.parseLong(new String(bytes));
		} else {
			return null;
		}
	}
}
}
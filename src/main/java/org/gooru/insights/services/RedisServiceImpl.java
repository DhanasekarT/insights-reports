package org.gooru.insights.services;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.gooru.insights.builders.utils.InsightsLogger;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.APIConstants.DateFormats;
import org.gooru.insights.models.ResponseParamDTO;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import flexjson.JSONSerializer;

@Component
public class RedisServiceImpl implements RedisService {

	private static final StringRedisSerializer STRING_SERIALIZER = new StringRedisSerializer();

	private static final LongSerializer LONG_SERIALIZER = LongSerializer.INSTANCE;

	@Autowired(required = false)
	private RedisTemplate<String, Long> redisLongTemplate = new RedisTemplate();

	@Autowired(required = false)
	private RedisTemplate<String, String> redisStringTemplate;

	@PostConstruct
	void init() {

		setStringSerializerTemplate();
		setLongSerializerTemplate();
	}

	public boolean clearQuery(String id) {
		boolean status = false;
		if (id != null && !id.isEmpty()) {
			for (String requestId : id.split(APIConstants.COMMA)) {
				if (hasKey(requestId)) {
					String queryId = getValue(requestId);
					removeKey(requestId);
					if (hasKey(queryId)) {
						removeKey(APIConstants.CACHE_PREFIX_ID + APIConstants.SEPARATOR + queryId);
						status = removeKey(queryId);
					}
				}
			}
		} else {
			status = removeKeys();
		}
		return status;
	}

	public String getQuery(String prefix, String id) {
		if (hasKey(prefix + id)) {
			if (hasKey(prefix + getValue(prefix + id))) {
				return getValue(prefix + getValue(prefix + id));
			}
		}
		return null;
	}

	public boolean insertKey(String data) {
		try {
			JSONObject jsonObject = new JSONObject(data);
			Iterator<String> itr = jsonObject.keys();
			while (itr.hasNext()) {
				String key = itr.next();
				putDirectValue(key, jsonObject.getString(key));
			}
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	private void setLongSerializerTemplate() {
		redisLongTemplate.setKeySerializer(STRING_SERIALIZER);
		redisLongTemplate.setValueSerializer(LONG_SERIALIZER);
	}

	private void setStringSerializerTemplate() {
		redisStringTemplate.setKeySerializer(STRING_SERIALIZER);
		redisStringTemplate.setValueSerializer(STRING_SERIALIZER);
	}

	private ValueOperations<String, Long> longOperation() {
		return redisLongTemplate.opsForValue();
	}

	private ValueOperations<String, String> stringOperation() {
		return redisStringTemplate.opsForValue();
	}

	public void putLongValue(String key, Long value) {
		longOperation().set(APIConstants.CACHE_PREFIX + APIConstants.SEPARATOR + key, value);
	}

	public String putStringValue(String key, String value) {
		if (!stringOperation().setIfAbsent(APIConstants.CACHE_PREFIX + APIConstants.SEPARATOR + key, value)) {
			return stringOperation().get(APIConstants.CACHE_PREFIX + APIConstants.SEPARATOR + key);
		}
		return null;
	}

	public String putDirectValue(String key, String value) {
		if (!stringOperation().setIfAbsent(key, value)) {
			return stringOperation().get(key);
		}
		return null;
	}

	public boolean hasKey(String key) {
		if (redisStringTemplate.hasKey(APIConstants.CACHE_PREFIX + APIConstants.SEPARATOR + key)) {
			return true;
		} else {
			return false;
		}
	}

	public String getValue(String key) {
		return stringOperation().get(APIConstants.CACHE_PREFIX + APIConstants.SEPARATOR + key);
	}

	public String getDirectValue(String key) {
		return stringOperation().get(key);
	}

	public boolean removeKey(String key) {
		try {
			redisStringTemplate.delete(APIConstants.CACHE_PREFIX + APIConstants.SEPARATOR + key);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	public <M> String putCache(String traceId,String query, Map<String, Object> userMap, ResponseParamDTO<M> responseParamDTO) {

		UUID queryId = UUID.randomUUID();
		String KEY_PREFIX = APIConstants.EMPTY;
		if (userMap.containsKey(APIConstants.GOORUUID) && userMap.get(APIConstants.GOORUUID) != null) {
			KEY_PREFIX += userMap.get(APIConstants.GOORUUID) + APIConstants.SEPARATOR;
		}
		if (hasKey(APIConstants.CACHE_PREFIX_ID + APIConstants.SEPARATOR + KEY_PREFIX + query.trim())) {
			return getValue(APIConstants.CACHE_PREFIX_ID + APIConstants.SEPARATOR + KEY_PREFIX + query.trim());
		}
		putStringValue(KEY_PREFIX + queryId.toString(), query.trim());
		putStringValue(KEY_PREFIX + query.trim(), new JSONSerializer().exclude(APIConstants.EXCLUDE_CLASSES).deepSerialize(responseParamDTO));
		putStringValue(APIConstants.CACHE_PREFIX_ID + APIConstants.SEPARATOR + KEY_PREFIX + query.trim(), queryId.toString());
		InsightsLogger.info(traceId, APIConstants.NEW_QUERY + queryId);
		return queryId.toString();

	}

	public boolean removeKeys() {
		try {
			Set<String> keys = redisStringTemplate.keys(APIConstants.CACHE_PREFIX + APIConstants.SEPARATOR + APIConstants.WILD_CARD);
			Iterator<String> itr = keys.iterator();
			while (itr.hasNext()) {
				redisStringTemplate.delete(itr.next());
			}
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	public Set<String> getKeys() {
		return redisStringTemplate.keys(APIConstants.CACHE_PREFIX + APIConstants.SEPARATOR + APIConstants.WILD_CARD);
	}

	public boolean removeKeys(String[] key) {
		try {
			redisStringTemplate.delete(APIConstants.CACHE_PREFIX + APIConstants.SEPARATOR + key);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	private void putLongValue(String key, Long value, Long ttl, String timeFormat) {
		longOperation().set(APIConstants.CACHE_PREFIX + APIConstants.SEPARATOR + key, value, ttl, checkTime(timeFormat));
	}

	private void putStringValue(String key, String value, Long ttl, String timeFormat) {
		stringOperation().set(APIConstants.CACHE_PREFIX + APIConstants.SEPARATOR + key, value, ttl, checkTime(timeFormat));
	}

	private TimeUnit checkTime(String timeFormat) {
		if (timeFormat != null && !timeFormat.isEmpty()) {
			if (DateFormats.HOUR.name().equalsIgnoreCase(timeFormat)) {
				return TimeUnit.HOURS;
			} else if (DateFormats.MINUTE.name().equalsIgnoreCase(timeFormat)) {
				return TimeUnit.MINUTES;
			} else if (DateFormats.SECOND.name().equalsIgnoreCase(timeFormat)) {
				return TimeUnit.SECONDS;
			} else if (DateFormats.MILLISECOND.name().equalsIgnoreCase(timeFormat)) {
				return TimeUnit.MILLISECONDS;
			} else if (DateFormats.DAY.name().equalsIgnoreCase(timeFormat)) {
				return TimeUnit.DAYS;
			} else if (DateFormats.NANOSECOND.name().equalsIgnoreCase(timeFormat)) {
				return TimeUnit.NANOSECONDS;
			} else {
				return TimeUnit.SECONDS;
			}
		} else {
			return TimeUnit.SECONDS;
		}
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
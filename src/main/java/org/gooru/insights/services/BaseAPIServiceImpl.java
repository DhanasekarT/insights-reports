package org.gooru.insights.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.gooru.insights.models.RequestParamsDTO;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import flexjson.JSONDeserializer;
import flexjson.JSONException;

@Service
public class BaseAPIServiceImpl implements BaseAPIService{


	public RequestParamsDTO buildRequestParameters(String data){

		try{
		return data != null ? deserialize(data, RequestParamsDTO.class) : null;
		}catch(Exception e){
			throw new JSONException();
		}
	}
	
	public boolean checkNull(String parameter) {

		if (parameter != null && parameter != "" && (!parameter.isEmpty())) {

			return true;

		} else {

			return false;
		}
	}
	
	public boolean checkNull(Object request){
		if (request != null) {

			return true;

		} else {

			return false;
		}
	}
	
	public boolean checkNull(Map<?,?> request) {

		if (request != null && (!request.isEmpty())) {

			return true;

		} else {

			return false;
		}
	}
	
	public boolean checkNull(Collection<?> request) {

		if (request != null && (!request.isEmpty())) {

			return true;

		} else {

			return false;
		}
	}
	
	public boolean checkNull(Integer parameter) {

		if (parameter != null && parameter.SIZE > 0 && (!parameter.toString().isEmpty())) {

			return true;

		} else {

			return false;
		}
	}

	public String assignValue(String parameter) {
		if (parameter != null && parameter != "" && (!parameter.isEmpty())) {

			return parameter;

		} else {

			return null;
		}

	}

	public Integer assignValue(Integer parameter) {

		if (parameter != null && parameter.SIZE > 0 && (!parameter.toString().isEmpty())) {

			return parameter;

		} else {

			return null;
		}
	}
	public <T>  T  deserialize(String json, Class<T> clazz) {
		try {
			return new JSONDeserializer<T>().use(null, clazz).deserialize(json);
		} catch (Exception e) {
			throw new JSONException();
		}
	}
	
	public <T> T deserializeTypeRef(String json, TypeReference<T> type) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
			return mapper.readValue(json, type);
	}
	
	public String[] convertStringtoArray(String data){
		return data.split(",");
	}
	
	public String[] convertSettoArray(Set<String> data){
		return data.toArray(new String[data.size()]);
	}
	
	public JSONArray InnerJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String commonKey) {
		JSONArray jsonArray = new JSONArray();
		if (!child.isEmpty() && !parent.isEmpty()) {
			for (Map<String, Object> childEntry : child) {
				Map<String, Object> appended = new HashMap<String, Object>();
				for (Map<String, Object> parentEntry : parent) {
					if (childEntry.containsKey(commonKey) && parentEntry.containsKey(commonKey)) {
						if (childEntry.get(commonKey).equals(parentEntry.get(commonKey))) {
							childEntry.remove(commonKey);
							appended.putAll(childEntry);
							appended.putAll(parentEntry);
							break;
						}
					}
				}
				if(checkNull(appended)){
				jsonArray.put(appended);
				}
			}
			return jsonArray;
		}
		return jsonArray;
	}
	
	public 	JSONArray InnerJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child){
		JSONArray jsonArray = new JSONArray();
		if (!child.isEmpty() && !parent.isEmpty()) {
			for (Map<String, Object> childEntry : child) {
				Map<String, Object> appended = new HashMap<String, Object>();
					Set<String> keys = childEntry.keySet();
				for (Map<String, Object> parentEntry : parent) {
					boolean valid = true;
					for(String key : keys){
					if(parentEntry.containsKey(key) && childEntry.containsKey(key) && (!parentEntry.get(key).equals(childEntry.get(key)))){
						valid = false;
					}
					}
					if(valid){
					appended.putAll(parentEntry);
					appended.putAll(childEntry);
					break;
					}
				}
				if(checkNull(appended)){
				jsonArray.put(appended);
				}
				}
			}
			return jsonArray;
		}

	public List<Map<String, Object>> innerJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child){
		List<Map<String, Object>> resultData = new ArrayList<Map<String,Object>>();
		if (!child.isEmpty() && !parent.isEmpty()) {
			for (Map<String, Object> childEntry : child) {
				Map<String, Object> appended = new HashMap<String, Object>();
					Set<String> keys = childEntry.keySet();
				for (Map<String, Object> parentEntry : parent) {
					boolean valid = true;
					for(String key : keys){
					if(parentEntry.containsKey(key) && childEntry.containsKey(key) && (!parentEntry.get(key).equals(childEntry.get(key)))){
						valid = false;
					}
					}
					if(valid){
					appended.putAll(parentEntry);
					appended.putAll(childEntry);
					break;
					}
				}
				if(checkNull(appended)){
					resultData.add(appended);
				}
				}
			}
			return resultData;
	}
	@Override
	public String convertArraytoString(String[] datas) {
		StringBuffer result = new StringBuffer();
		for(String data : datas){
			if(result.length() > 0){
			result.append(",");
			}
			result.append(data);
		}
		return result.toString();
	}
	
	public List<Map<String, Object>> sortBy(List<Map<String, Object>> requestData, String sortBy, String sortOrder) {

		if (checkNull(sortBy)) {
			for (final String name : sortBy.split(",")) {
				boolean ascending = false;
				boolean descending = false;
				if (checkNull(sortOrder)) {
					if (sortOrder.equalsIgnoreCase("ASC")) {
						ascending = true;
					} else if (sortOrder.equalsIgnoreCase("DESC")) {
						descending = true;
					} else {
						ascending = true;
					}
				} else {
					ascending = true;
				}
				if (ascending) {
					Collections.sort(requestData, new Comparator<Map<String, Object>>() {
						public int compare(final Map<String, Object> m1, final Map<String, Object> m2) {
							if (m1.containsKey(name) && m2.containsKey(name)) {
								if (m1.get(name) instanceof String) {
									if (m2.containsKey(name))
										return ((String) m1.get(name).toString().toLowerCase()).compareTo((String) m2.get(name).toString().toLowerCase());
								} else if (m1.get(name) instanceof Long) {
									if (m2.containsKey(name))
										return ((Long) m1.get(name)).compareTo((Long) m2.get(name));

								} else if (m1.get(name) instanceof Integer) {
									if (m2.containsKey(name))
										return ((Integer) m1.get(name)).compareTo((Integer) m2.get(name));

								} else if (m1.get(name) instanceof Double) {
									if (m2.containsKey(name))
										return ((Double) m1.get(name)).compareTo((Double) m2.get(name));
								}
							}
							return 1;
						}
					});
				}
				if (descending) {
					Collections.sort(requestData, new Comparator<Map<String, Object>>() {
						public int compare(final Map<String, Object> m1, final Map<String, Object> m2) {

							if (m2.containsKey(name)) {
								if (m1.containsKey(name)) {
									if (m2.get(name) instanceof String) {
										if (m2.containsKey(name))
											return ((String) m2.get(name).toString().toLowerCase()).compareTo((String) m1.get(name).toString().toLowerCase());

									} else if (m2.get(name) instanceof Long) {
										if (m2.containsKey(name))
											return ((Long) m2.get(name)).compareTo((Long) m1.get(name));

									} else if (m2.get(name) instanceof Integer) {
										if (m2.containsKey(name))
											return ((Integer) m2.get(name)).compareTo((Integer) m1.get(name));

									} else if (m2.get(name) instanceof Double) {
										if (m2.containsKey(name))
											return ((Double) m2.get(name)).compareTo((Double) m1.get(name));
									}
								} else {
									return 1;
								}
							} else {
								return -1;
							}
							return 0;
						}
					});

				}
			}
		}
		return requestData;
	}
	
}

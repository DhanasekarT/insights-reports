package org.gooru.insights.services;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.gooru.insights.models.RequestParamsDTO;
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
}

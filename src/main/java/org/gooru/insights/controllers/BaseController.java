package org.gooru.insights.controllers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Controller;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class BaseController {

	protected ModelAndView getModel(JSONArray data,Map<String,Object> messageData){
		return  this.resultSet(data,messageData);
	}
	
	protected ModelAndView getModel(JSONArray data,JSONObject messageData){
		return  this.resultSet(data,messageData);
	}

	public ModelAndView getModel(Map<String,String> data){
		return  this.resultSet(data);
	}
	public ModelAndView getModels(Map<Integer,String> data){
		return  this.resultSets(data);
	}

	public ModelAndView getReportModel(Map<String, String> data) {

		ModelAndView model = new ModelAndView("content");

		if (data != null && !data.isEmpty()) {
			model.addObject("content", data);
		} else {
			Map<String, String> finalData = new LinkedHashMap<String, String>();
			finalData.put("Message", "File download link will be sent to your email Account");
			model.addObject("content", finalData);
		}
		return model;
	}
	public void sendErrorResponse(HttpServletRequest request, HttpServletResponse response,Map<Integer,String> errorMap) {
		for(Map.Entry<Integer,String> entry : errorMap.entrySet()){ 
		response.setStatus(entry.getKey());
		 response.setContentType("application/json");
		 Map<String, Object> resultMap = new HashMap<String, Object>();
		 try {
		 
		 resultMap.put("statusCode", entry.getKey());
		 resultMap.put("message", entry.getValue());
		 JSONObject resultJson = new JSONObject(resultMap);
		 response.getWriter().write(resultJson.toString());
		 response.setStatus(entry.getKey());
		 } catch (IOException e) {
			 e.printStackTrace();
		 }
		}
	}
	
	public ModelAndView resultSet(List<String> data,Map<String,Object> messageData){
		ModelAndView model = new ModelAndView("content");
		model.addObject("content", data);
		addFilterData(model, messageData);
		if(messageData != null){
		model.addObject("message", messageData);
		
		}
		return model;
	}
	
	public ModelAndView resultSet(JSONArray data,Map<String,Object> messageData){
		ModelAndView model = new ModelAndView("content");
		try {
			JSONObject resultMap = new JSONObject();
				resultMap.put("content",data );
			addFilterData(resultMap, messageData);
			if(messageData != null){
			resultMap.put("message",messageData );
			}
			model.addObject("content", resultMap);
		} catch (JSONException e) {
			e.printStackTrace();
		}
			return model;
		}
	
	public ModelAndView resultSet(JSONArray data,JSONObject messageData){
		ModelAndView model = new ModelAndView("content");
		System.out.println("DATA array");
		try {
			JSONObject resultMap = new JSONObject();
				resultMap.put("content",data );
			addFilterData(resultMap, messageData);
			if(messageData.length() > 0){
			resultMap.put("message",messageData );
			}
			System.out.println("error message");
			model.addObject("content", resultMap);
		} catch (JSONException e) {
			System.out.println("error catch");
			e.printStackTrace();
		}
			return model;
		}
	
	public ModelAndView resultSet(Map<String,String> messageData){
		ModelAndView model = new ModelAndView("content");
		try {
			JSONObject resultMap = new JSONObject();
				resultMap.put("content",new ArrayList<Map<String,String>>() );
				resultMap.put("paginate", new HashMap<String, String>());
			resultMap.put("message",messageData );
			model.addObject("content", resultMap);
		} catch (JSONException e) {
			e.printStackTrace();
		}
			return model;
		}
	
	public ModelAndView resultSets(Map<Integer,String> messageData){
		ModelAndView model = new ModelAndView("content");
		try {
			JSONObject resultMap = new JSONObject();
				resultMap.put("content",new ArrayList<Map<String,String>>() );
				resultMap.put("paginate", new HashMap<String, String>());
			resultMap.put("message",messageData );
			model.addObject("content", resultMap);
		} catch (JSONException e) {
			e.printStackTrace();
		}
			return model;
		}
	
	public void addFilterData(ModelAndView model,Map<String,Object> messageData){
		if(messageData != null){
			Map<String,Object> filterDate = new HashMap<String,Object>();
			Object totalRows = messageData.get("totalRows");
			if(totalRows != null){
				filterDate.put("totalRows",totalRows);
			}
			model.addObject("paginate",filterDate);
			messageData.remove("totalRows");
	}
	}
	
	public void addFilterData(JSONObject data,Map<String,Object> messageData) throws JSONException{
		Map<String,Object> paginateData = new HashMap<String, Object>();
		if(messageData != null){
			Object totalRows=0 ;
				totalRows = (messageData.get("totalRows") != null ? messageData.get("totalRows") : 0 );				
				
				if(messageData.containsKey("totalRows")){
					messageData.remove("totalRows");
				}
				
			if(totalRows != null){
			paginateData.put("totalRows",totalRows);
					
			}
	}
		data.put("paginate", paginateData);
	}
	
	public void addFilterData(JSONObject data,JSONObject messageData) throws JSONException{
		Map<String,Long> paginateData = new HashMap<String, Long>();
		if(messageData.has("totalRows")){
			Long totalRows ;
				totalRows = (messageData.get("totalRows") != null ? messageData.getLong("totalRows") : 0L );				
				
				if(messageData.has("totalRows")){
					messageData.remove("totalRows");
				}
			paginateData.put("totalRows",totalRows);
	}
		data.put("paginate", paginateData);
	}
	
	public void sendError(HttpServletResponse response,Map<Integer,String> errorMap){

		try {
			JSONObject json = new JSONObject();
			json.put("paginate", new JSONObject());
			json.put("content", new JSONArray());
			Map<Object,Object> message = new HashMap<Object, Object>();
			Integer errorCode =500;
		for(Map.Entry<Integer,String> entry : errorMap.entrySet()){
			errorCode =  entry.getKey();
				message.put(errorCode, entry.getValue()); 
				json.append("message", message);
		}
		response.getWriter().write(json.toString());
		response.setStatus(errorCode);
		
	} catch (Exception e) {
		e.printStackTrace();
	}
	}
	
	
}

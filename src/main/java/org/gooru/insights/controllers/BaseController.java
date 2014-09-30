package org.gooru.insights.controllers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.mortbay.util.ajax.JSON;
import org.springframework.stereotype.Controller;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class BaseController {

	Map<String,Object> errorData = new HashMap<String,Object>();
	
	protected ModelAndView getModel(JSONArray data,Map<String,Object> messageData){
		return  this.resultSet(data,messageData);
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
		clearMessage();
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
			clearMessage();
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
		if(messageData != null){
			Long totalRows;
				totalRows = (messageData.get("totalRows") != null ? Long.valueOf(messageData.get("totalRows").toString()) : 0L );				
				if(totalRows != null){
					messageData.remove("totalRows");
				}
			Map<String,Long> paginateData = new HashMap<String, Long>();
			if(totalRows != null){
			paginateData.put("totalRows",totalRows);
					
			}
			data.put("paginate", paginateData);
	}
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
		
	} catch (Exception e) {
		e.printStackTrace();
	}
	}
	
	public Map<String,Object> getMessage(){
	return this.errorData;
	}
	
	public void clearMessage(){
		this.errorData = new HashMap<String,Object>();
	}
	
}

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
import org.springframework.stereotype.Controller;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class BaseController {

	Map<String,String> dataData = new HashMap<String,String>();
	
	protected ModelAndView getModel(JSONArray data,Map<String,String> messageData){
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
	
	public ModelAndView resultSet(List<String> data,Map<String,String> messageData){
		ModelAndView model = new ModelAndView("content");
		model.addObject("content", data);
		addFilterDate(model, messageData);
		if(messageData != null){
		model.addObject("message", messageData);
		
		}
		clearMessage();
		return model;
	}
	
	public ModelAndView resultSet(JSONArray data,Map<String,String> messageData){
		ModelAndView model = new ModelAndView("content");
		try {
			JSONObject resultMap = new JSONObject();
				resultMap.put("content",data );
			addFilterDate(resultMap, messageData);
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
	public void addFilterDate(ModelAndView model,Map<String,String> messageData){
		if(messageData != null){
	
			List<Map<String,String>> dateRange = new ArrayList<Map<String,String>>();
			Map<String,String> filterDate = new HashMap<String,String>();
			String unixStartDate = null;
			String unixEndDate = null;
			String totalRows = null;
				unixStartDate  = messageData.get("unixStartDate");
				unixEndDate = messageData.get("unixEndDate");
				totalRows = messageData.get("totalRows");
				if(unixStartDate != null){
					filterDate.put("unixStartDate", unixStartDate);
					messageData.remove("unixStartDate");
				}
				if(unixEndDate != null){
					filterDate.put("unixEndDate", unixEndDate);
					messageData.remove("unixEndDate");
				}
				if(totalRows != null){
					messageData.remove("totalRows");
				}
			dateRange.add(filterDate);
			
			model.addObject("dateRange",dateRange);
			filterDate = new HashMap<String, String>();
			dateRange = new ArrayList<Map<String,String>>() ;
			if(totalRows != null){
				filterDate.put("totalRows",totalRows);
				dateRange.add(filterDate);
				
			}
			model.addObject("paginate",filterDate);
	}
	}
	
	public void addFilterDate(JSONObject data,Map<String,String> messageData) throws JSONException{
		if(messageData != null){
			Map<String,String> filterDate = new HashMap<String,String>();
			String unixStartDate = null;
			String unixEndDate = null;
			Long totalRows;
				unixStartDate  = messageData.get("unixStartDate");
				unixEndDate = messageData.get("unixEndDate");
				totalRows = (messageData.get("totalRows") != null ? Long.valueOf(messageData.get("totalRows")) : 0L );				
				if(unixStartDate != null){
					filterDate.put("unixStartDate", unixStartDate);
					messageData.remove("unixStartDate");
				}
				if(unixEndDate != null){
					filterDate.put("unixEndDate", unixEndDate);
					messageData.remove("unixEndDate");
				}
				if(totalRows != null){
					messageData.remove("totalRows");
				}
			data.put("dateRange", filterDate);
			Map<String,Long> paginateData = new HashMap<String, Long>();
			if(totalRows != null){
			paginateData.put("totalRows",totalRows);
					
			}
			data.put("paginate", paginateData);
	}
	}
	
	public void sendError(HttpServletResponse response,Map<Integer,String> errorMap) throws IOException{
		
		for(Map.Entry<Integer,String> entry : errorMap.entrySet()){
			
			JSONObject json = new JSONObject();
			try {
				json.put("developer Message:", entry.getValue()); 
				json.put("status Code:", entry.getKey());
			} catch (JSONException e) {
				e.printStackTrace();
			}
			response.sendError(entry.getKey(),json.toString());
		}
	}
	public Map<String,String> getMessage(){
	return this.dataData;
	}
	
	public void clearMessage(){
		this.dataData = new HashMap<String,String>();
	}
	
}

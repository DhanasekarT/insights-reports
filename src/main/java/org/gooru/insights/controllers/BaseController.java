package org.gooru.insights.controllers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;
import org.springframework.stereotype.Controller;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class BaseController {

	List<Map<String,String>> errorData = new ArrayList<Map<String,String>>();
	
	protected ModelAndView getModel(List<Map<String,Object>> data,List<Map<String,String>> errorData){
		return  this.resultSet(data,errorData);
	}
	protected ModelAndView getModelString(List<Map<String,String>> data,List<Map<String,String>> errorData){
		return  this.resultSetString(data,errorData);
	}
	protected ModelAndView getModel(Map<String,Object> data,List<Map<String,String>> errorData){
		return  this.resultSet(data,errorData);
	}
	protected ModelAndView getModelObject(List<Map<Object,Object>> data,List<Map<String,String>> errorData){
		return  this.resultListObject(data,errorData);
	}
	public ModelAndView resultSetString(List<Map<String,String>> data,List<Map<String,String>> errorData){
		ModelAndView model = new ModelAndView("content");
			model.addObject("content", data);
		addFilterDate(model, errorData);
		if(errorData != null){

			model.addObject("message", errorData);
		
		}
		clearErrors();
		return model;
	}
	
	public ModelAndView resultListObject(List<Map<Object,Object>> data,List<Map<String,String>> errorData){
		ModelAndView model = new ModelAndView("content");
			model.addObject("content", data);
		addFilterDate(model, errorData);
		if(errorData != null){

			model.addObject("message", errorData);
		
		}
		clearErrors();
		return model;
	}
	protected ModelAndView getModel(JSONObject data,List<Map<String,String>> errorData){
		return  this.resultSet(data,errorData);
	}
	
	public ModelAndView resultSet(List<Map<String,Object>> data,List<Map<String,String>> errorData){
		ModelAndView model = new ModelAndView("content");
			model.addObject("content", data);
		addFilterDate(model, errorData);
		if(errorData != null){

			model.addObject("message", errorData);
		
		}
		clearErrors();
		return model;
	}
	
	public ModelAndView resultSet(String data,List<Map<String,String>> errorData){
		ModelAndView model = new ModelAndView("content");
			model.addObject("content", data);
		addFilterDate(model, errorData);
		if(errorData != null){
		model.addObject("message", errorData);
		
		}
		clearErrors();
		return model;
	}
	
	public ModelAndView resultSet(Map<String,Object> data,List<Map<String,String>> errorData){
		ModelAndView model = new ModelAndView("content");
			model.addObject("content", data);
		addFilterDate(model, errorData);
		if(errorData != null){

			model.addObject("message", errorData);
		
		}
		clearErrors();
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
		 } catch (IOException e) {
			 e.printStackTrace();
		 }
		}
	}
	
	public ModelAndView resultSet(JSONObject data,List<Map<String,String>> errorData){
		ModelAndView model = new ModelAndView("content");
			model.addObject("content", data);
		addFilterDate(model, errorData);
		if(errorData != null){
		model.addObject("message", errorData);
		
		}
		clearErrors();
		return model;
	}
	
	public void addFilterDate(ModelAndView model,List<Map<String,String>> errorData){
		if(errorData != null){
	
			List<Map<String,String>> dateRange = new ArrayList<Map<String,String>>();
			Map<String,String> filterDate = new HashMap<String,String>();
			String unixStartDate = null;
			String unixEndDate = null;
			String totalRows = null;
			for(Map<String,String> values : errorData){
				unixStartDate  = values.get("unixStartDate");
				unixEndDate = values.get("unixEndDate");
				totalRows = values.get("totalRows");
				if(unixStartDate != null){
					filterDate.put("unixStartDate", unixStartDate);
					values.remove("unixStartDate");
				}
				if(unixEndDate != null){
					filterDate.put("unixEndDate", unixEndDate);
					values.remove("unixEndDate");
				}
				if(totalRows != null){
					values.remove("totalRows");
				}
				
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
	
	public List<Map<String,String>> handleErrors(){
	return this.errorData;
	}
	
	public void clearErrors(){
		this.errorData = new ArrayList<Map<String,String>>();
	}
	
	

}

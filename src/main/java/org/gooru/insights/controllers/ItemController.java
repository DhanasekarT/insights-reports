package org.gooru.insights.controllers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.constants.InsightsOperationConstants;
import org.gooru.insights.security.AuthorizeOperations;
import org.gooru.insights.services.ItemService;
import org.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

@RequestMapping(value ="/query")
@Controller
public class ItemController extends BaseController{
	
	@Autowired
	ItemService itemService;
	
	
	@RequestMapping(value="/classpage/collections",method = {RequestMethod.GET,RequestMethod.POST})
	public ModelAndView getClasspageCollectionDetail(HttpServletRequest request,@RequestParam(value="data", required = true) String data,@RequestParam(value="sessionToken",required = false) String sessionToken,HttpServletResponse response){
		ModelAndView model = new ModelAndView();
		model.setViewName("content");
		model.addObject("content","got working");
		return model;
	}
	
	@RequestMapping(value="/classpage",method = {RequestMethod.GET,RequestMethod.POST})
	public ModelAndView getClasspageDetail(HttpServletRequest request,@RequestParam(value="data", required = false) String data,HttpServletResponse response){
		ModelAndView model = new ModelAndView();
		model.setViewName("content");
		model.addObject("content","got working");
		return model;
	}

	@RequestMapping(method ={RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEW)
	public ModelAndView getEventDetail(HttpServletRequest request,@RequestParam(value="data",required = true) String data,HttpServletResponse response) throws IOException{
	    Map<Integer,String> errorMap = new HashMap<Integer,String>();
	    JSONArray jsonArray = itemService.getEventDetail(data,getMessage(),errorMap);
	     
	    if(!errorMap.isEmpty()){
	    	sendError(response,errorMap);
	    	return null;
	    }
	    return getModel(jsonArray, getMessage());
	}

	@RequestMapping(value="/clear/data",method =RequestMethod.GET)
	public ModelAndView clearDataCache(){
		Map<String,String> dataMap = new HashMap<String,String>();
		
		if(itemService.clearDataCache()){
		dataMap.put("status", "data cache cleared");	
		}else{
		dataMap.put("status", "problem while clear the data cache cleared");	
		}
		return getModel(dataMap);
	}
	
	@RequestMapping(value="/combine",method ={RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEW)
	public ModelAndView getItems(HttpServletRequest request,@RequestParam(value="data",required = true) String data,HttpServletResponse response) throws IOException{
		Map<Integer,String> errorMap = new HashMap<Integer,String>();
		JSONArray jsonArray = itemService.processApi(data, getMessage(), errorMap);
		
		if(!errorMap.isEmpty()){
		sendError(response,errorMap);
		return null;
		}
		return getModel(jsonArray, getMessage());
	}
	
	@RequestMapping(value="/clear/connection",method =RequestMethod.GET)
	public ModelAndView clearConnectionCache(){
		Map<String,String> dataMap = new HashMap<String,String>();
		itemService.clearConnectionCache();
		dataMap.put("status", "connection cache cleared");
		return getModel(dataMap);
	}

}

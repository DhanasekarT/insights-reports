package org.gooru.insights.controllers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.InsightsOperationConstants;
import org.gooru.insights.security.AuthorizeOperations;
import org.gooru.insights.services.ItemService;
import org.json.JSONArray;
import org.json.JSONObject;
import org.mortbay.util.ajax.JSON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

@RequestMapping(value ="/query")
@Controller
public class ItemController extends BaseController implements APIConstants{
	
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
	public ModelAndView getEventDetail(HttpServletRequest request,@RequestParam(value="data",required = true) String data,@RequestParam(value="sessionToken",required = true) String sessionToken,HttpServletResponse response) throws IOException{
		
		Map<Integer,String> errorMap = new HashMap<Integer,String>();
		Map<String,Object> dataMap = new HashMap<String,Object>();
	    
//	    Map<String,Object> userMap = itemService.getUserObject(sessionToken, errorMap); 
	   
	    Map<String,Object> userMap = itemService.getUserObjectData(sessionToken, errorMap); 
	    
	    JSONArray jsonArray = itemService.getEventDetail(data,dataMap,userMap,errorMap);
	     
	    if(!errorMap.isEmpty()){
	    	sendError(response,errorMap);
	    	return null;
	    }
	    return getModel(jsonArray, dataMap);	
	
	}
	
	@RequestMapping(value="/report",method ={RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEW)
	public ModelAndView getPartyReports(HttpServletRequest request,@RequestParam(value="reportType",required = true) String reportType,@RequestParam(value="data",required = true) String data,@RequestParam(value="sessionToken",required = true) String sessionToken,HttpServletResponse response) throws IOException{
		
		Map<Integer,String> errorMap = new HashMap<Integer,String>();
		Map<String,Object> dataMap = new HashMap<String,Object>();
	    	   
	    Map<String,Object> userMap = itemService.getUserObjectData(sessionToken, errorMap); 
	    
	    JSONArray jsonArray = itemService.getPartyReport(data,dataMap,userMap,errorMap);
	     
	    if(!errorMap.isEmpty()){
	    	sendError(response,errorMap);
	    	return null;
	    }
	    return getModel(jsonArray, dataMap);	
	
	}
	
	@RequestMapping(value="/clear/id",method =RequestMethod.GET)
	public ModelAndView clearRedisCache(HttpServletRequest request,@RequestParam(value="queryId",required = false) String queryId,HttpServletResponse response){
		Map<String,String> dataMap = new HashMap<String,String>();
		if(itemService.clearQuery(queryId)){
			if(queryId != null && !queryId.isEmpty()){
			dataMap.put("status","query has been removed");
			}else{
				dataMap.put("status","All the querys has been removed");
			}
			return getModel(dataMap);
		}
		dataMap.put("status","unable to delete the query");
		return getModel(dataMap);
	}
	
	@RequestMapping(value="/{id}",method =RequestMethod.GET)
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEW)
	public ModelAndView getRedisCache(HttpServletRequest request,@PathVariable("id") String queryId,@RequestParam(value="sessionToken",required = true) String sessionToken,HttpServletResponse response){
		
		Map<String,Object> dataMap = new HashMap<String,Object>();
		Map<Integer,String> errorMap = new HashMap<Integer,String>();
		Map<String,Object> userMap = itemService.getUserObjectData(sessionToken, errorMap); 
		
		 if(!errorMap.isEmpty()){
		    	sendError(response,errorMap);
		    	return null;
		 }
		 
		 String prefix = "";
		 if(userMap.containsKey("gooruUId") && userMap.get("gooruUId") != null){
			 prefix = userMap.get("gooruUId").toString()+SEPARATOR;
		 }
		 
		return getModel(itemService.getQuery(prefix,queryId,dataMap),dataMap);
	}

	
	@RequestMapping(value="/list",method =RequestMethod.GET)
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEW)
	public ModelAndView getRedisCacheList(HttpServletRequest request,@RequestParam(value="queryId",required = false) String queryId,@RequestParam(value="sessionToken",required = true) String sessionToken,HttpServletResponse response){
		
		Map<String,Object> dataMap = new HashMap<String,Object>();
		Map<Integer,String> errorMap = new HashMap<Integer,String>();
		Map<String,Object> userMap = itemService.getUserObjectData(sessionToken, errorMap); 
		 
		 if(!errorMap.isEmpty()){
		    	sendError(response,errorMap);
		    	return null;
		 }
		 
		 String prefix = "";
		 if(userMap.containsKey("gooruUId") && userMap.get("gooruUId") != null){
			 prefix = userMap.get("gooruUId").toString()+SEPARATOR;
		 }
		 
		return getModel(itemService.getCacheData(prefix,queryId),dataMap);
	}
	
	@RequestMapping(value="/keys",method =RequestMethod.PUT)
	public ModelAndView putRedisData(HttpServletRequest request,@RequestBody String data ,HttpServletResponse response){
		Map<String,String> resultData = new HashMap<String, String>();
		if(itemService.insertKey(data)){
			resultData.put("status", "inserted");
		}else{
			resultData.put("status", "failed to insert");
		}
		return getModel(resultData);
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
		Map<String,Object> dataMap = new HashMap<String,Object>();
		JSONArray jsonArray = itemService.processApi(data,dataMap, errorMap);
		
		if(!errorMap.isEmpty()){
		sendError(response,errorMap);
		return null;
		}
		return getModel(jsonArray,dataMap);
	}
	
	@RequestMapping(value="/clear/connection",method =RequestMethod.GET)
	public ModelAndView clearConnectionCache(){
		Map<String,String> dataMap = new HashMap<String,String>();
		itemService.clearConnectionCache();
		dataMap.put("status", "connection cache cleared");
		return getModel(dataMap);
	}

}

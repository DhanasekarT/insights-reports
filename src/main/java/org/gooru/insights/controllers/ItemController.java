package org.gooru.insights.controllers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.poi.util.IOUtils;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.InsightsOperationConstants;
import org.gooru.insights.security.AuthorizeOperations;
import org.gooru.insights.services.ItemService;
import org.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

@RequestMapping(value ="/query")
@Controller
@EnableAsync
public class ItemController extends BaseController implements APIConstants{
	
	@Autowired
	ItemService itemService;
	
	@RequestMapping(value="/server/status",method = RequestMethod.GET)
	public ModelAndView checkAPiStatus(HttpServletRequest request,HttpServletResponse response){
		ModelAndView model = new ModelAndView();
		model.setViewName("content");
		model.addObject("content","tomcat started");
		return model;
	}

	@RequestMapping(method = { RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations = InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEW)
	public ModelAndView generateQuery(HttpServletRequest request, @RequestParam(value = "data", required = true) String data,
			@RequestParam(value = "sessionToken", required = true) String sessionToken, HttpServletResponse response) throws IOException {

		Map<Integer, String> errorMap = new HashMap<Integer, String>();
		Map<String, Object> dataMap = new HashMap<String, Object>();

		/*
		 * validate API Directly from Gooru API permanently disabled since we
		 * have Redis server support but maintaining for backup.
		 * Map<String,Object> userMap = itemService.getUserObject(sessionToken,
		 * errorMap);
		 */
		Map<String, Object> userMap = itemService.getUserObjectData(sessionToken, errorMap);

		JSONArray jsonArray = itemService.generateQuery(data, dataMap, userMap, errorMap);

		if (!errorMap.isEmpty()) {
			sendError(response, errorMap);
			return null;
		}
		return getModel(jsonArray, dataMap);
	}


	@RequestMapping(value="/export/{reportType}",method ={RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEW)
	public ModelAndView getExportReport(HttpServletRequest request, @PathVariable(value = "reportType") String reportType, @RequestParam(value = "sessionToken", required = true) String sessionToken,@RequestParam(value = "email", required = true) String emailId,
 HttpServletResponse response) throws IOException {
		Map<Integer, String> errorMap = new HashMap<Integer, String>();
		Map<String, Object> dataMap = new HashMap<String, Object>();
		Map<String, String> finalData = new HashMap<String, String>();

		Map<String, Object> userMap = itemService.getUserObjectData(sessionToken, errorMap);
		itemService.getExportReportArray(request, reportType, dataMap, userMap, errorMap, finalData, emailId);
		System.out.print("finalData : " + finalData);
		if (!errorMap.isEmpty()) {
			sendError(response, errorMap);
			return null;
		}

		return getReportModel(finalData);
	}
	
	@RequestMapping(value="/{action}/report",method = RequestMethod.POST)
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEW)
	public ModelAndView manageReports(HttpServletRequest request,@PathVariable(value="action") String action,@RequestParam(value="reportName",required = true) String reportName,@RequestParam(value="sessionToken",required = true) String sessionToken,@RequestBody String data ,HttpServletResponse response) throws IOException{
		
		Map<Integer,String> errorMap = new HashMap<Integer,String>();
		itemService.manageReports(action,reportName,data,errorMap);
		
		return getModels(errorMap);
	}
	
	@RequestMapping(value="/report/{reportType}",method ={RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEW)
	public ModelAndView getPartyReports(HttpServletRequest request,@PathVariable(value="reportType") String reportType,@RequestParam(value="sessionToken",required = true) String sessionToken,HttpServletResponse response) throws IOException{
		
		Map<Integer,String> errorMap = new HashMap<Integer,String>();
		Map<String,Object> dataMap = new HashMap<String,Object>();
	    	   
	    Map<String,Object> userMap = itemService.getUserObjectData(sessionToken, errorMap); 
	    
	    JSONArray jsonArray = itemService.getPartyReport(request,reportType,dataMap,userMap,errorMap);
	     
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

	public void generateExcelOutput(HttpServletResponse response, File excelFile) throws IOException {
		InputStream sheet = new FileInputStream(excelFile);
		response.setContentType("application/xls");
		IOUtils.copy(sheet, response.getOutputStream());
		response.getOutputStream().flush();
		response.getOutputStream().close();
	}
	
	public void generateCSVOutput(HttpServletResponse response, File excelFile) throws IOException {
		InputStream sheet = new FileInputStream(excelFile);
		response.setContentType("application/csv");
		IOUtils.copy(sheet, response.getOutputStream());
		response.getOutputStream().flush();
		response.getOutputStream().close();
	}
}

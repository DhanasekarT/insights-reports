package org.gooru.insights.controllers;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.constants.APIConstants.*;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.InsightsOperationConstants;
import org.gooru.insights.constants.ResponseParamDTO;
import org.gooru.insights.security.AuthorizeOperations;
import org.gooru.insights.services.BaseConnectionServiceImpl;
import org.gooru.insights.services.ItemService;
import org.json.JSONArray;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping(value ="/query")
public class ItemController extends BaseController{
	
	Logger logger = LoggerFactory.getLogger(ItemController.class);
	
	@Autowired
	private ItemService itemService;
	
	/**
	 * 
	 * @param request
	 * @param response
	 * @return
	 */
	@RequestMapping(value = "/server/status", method = RequestMethod.GET)
	public ModelAndView checkAPiStatus(HttpServletRequest request, HttpServletResponse response) {
		return getModel(getItemService().serverStatus());
	}

	/**
	 * This performs Elastic search query operation
	 * @param request HttpServlet Request 
	 * @param data This will hold the client request data
	 * @param sessionToken This is an Gooru sessionToken for validation
	 * @param response HttpServlet Response
	 * @return returns Model object
	 * @throws Exception
	 */
	@RequestMapping(method = { RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations = InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEW)
	public ModelAndView generateQuery(HttpServletRequest request, @RequestParam(value = "data", required = true) String data,
			@RequestParam(value = "sessionToken", required = true) String sessionToken, HttpServletResponse response) throws Exception {

		/**
		 * validate API Directly from Gooru API permanently disabled since we
		 * have Redis server support but maintaining for backup.
		 * Map<String,Object> userMap = itemService.getUserObject(sessionToken,
		 * errorMap);
		 */
		Map<String, Object> userMap = itemService.getUserObjectData(sessionToken);

		return getModel(itemService.generateQuery(data, userMap));
	}
	
	@RequestMapping(value = "/{action}/report", method = RequestMethod.POST)
	@AuthorizeOperations(operations = InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEW)
	public ModelAndView manageReports(HttpServletRequest request, @PathVariable(value = "action") String action, @RequestParam(value = "reportName", required = true) String reportName,
			@RequestParam(value = "sessionToken", required = true) String sessionToken, @RequestBody String data, HttpServletResponse response) throws Exception {

		return getModel(itemService.manageReports(action, reportName, data));
	}
	
	/**
	 * 
	 * @param request
	 * @param reportType
	 * @param sessionToken
	 * @param response
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value = "/report/{reportType}", method = { RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations = InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEW)
	public ModelAndView getPartyReports(HttpServletRequest request, @PathVariable(value = "reportType") String reportType, @RequestParam(value = "sessionToken", required = true) String sessionToken,
			HttpServletResponse response) throws Exception {

		Map<String, Object> userMap = itemService.getUserObjectData(sessionToken);
		return getModel(itemService.getPartyReport(request, reportType, userMap));
	}
	
	/**
	 * 
	 * @param request
	 * @param queryId
	 * @param response
	 * @return
	 */
	@RequestMapping(value="/clear/id",method =RequestMethod.GET)
	public ModelAndView clearRedisCache(HttpServletRequest request,@RequestParam(value="queryId",required = true) String queryId,HttpServletResponse response){
	
		return getModel(itemService.clearQuery(queryId));
	}
	
	/**
	 * 
	 * @param request
	 * @param queryId
	 * @param sessionToken
	 * @param response
	 * @return
	 */
	@RequestMapping(value="/{id}",method =RequestMethod.GET)
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEW)
	public ModelAndView getRedisCache(HttpServletRequest request,@PathVariable("id") String queryId,@RequestParam(value="sessionToken",required = true) String sessionToken,HttpServletResponse response){
		
		Map<String,Object> userMap = itemService.getUserObjectData(sessionToken); 
		return getModel(itemService.getQuery(queryId,userMap));
	}

	/**
	 * 
	 * @param request
	 * @param queryId
	 * @param sessionToken
	 * @param response
	 * @return
	 */
	@RequestMapping(value="/list",method =RequestMethod.GET)
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEW)
	public ModelAndView getRedisCacheList(HttpServletRequest request,@RequestParam(value="queryId",required = false) String queryId,@RequestParam(value="sessionToken",required = true) String sessionToken,HttpServletResponse response){
		
		Map<String,Object> userMap = itemService.getUserObjectData(sessionToken); 
		return getModel(getItemService().getCacheData(queryId,userMap));
	}
	
	/**
	 * 
	 * @param request
	 * @param data
	 * @param response
	 * @return
	 */
	@RequestMapping(value="/keys",method =RequestMethod.PUT)
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEW)
	public ModelAndView putRedisData(HttpServletRequest request,@RequestBody String data ,HttpServletResponse response){
		return getModel(getItemService().insertKey(data));
	}
	
	/**
	 * 
	 * @return
	 */
	@RequestMapping(value="/clear/data",method =RequestMethod.GET)
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEW)
	public ModelAndView clearDataCache(){
		return getModel(getItemService().clearDataCache());
	}
	
	/**
	 * 
	 * @param request
	 * @param data
	 * @param sessionToken
	 * @param response
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value="/combine",method ={RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEW)
	public ModelAndView getItems(HttpServletRequest request,@RequestParam(value="data",required = true) String data,@RequestParam(value="sessionToken",required = true) String sessionToken,HttpServletResponse response) throws Exception{

		Map<String,Object> dataMap = itemService.getUserObjectData(sessionToken); 
		return getModel(getItemService().processApi(data,dataMap));
	}
	
	/**
	 * 
	 * @return
	 */
	@RequestMapping(value="/clear/connection",method =RequestMethod.GET)
	public ModelAndView clearConnectionCache(){
		return getModel(getItemService().clearConnectionCache());
	}

	public ItemService getItemService() {
		return itemService;
	}
}

package org.gooru.insights.controllers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.map.HashedMap;
import org.gooru.insights.services.ItemService;
import org.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

@RequestMapping(value ="/items")
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

	@RequestMapping(value="/search",method ={RequestMethod.GET,RequestMethod.POST})
	public ModelAndView getEventDetail(HttpServletRequest request,@RequestParam(value="data",required = true) String data,HttpServletResponse response) throws IOException{
		Map<Integer,String> errorMap = new HashMap<Integer,String>();
		JSONArray jsonArray = itemService.getEventDetail(data,getMessage(),errorMap);
		sendError(response,errorMap);
		return getModel(jsonArray, getMessage());
//		ModelAndView model = new ModelAndView();
//		model.setViewName("content");
//		model.addObject("content",itemService.getEventDetail(data,getMessage(),errorMap));
//	return model;
	}
}

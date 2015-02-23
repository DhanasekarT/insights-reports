package org.gooru.insights.controllers;

import org.gooru.insights.builders.utils.MessageHandler;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.ResponseParamDTO;
import org.springframework.stereotype.Controller;
import org.springframework.web.servlet.ModelAndView;

import flexjson.JSONSerializer;

@Controller
public class BaseController extends APIConstants{

	
	public <M> ModelAndView getModel(ResponseParamDTO<M> data) {

		ModelAndView model = new ModelAndView(MessageHandler.getMessage(APIConstants.VIEW_NAME));
		model.addObject(MessageHandler.getMessage(APIConstants.RESPONSE_NAME), new JSONSerializer().exclude(EXCLUDE_CLASSES).serialize(data));
		return model;
	}
	
	
}

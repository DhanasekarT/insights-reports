/*******************************************************************************
 * MethodAuthorizationAspect.java
 * insights-read-api
 * Created by Gooru on 2014
 * Copyright (c) 2014 Gooru. All rights reserved.
 * http://www.goorulearning.org/
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
package org.gooru.insights.security;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.gooru.insights.builders.utils.InsightsLogger;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.CassandraConstants;
import org.gooru.insights.constants.ErrorConstants;
import org.gooru.insights.exception.handlers.AccessDeniedException;
import org.gooru.insights.exception.handlers.ReportGenerationException;
import org.gooru.insights.models.User;
import org.gooru.insights.services.BaseCassandraService;
import org.gooru.insights.services.RedisService;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.netflix.astyanax.model.ColumnList;


@Aspect
public class MethodAuthorizationAspect extends OperationAuthorizer {

	private ColumnList<String> endPoint;
	
	private static final String GOORU_PREFIX = "authenticate_";
	
	private ColumnList<String> entityOperationsRole;

	private static final String TRACE_ID = "aspect";
	
	@Autowired
	private RedisService redisService;
	
	@Autowired
	private BaseCassandraService baseCassandraService;
	
	@PostConstruct
	public void init(){
		 endPoint = baseCassandraService.readColumns(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.JOB_CONFIG_SETTINGS.columnFamily(),"gooru.api.rest.endpoint", new ArrayList<String>()).getResult();
		 entityOperationsRole = baseCassandraService.readColumns(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.JOB_CONFIG_SETTINGS.columnFamily(),"entity_role_opertaions", new ArrayList<String>()).getResult();
	}
	
	@Around("accessCheckPointcut() && @annotation(authorizeOperations) && @annotation(requestMapping)")
	public Object operationsAuthorization(ProceedingJoinPoint pjp, AuthorizeOperations authorizeOperations, RequestMapping requestMapping) throws Throwable {

		// Check method access
		boolean permitted = validateApi(authorizeOperations, pjp);
		if (permitted) {
			return pjp.proceed();
		} else {
			throw new AccessDeniedException("Permission Denied! You don't have access");
		}
	}
	
	private boolean validateApi(AuthorizeOperations authorizeOperations, ProceedingJoinPoint pjp){
		Map<String,Boolean> isValid = hasRedisOperations(authorizeOperations,pjp);	
		if(isValid.get("doApi")){
			InsightsLogger.info(TRACE_ID, "doing API request");
			return hasApiOperationsAuthority(authorizeOperations,pjp);
		}
		return isValid.get("proceed");
	}
	
	private boolean hasApiOperationsAuthority(AuthorizeOperations authorizeOperations, ProceedingJoinPoint pjp) {
		
		HttpServletRequest request = null;
		HttpSession session = null;
		String sessionToken = null;
		Map<String,Boolean> validStatus = new HashMap<String,Boolean>();
		validStatus.put("proceed", false);
		if (RequestContextHolder.getRequestAttributes() != null) {
		request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
		session = request.getSession(true);
		}
				if(request.getParameter("sessionToken") != null && ! request.getParameter("sessionToken").isEmpty()){
					sessionToken = request.getParameter("sessionToken");					
					String address = endPoint.getColumnByName("constant_value").getStringValue()+"/v2/user/token/"+ sessionToken + "?sessionToken=" + sessionToken;
					ClientResource client = new ClientResource(address);
					if (client.getStatus().isSuccess()) {
						try{
							Representation representation = client.get();
							JsonRepresentation jsonRepresentation = new JsonRepresentation(
									representation);
							JSONObject jsonObj = jsonRepresentation.getJsonObject();
							User user = new User();
							user.setFirstName(jsonObj.getString("firstName"));
							user.setLastName(jsonObj.getString("lastName"));
							user.setEmailId(jsonObj.getString("emailId"));
							user.setGooruUId(jsonObj.getString("gooruUId"));
							if(hasGooruAdminAuthority(authorizeOperations, jsonObj)){
								session.setAttribute("token", sessionToken);
								return true;
							}
							 if(hasAuthority(authorizeOperations, jsonObj)){
								 session.setAttribute("token", sessionToken);
								 return true;
							 }
						}catch(Exception e){
							throw new AccessDeniedException("Invalid sessionToken!");
						}
					}else{
						throw new AccessDeniedException("Invalid sessionToken!");
					}
				}else{
					InsightsLogger.debug(TRACE_ID, "session token is null");
					throw new AccessDeniedException("sessionToken can not be NULL!");
				}
				return false;
		}

	public Map<String,Boolean> hasRedisOperations(AuthorizeOperations authorizeOperations, ProceedingJoinPoint pjp) {
	
		HttpServletRequest request = null;
		HttpSession session = null;
		String sessionToken = null;
		Map<String,Boolean> validStatus = new HashMap<String,Boolean>();
		validStatus.put("proceed", false);
		validStatus.put("doApi", false);
		if (RequestContextHolder.getRequestAttributes() != null) {
		request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
		session = request.getSession(true);
		}
		if(request.getParameter("sessionToken") != null && ! request.getParameter("sessionToken").isEmpty()){
			sessionToken = request.getParameter("sessionToken");					

			try{
			String result = redisService.getDirectValue(GOORU_PREFIX+sessionToken);
			if(result == null || result.isEmpty()){
				InsightsLogger.error(TRACE_ID, "null value in redis data for "+GOORU_PREFIX+sessionToken);
				validStatus.put("doApi", true);
				return validStatus;
			}
				JSONObject jsonObject = new JSONObject(result);
				jsonObject = new JSONObject(jsonObject.getString("userToken"));
				jsonObject = new JSONObject(jsonObject.getString("user"));
				User user = new User();
				user.setFirstName(jsonObject.getString("firstName"));
				user.setLastName(jsonObject.getString("lastName"));
				user.setEmailId(jsonObject.getString("emailId"));
				user.setGooruUId(jsonObject.getString("partyUid"));
				if(hasGooruAdminAuthority(authorizeOperations, jsonObject)){
					session.setAttribute("sessionToken", sessionToken);
					validStatus.put("proceed", true);
					return validStatus;
				}
				 if(hasAuthority(authorizeOperations, jsonObject)){
					 session.setAttribute("sessionToken", sessionToken);
					 validStatus.put("proceed", true);
					 return validStatus;
				 }
				} catch (Exception e) {
					InsightsLogger.error(TRACE_ID, "Exception from redis:"+e.getMessage());
					validStatus.put("doApi", true);
					return validStatus;
				}
	}else{
		throw new AccessDeniedException("sessionToken can not be NULL!");
	}
		return validStatus;
	}
	
	@Pointcut("execution(* org.gooru.insights.controllers.*.*(..))")
	public void accessCheckPointcut() {
	}
	
	public boolean hasOperationsAuthority(AuthorizeOperations authorizeOperations, ProceedingJoinPoint pjp) {
		
		HttpServletRequest request = null;
		HttpSession session = null;
		String sessionToken = null;
		
		if (RequestContextHolder.getRequestAttributes() != null) {
		request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
		session = request.getSession(true);
		}
		if(session.getAttribute("token") != null && !session.getAttribute("token").toString().isEmpty()){
			if(request.getParameter("sessionToken") != null){
				sessionToken = (String) session.getAttribute("token");
					if(sessionToken.equalsIgnoreCase(request.getParameter("sessionToken"))) {
						return true;
					}
					else{
						if(request.getParameter("sessionToken") != null && ! request.getParameter("sessionToken").isEmpty()){
							sessionToken = request.getParameter("sessionToken");					
							String address = endPoint.getColumnByName("constant_value").getStringValue()+"/v2/user/token/"+ sessionToken + "?sessionToken=" + sessionToken;
							ClientResource client = new ClientResource(address);
							if (client.getStatus().isSuccess()) {
								try{
									Representation representation = client.get();
									JsonRepresentation jsonRepresentation = new JsonRepresentation(
											representation);
									JSONObject jsonObj = jsonRepresentation.getJsonObject();
									User user = new User();
									user.setFirstName(jsonObj.getString("firstName"));
									user.setLastName(jsonObj.getString("lastName"));
									user.setEmailId(jsonObj.getString("emailId"));
									user.setGooruUId(jsonObj.getString("gooruUId"));
									if(hasGooruAdminAuthority(authorizeOperations, jsonObj)){
										session.setAttribute("token", sessionToken);
										 return true;
									}
									 if(hasAuthority(authorizeOperations, jsonObj)){
										 session.setAttribute("token", sessionToken);
										 return true;
									 }else{
										 return false;
									 }
								}catch(Exception e){
									throw new AccessDeniedException("Invalid sessionToken!");
								}
							}else{
								throw new AccessDeniedException("Invalid sessionToken!");
							}
						}else{
							throw new AccessDeniedException("sessionToken can not be NULL!");
						}
				}
			}else{
				throw new AccessDeniedException("sessionToken can not be NULL!");
			}
		} else {
				if(request.getParameter("sessionToken") != null && ! request.getParameter("sessionToken").isEmpty()){
					sessionToken = request.getParameter("sessionToken");					
					String address = endPoint.getColumnByName("constant_value").getStringValue()+"/v2/user/token/"+ sessionToken + "?sessionToken=" + sessionToken;
					ClientResource client = new ClientResource(address);
					if (client.getStatus().isSuccess()) {
						try{
							Representation representation = client.get();
							JsonRepresentation jsonRepresentation = new JsonRepresentation(
									representation);
							JSONObject jsonObj = jsonRepresentation.getJsonObject();
							User user = new User();
							user.setFirstName(jsonObj.getString("firstName"));
							user.setLastName(jsonObj.getString("lastName"));
							user.setEmailId(jsonObj.getString("emailId"));
							user.setGooruUId(jsonObj.getString("gooruUId"));
							if(hasGooruAdminAuthority(authorizeOperations, jsonObj)){
								session.setAttribute("token", sessionToken);
								 return true;
							}
							 if(hasAuthority(authorizeOperations, jsonObj)){
								 session.setAttribute("token", sessionToken);
								 return true;
							 }else{
								 return false;
							 }
						}catch(Exception e){
							throw new AccessDeniedException("Invalid sessionToken!");
						}
					}else{
						throw new AccessDeniedException("Invalid sessionToken!");
					}
				}else{
					throw new AccessDeniedException("sessionToken can not be NULL!");
				}
		}
	}
	
	private boolean hasGooruAdminAuthority(AuthorizeOperations authorizeOperations, JSONObject jsonObj) {
		boolean roleAuthority = false;
		try {
			String userRole = jsonObj.getString("userRoleSetString");
			String operations = authorizeOperations.operations();
			for (String op : operations.split(",")) {
				if (userRole.contains("ROLE_GOORU_ADMIN") || userRole.contains("Content_Admin") || userRole.contains("Organization_Admin")) {
					roleAuthority = true;
				}
			}
		} catch (JSONException e) {
			throw new ReportGenerationException(ErrorConstants.INVALID_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.JSON) + e);
		}
		return roleAuthority;

	}
	
	private boolean hasAuthority(AuthorizeOperations authorizeOperations, JSONObject jsonObj) {
		String userRole = null;
		try {
			userRole = jsonObj.getString("userRoleSetString");
		} catch (JSONException e) {
			throw new ReportGenerationException(ErrorConstants.INVALID_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.JSON) + e);
		}
		String operations = authorizeOperations.operations();
		for (String op : operations.split(APIConstants.COMMA)) {
			String roles = entityOperationsRole.getColumnByName(op).getStringValue();
			InsightsLogger.info(TRACE_ID, APIConstants.ROLES + roles + APIConstants.COLON + userRole.contains(roles));
			for (String role : roles.split(",")) {
				if ((userRole.contains(role))) {
					return true;
				}
			}
		}

		return false;

	}
	
	public static void main(String args[]){
		String address = "http://qa.goorulearning.org/gooruapi/rest/v2/user/token/af9b01f2-0e63-11e4-94c4-12313924c4da?sessionToken=af9b01f2-0e63-11e4-94c4-12313924c4da";
		ClientResource client = new ClientResource(address);
		if (client.getStatus().isSuccess()) {
			try{
				Representation representation = client.get();
				JsonRepresentation jsonRepresentation = new JsonRepresentation(
						representation);
				JSONObject jsonObj = jsonRepresentation.getJsonObject();
				
			}catch(Exception e){
				throw new AccessDeniedException("Invalid sessionToken!");
			}
		}
		
	}
}

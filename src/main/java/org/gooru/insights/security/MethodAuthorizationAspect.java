/*******************************************************************************
 * MethodAuthorizationAspect.java
 * insights-read-api
 * Created by Gooru on 2014
 * Copyright (c) 2015 Gooru. All rights reserved.
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
import org.gooru.insights.builders.utils.MessageHandler;
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
import org.restlet.data.Form;
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
	
	private ColumnList<String> entityOperationsRole;
	
	private static final String GOORU_PREFIX = "authenticate_";

	private static final String ASPECT = "aspect";
	
	private static final String DO_API = "doApi";
	
	private static final String REST_HEADER = "org.restlet.http.headers";
	
	private static final String ENTITY_ROLE = "entity_role_opertaions";
	
	private static final String GOORU_REST_ENDPOINT = "gooru.api.rest.endpoint";
	
	private static final String USER_ROLE_SET_STRING = "userRoleSetString";
	
	private static final String ROLE_GOORU_ADMIN = "ROLE_GOORU_ADMIN";
	
	private static final String CONTENT_ADMIN = "Content_Admin";
	
	private static final String ORGANIZATION_ADMIN = "Organization_Admin";
	
	@Autowired
	private RedisService redisService;
	
	@Autowired
	private BaseCassandraService baseCassandraService;
	
	@PostConstruct
	public void init(){
		 endPoint = baseCassandraService.readColumns(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.JOB_CONFIG_SETTINGS.columnFamily(),GOORU_REST_ENDPOINT, new ArrayList<String>()).getResult();
		 entityOperationsRole = baseCassandraService.readColumns(CassandraConstants.Keyspaces.INSIGHTS.keyspace(), CassandraConstants.ColumnFamilies.JOB_CONFIG_SETTINGS.columnFamily(),ENTITY_ROLE, new ArrayList<String>()).getResult();
	}
	
	@Around("accessCheckPointcut() && @annotation(authorizeOperations) && @annotation(requestMapping)")
	public Object operationsAuthorization(ProceedingJoinPoint pjp, AuthorizeOperations authorizeOperations, RequestMapping requestMapping) throws Throwable {

		// Check method access
		boolean permitted = validateApi(authorizeOperations, pjp);
		if (permitted) {
			return pjp.proceed();
		} else {
			throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E110));
		}
	}
	
	private boolean validateApi(AuthorizeOperations authorizeOperations, ProceedingJoinPoint pjp){
		
		Map<String,Boolean> isValid = hasRedisOperations(authorizeOperations,pjp);	
		if(isValid.get(DO_API)){
			InsightsLogger.info(ASPECT, APIConstants.API_REQUEST);
			return hasApiOperationsAuthority(authorizeOperations,pjp);
		}
		return isValid.get(APIConstants.PROCEED);
	}
	
	private boolean hasApiOperationsAuthority(AuthorizeOperations authorizeOperations, ProceedingJoinPoint pjp) {
		
		HttpServletRequest request = null;
		HttpSession session = null;
		String sessionToken = null;
		Map<String,Boolean> validStatus = new HashMap<String,Boolean>();
		validStatus.put(APIConstants.PROCEED, false);
		if (RequestContextHolder.getRequestAttributes() != null) {
		request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
		session = request.getSession(true);
		}
		sessionToken = getSessionToken(request);
				if(sessionToken != null && !sessionToken.isEmpty()){
					String address = endPoint.getColumnByName(APIConstants.CONSTANT_VALUE).getStringValue()+APIConstants.GOORU_URI+ sessionToken;
					ClientResource client = new ClientResource(address);
					Form headers = (Form)client.getRequestAttributes().get(REST_HEADER);
					if (headers == null) {
					    headers = new Form();
					}
					    headers.add(APIConstants.GOORU_SESSION_TOKEN, sessionToken);
					    client.getRequestAttributes().put(REST_HEADER, headers);
					if (client.getStatus().isSuccess()) {
						try{
							Representation representation = client.get();
							JsonRepresentation jsonRepresentation = new JsonRepresentation(
									representation);
							JSONObject jsonObj = jsonRepresentation.getJsonObject();
							User user = new User();
							user.setFirstName(jsonObj.getString(APIConstants.FIRST_NAME));
							user.setLastName(jsonObj.getString(APIConstants.LAST_NAME));
							user.setEmailId(jsonObj.getString(APIConstants.EMAIL_ID));
							user.setGooruUId(jsonObj.getString(APIConstants.GOORUUID));
							if(hasGooruAdminAuthority(authorizeOperations, jsonObj)){
								session.setAttribute(APIConstants.TOKEN, sessionToken);
								return true;
							}
							 if(hasAuthority(authorizeOperations, jsonObj)){
								 session.setAttribute(APIConstants.TOKEN, sessionToken);
								 return true;
							 }
						}catch(Exception e){
							throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E102, new String[]{APIConstants.SESSION_TOKEN}));
						}
					}else{
						throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E102, new String[]{APIConstants.SESSION_TOKEN}));
					}
				}else{
					InsightsLogger.debug(ASPECT, MessageHandler.getMessage(ErrorConstants.E100, new String[]{APIConstants.SESSION_TOKEN}));
					throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E100, new String[]{APIConstants.SESSION_TOKEN}));
				}
				return false;
		}

	public Map<String,Boolean> hasRedisOperations(AuthorizeOperations authorizeOperations, ProceedingJoinPoint pjp) {
	
		HttpServletRequest request = null;
		HttpSession session = null;
		String sessionToken = null;
		Map<String,Boolean> validStatus = new HashMap<String,Boolean>();
		validStatus.put(APIConstants.PROCEED, false);
		validStatus.put(DO_API, false);
		if (RequestContextHolder.getRequestAttributes() != null) {
		request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
		session = request.getSession(true);
		}
		sessionToken = getSessionToken(request);
		if(sessionToken != null && !sessionToken.isEmpty()){
			try{
			String result = redisService.getDirectValue(GOORU_PREFIX+sessionToken);
			if(result == null || result.isEmpty()){
				InsightsLogger.error(ASPECT, ErrorConstants.IS_NULL.replace(ErrorConstants.REPLACER, ErrorConstants.REDIS_DATA)+GOORU_PREFIX+sessionToken);
				validStatus.put(DO_API, true);
				return validStatus;
			}
				JSONObject jsonObject = new JSONObject(result);
				jsonObject = new JSONObject(jsonObject.getString(APIConstants.USER_TOKEN));
				jsonObject = new JSONObject(jsonObject.getString(APIConstants.USER));
				User user = new User();
				user.setFirstName(jsonObject.getString(APIConstants.FIRST_NAME));
				user.setLastName(jsonObject.getString(APIConstants.LAST_NAME));
				user.setEmailId(jsonObject.getJSONArray(APIConstants.IDENTITIES).getJSONObject(0).getString(APIConstants.EXTERNAL_ID));
				user.setGooruUId(jsonObject.getString(APIConstants.PARTY_UID));
				if(hasGooruAdminAuthority(authorizeOperations, jsonObject)){
					session.setAttribute(APIConstants.SESSION_TOKEN, sessionToken);
					validStatus.put(APIConstants.PROCEED, true);
					return validStatus;
				}
				 if(hasAuthority(authorizeOperations, jsonObject)){
					 session.setAttribute(APIConstants.SESSION_TOKEN, sessionToken);
					 validStatus.put(APIConstants.PROCEED, true);
					 return validStatus;
				 }
				} catch (Exception e) {
					InsightsLogger.error(ASPECT, ErrorConstants.EXCEPTION_IN.replace(ErrorConstants.REPLACER, ErrorConstants.REDIS),e);
					validStatus.put(DO_API, true);
					return validStatus;
				}
	}else{
		throw new AccessDeniedException(MessageHandler.getMessage(ErrorConstants.E102, new String[]{APIConstants.SESSION_TOKEN}));
	}
		return validStatus;
	}
	
	@Pointcut("execution(* org.gooru.insights.controllers.*.*(..))")
	public void accessCheckPointcut() {
	}
	
	private boolean hasGooruAdminAuthority(AuthorizeOperations authorizeOperations, JSONObject jsonObj) {
		
		boolean roleAuthority = false;
		try {
			String userRole = jsonObj.getString(USER_ROLE_SET_STRING);
			String operations = authorizeOperations.operations();
			for (String op : operations.split(APIConstants.COMMA)) {
				if (userRole.contains(ROLE_GOORU_ADMIN) || userRole.contains(CONTENT_ADMIN) || userRole.contains(ORGANIZATION_ADMIN)) {
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
			userRole = jsonObj.getString(USER_ROLE_SET_STRING);
		} catch (JSONException e) {
			throw new ReportGenerationException(ErrorConstants.INVALID_ERROR.replace(ErrorConstants.REPLACER, ErrorConstants.JSON) + e);
		}
		String operations = authorizeOperations.operations();
		for (String op : operations.split(APIConstants.COMMA)) {
			String roles = entityOperationsRole.getColumnByName(op).getStringValue();
			InsightsLogger.info(ASPECT, APIConstants.ROLES + roles + APIConstants.COLON + userRole.contains(roles));
			for (String role : roles.split(APIConstants.COMMA)) {
				if ((userRole.contains(role))) {
					return true;
				}
			}
		}

		return false;

	}
	
	private String getSessionToken(HttpServletRequest request) {

		if (request.getHeader(APIConstants.GOORU_SESSION_TOKEN) != null) {
			return request.getHeader(APIConstants.GOORU_SESSION_TOKEN);
		} else {
			return request.getParameter(APIConstants.SESSION_TOKEN);
		}
	}
}

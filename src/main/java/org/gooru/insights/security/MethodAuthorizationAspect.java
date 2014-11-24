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
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.gooru.insights.constants.CassandraConstants.columnFamilies;
import org.gooru.insights.constants.CassandraConstants.keyspaces;
import org.gooru.insights.models.User;
import org.gooru.insights.services.BaseCassandraService;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.netflix.astyanax.model.ColumnList;


@Aspect
public class MethodAuthorizationAspect extends OperationAuthorizer {

	@Resource(name = "gooruConstants")
	private Properties gooruConstants;

//	@Autowired
//	CassandraService cassandraService;
	
	private ColumnList<String> endPoint = null;
	
	private ColumnList<String> entityOperationsRole = null;
	
	@Autowired
	private BaseCassandraService baseCassandraService;
	
	@PostConstruct
	public void init(){
		 endPoint = baseCassandraService.readColumns(keyspaces.INSIGHTS.keyspace(), columnFamilies.JOB_CONFIG_SETTINGS.columnFamily(),"gooru.api.rest.endpoint", new ArrayList<String>()).getResult();
		 entityOperationsRole = baseCassandraService.readColumns(keyspaces.INSIGHTS.keyspace(), columnFamilies.JOB_CONFIG_SETTINGS.columnFamily(),"entity_role_opertaions", new ArrayList<String>()).getResult();
//		 endPoint = cassandraService.getDashBoardKeys("gooru.api.rest.endpoint");
//		 entityOperationsRole = cassandraService.getDashBoardKeys("entity_role_opertaions");
	}
	@Around("accessCheckPointcut() && @annotation(authorizeOperations) && @annotation(requestMapping)")
	public Object operationsAuthorization(ProceedingJoinPoint pjp, AuthorizeOperations authorizeOperations, RequestMapping requestMapping) throws Throwable {

		// Check method access
		boolean permitted = hasOperationsAuthority(authorizeOperations, pjp);
		if (permitted) {
			return pjp.proceed();
		} else {
			throw new AccessDeniedException("Permission Denied! You don't have access");
		}
	}

	@Pointcut("execution(* org.gooru.insights.api.controllers.*.*(..))")
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
	
	private boolean hasGooruAdminAuthority(AuthorizeOperations authorizeOperations,JSONObject jsonObj){
		boolean roleAuthority = false;
		try {
			String userRole = jsonObj.getString("userRoleSetString");
			String operations =  authorizeOperations.operations();
			for(String op :operations.split(",")){
				if(userRole.contains("ROLE_GOORU_ADMIN") || userRole.contains("Content_Admin") || userRole.contains("Organization_Admin")){
					roleAuthority = true;
				}
			}
		} catch (JSONException e) {
			System.out.print("Exception while JSON get"+e);
		}
		return roleAuthority;
		
	}
	
	private boolean hasAuthority(AuthorizeOperations authorizeOperations,JSONObject jsonObj){
			String userRole = null;
			try {
				userRole = jsonObj.getString("userRoleSetString");
			} catch (JSONException e) {
				System.out.print("Exception while json parsing"+ e);
				return  false;
			}
			String operations =  authorizeOperations.operations();
			for(String op : operations.split(",")){
				String roles = entityOperationsRole.getColumnByName(op).getStringValue();
				System.out.print("role : " +roles + "roleAuthority > :"+userRole.contains(roles));
				for(String role : roles.split(",")){
					if((userRole.contains(role))){
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

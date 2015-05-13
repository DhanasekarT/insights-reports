/*******************************************************************************
 * OperationAuthorizer.java
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

import org.gooru.insights.constants.InsightsOperationConstants;
import org.gooru.insights.models.RoleEntityOperation;
import org.gooru.insights.models.User;
import org.gooru.insights.models.UserRoleAssoc;
import org.gooru.insights.services.BaseAPIServiceImpl;
import org.springframework.stereotype.Component;

@Component
public class OperationAuthorizer {

	boolean hasRole(Short userRoleId, User user) {
		
		if (user != null && user.getUserRoleSet() != null) {
			for (UserRoleAssoc userRoleAssoc : user.getUserRoleSet()) {
				if (userRoleAssoc.getRole().getRoleId().equals(userRoleId)) {
					return true;
				}
			}
		}
		return false;
	}

	public boolean hasAuthorization(String operation, User user) {
		
		if (user != null && operation != null && user.getUserRoleSet() != null) {
			for (UserRoleAssoc userRoleAssoc : user.getUserRoleSet()) {
				if (userRoleAssoc.getRole().getRoleOperations() == null) {
					break;
				}
				for (RoleEntityOperation entityOperation : userRoleAssoc
						.getRole().getRoleOperations()) {
					if ((BaseAPIServiceImpl.buildString(new String[]{entityOperation.getEntityOperation().getEntityName(),InsightsOperationConstants.ENTITY_ACTION_SEPARATOR,entityOperation
							.getEntityOperation().getOperationName()}).equals(operation))) {
						return true;
					}
				}
			}
		}
		return false;
	}
}
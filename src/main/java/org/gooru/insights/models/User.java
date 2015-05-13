package org.gooru.insights.models;

import java.io.Serializable;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;

import org.apache.commons.lang.StringUtils;

@Entity(name="user")
public class User implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8293909847220631830L;
	
	private static final String TYPE = "user";
	
	@Column
	private Integer userId;
	
	@Column
	private String gooruUId;
	
	@Column
	private String firstName;
	
	@Column
	private String lastName;
	
	@Column
	private String username;
	
	@Column
	private String emailId = "";
	
	@Column
	private Integer confirmStatus;
	private String registerToken;
	private UserRole userRole;
	private Set<UserRoleAssoc> userRoleSet;
	private String userRoleSetString;
	private String referenceUid;
	
	

	private String importCode;
	private Integer addedBySystem;

	private User parentUser;
	private Integer accountTypeId;

	private String profileImageUrl;

	
	
	private Integer viewFlag;
	
	private String token;
    
	private Boolean isDeleted;
	


	public String getEmailId() {
		return emailId;
	}

	public void setEmailId(String emailId) {
		this.emailId = emailId;
	}

	public Integer getUserId() {
		return userId;
	}

	public void setUserId(Integer userId) {
		this.userId = userId;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public UserRole getUserRole() {
		return userRole;
	}

	public void setUserRole(UserRole userRole) {
		this.userRole = userRole;
	}

	public Set<UserRoleAssoc> getUserRoleSet() {
		return userRoleSet;
	}

	public void setUserRoleSet(Set<UserRoleAssoc> userRoleSet) {
		this.userRoleSet = userRoleSet;

		userRoleSetString = "";
		if (userRoleSet != null) {
			for (UserRoleAssoc userRoleAssoc : userRoleSet) {
				if (!userRoleSetString.isEmpty()) {
					userRoleSetString += ",";
				}
				userRoleSetString += userRoleAssoc.getRole().getName();
			}
		}
	}

	public String getUserRoleSetString() {
		return this.userRoleSetString;
	}
	
	public void setUsername(String username) {
		this.username = username;
	}

	public String getUsername() {
		return username;
	}

	public String getUsernameDisplay() {
		String usernameDisplay = username;
		if (username == null || username.isEmpty()) {
			String firstName = "";
			if (this.getFirstName() != null) {
				firstName = this.getFirstName();
				firstName = StringUtils.remove(firstName, " ");
			}
			String lastName = "";
			if (this.getLastName() != null) {
				lastName = this.getLastName();
			}

			usernameDisplay = firstName;
			if (lastName.length() > 0) {
				usernameDisplay = usernameDisplay + lastName.substring(0, 1);
			}
			if (usernameDisplay.length() > 20) {
				usernameDisplay = usernameDisplay.substring(0, 20);
			}
		}
		return usernameDisplay;
	}

	public void setRegisterToken(String registerToken) {
		this.registerToken = registerToken;
	}

	public String getRegisterToken() {
		return registerToken;
	}

	public void setConfirmStatus(Integer confirmStatus) {
		this.confirmStatus = confirmStatus;
	}

	public Integer getConfirmStatus() {
		return confirmStatus;
	}

	public String getImportCode() {
		return importCode;
	}

	public void setImportCode(String importCode) {
		this.importCode = importCode;
	}

	public Integer getAddedBySystem() {
		return addedBySystem;
	}

	public void setAddedBySystem(Integer addedBySystem) {
		this.addedBySystem = addedBySystem;
	}

	public Integer getAccountTypeId() {
		return accountTypeId;
	}

	public void setAccountTypeId(Integer accountTypeId) {
		this.accountTypeId = accountTypeId;
	}

	public User getParentUser() {
		return parentUser;
	}

	public void setParentUser(User parentUser) {
		this.parentUser = parentUser;
	}

	public String getProfileImageUrl() {
		return profileImageUrl;
	}

	public void setProfileImageUrl(String profileImageUrl) {
		this.profileImageUrl = profileImageUrl;
	}


	public void setViewFlag(Integer viewFlag) {
		this.viewFlag = viewFlag;
	}

	public Integer getViewFlag() {
		return viewFlag;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public void setIsDeleted(Boolean isDeleted) {
		if(isDeleted == null){
			isDeleted = false;
		}
		this.isDeleted = isDeleted;
	}

	public Boolean getIsDeleted() {
		return isDeleted;
	}

	public String getReferenceUid() {
		return referenceUid;
	}

	public void setReferenceUid(String referenceUid) {
		this.referenceUid = referenceUid;
	}

	public String getGooruUId() {
		return gooruUId;
	}

	public void setGooruUId(String gooruUId) {
		this.gooruUId = gooruUId;
	}
	
}


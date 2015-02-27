package org.gooru.insights.services;

public interface MailService {

	public void sendMail(String to, String subject, String body, String file);
   
    public void sendPreConfiguredMail(String message); 
    
    public void checkSendType(String to, String subject, String body, String file);

}

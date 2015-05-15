package org.gooru.insights.services;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.MailParseException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

@Service
public class MailServiceImpl implements MailService {

	@Autowired
	private JavaMailSender mailSender;

	@Autowired
	private SimpleMailMessage preConfiguredMessage;
	
	@Autowired
	private BaseConnectionService baseConnectionService;

	
	public BaseConnectionService getBaseConnectionService() {
		return baseConnectionService;
	}
	
	public SimpleMailMessage getSimpleMailMessage() {
		return preConfiguredMessage;
	}
	
	public JavaMailSender getJavaMailSender() {
		return mailSender;
	}
	/**
	 * This method will send compose and send the message
	 * */
	public void sendMail(String to, String subject, String body, String fileLink) {
		
		MimeMessage message = getJavaMailSender().createMimeMessage();
		try{
			MimeMessageHelper helper = new MimeMessageHelper(message, true);
			helper.setTo(to);
			helper.setSubject(subject);
			helper.setText("Hi,"+body);
			helper.setReplyTo(getBaseConnectionService().getDefaultReplyToEmail());
			helper.setText(body,body+"<html><a href=\""+fileLink+"\">here. </a></html><BR> This is download link will expire in 24 hours. <BR><BR>Best Regards,<BR>Insights Team.");
			getJavaMailSender().send(message);
		}catch (MessagingException e) {
			throw new MailParseException(e);
	     }

	}

	/**
	 * This method will send compose and send the message
	 * */
	public void sendMail(String to, String subject, String body) {
		
		MimeMessage message = getJavaMailSender().createMimeMessage();
		try{
			MimeMessageHelper helper = new MimeMessageHelper(message, true);
			helper.setTo(to);
			helper.setSubject(subject);
			helper.setText("Hi,"+body);
			helper.setReplyTo(getBaseConnectionService().getDefaultReplyToEmail());
			getJavaMailSender().send(message);
		}catch (MessagingException e) {
			throw new MailParseException(e);
	     }

	}
	public void checkSendType(String to, String subject, String body, String file){
		
		if(to != null && (!to.isEmpty())){
			this.sendMail(to, subject, body, file);
		}else{
			this.sendDefaultMail(subject, body, file);
		}
	}

	/**
	 * This method will send a pre-configured message
	 * */
	public void sendPreConfiguredMail(String message) {
		
		SimpleMailMessage mailMessage = new SimpleMailMessage(getSimpleMailMessage());
		mailMessage.setText(message);
		getJavaMailSender().send(mailMessage);

	}
	
	/** this method will send to default mail id which is insights@goorulearning.org*/
	public void sendDefaultMail(String subject,String body,String fileLink){
		sendMail(getBaseConnectionService().getDefaultToEmail(), subject, body, fileLink);
	}

}

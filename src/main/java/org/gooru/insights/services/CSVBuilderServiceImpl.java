package org.gooru.insights.services;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Resource;

import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class CSVBuilderServiceImpl implements CSVBuilderService{

	@Resource(name = "filePath")
	private Properties filePath;

	
	@Override
	public String generateCSV(String startDate, String endDate,
			Integer partnerIpdId, List<Map<String, String>> resultSet,
			String fileName) throws ParseException, IOException {
		
		boolean headerColumns = false;

		// Set output File
		File csvfile = new File(setFilePath(fileName));
		@SuppressWarnings("resource")
		PrintStream stream = new PrintStream(csvfile);
		
		//print header row
		

		//print row values
		for (Map<String, String> map : resultSet) {
		
			if (!headerColumns) {
				for (Map.Entry<String, String> entry : map.entrySet()) {
					stream.print(entry.getKey() + ";");
					headerColumns = true;
				}
				// print new line
				stream.println("");
		}
			for (Map.Entry<String, String> entry : map.entrySet()) {
				stream.print(entry.getValue() + ";");
			}
			//print new line
			stream.println("");
		}
		
		return getFilePath(fileName);
	}
///instance download so used exact link
	public File generateCSVReport(List<Map<String,Object>> resultSet,String fileName)throws ParseException, IOException{
	
		boolean headerColumns = false;

		// Set output File
		File csvfile = new File(setFilePath(fileName));
		@SuppressWarnings("resource")
		PrintStream stream = new PrintStream(csvfile);
		
		//print header row
		

		//print row values
		for (Map<String, Object> map : resultSet) {
		
			if (!headerColumns) {
				for (Map.Entry<String, Object> entry : map.entrySet()) {
					stream.print(entry.getKey() + "|");
					headerColumns = true;
				}
				// print new line
				stream.println("");
		}
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				stream.print(entry.getValue() + "|");
			}
			//print new line
			stream.println("");
		}
		
		return csvfile;
	}
	
	public String generateCSVMapReport(List<Map<String,Object>> resultSet,String fileName)throws ParseException, IOException{
		
		// Set output File
		File csvfile = new File(setFilePath(fileName));
		@SuppressWarnings("resource")
		PrintStream stream = new PrintStream(csvfile);
		
		//print row values
		ObjectMapper objectMapper = new ObjectMapper(); 
		
		for (Map<String, Object> map : resultSet) {
			stream.print(objectMapper.writeValueAsString(map) + "|");
			stream.println("");
		}
		
		return getFilePath(fileName);
	}

	public String generateCSVJSONReport(JSONArray resultSet,String fileName)throws ParseException, IOException{
		
		// Set output File
		File csvfile = new File(setFilePath(fileName));
		@SuppressWarnings("resource")
		PrintStream stream = new PrintStream(csvfile);
		
		//print row values
		ObjectMapper objectMapper = new ObjectMapper(); 
		
		for (int i=0; i<resultSet.length(); i++) {
		    JSONObject item;
			try {
				item = resultSet.getJSONObject(i);
				stream.print(objectMapper.writeValueAsString(item.toString()) + "|");
				stream.println("");
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}		
		return getFilePath(fileName);
	}
	public Properties getFilePath() {
		return filePath;
	}

	public void setFilePath(Properties filePath) {
		this.filePath = filePath;
	}
	
	public String setFilePath(String file){
		
		String fileName = this.getFilePath().getProperty("insights.file.real.path");
		
		if(file != null && (!file.isEmpty())){
			fileName += file;
		
		}else{
			fileName +="insights";
		}
		return fileName;
	}

	public String getFilePath(String file){
		
		//String fileName = this.getFilePath().getProperty("insights.file.app.path");
		String fileName = "/tmp/";
		if(file != null && (!file.isEmpty())){
			fileName += file;
		
		}else{
			fileName +="insights";
		}
		return fileName;
	}

	public void removeExpiredFiles(){
		Date d = new Date();
		File directory = new File(this.getFilePath().getProperty("insights.file.real.path"));
		File[] fList = directory.listFiles();
		for (File file : fList){
			if (file.isFile()){
			long diff = d.getTime() - file.lastModified();
			long diffHours = (diff / (60 * 60 * 1000) % 24);
			//long diffDays = (diff / (60 * 60 * 1000));
				if(diffHours > 24){
					System.out.print(file.getName() + " is deleted");
					file.delete();
				}	
			}
		}
	}
	
}

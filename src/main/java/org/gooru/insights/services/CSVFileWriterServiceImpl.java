package org.gooru.insights.services;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.builders.utils.DateTime;
import org.gooru.insights.builders.utils.InsightsLogger;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.CassandraConstants.CassandraRowKeys;
import org.gooru.insights.constants.ErrorConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CSVFileWriterServiceImpl implements CSVFileWriterService{
	
	@Autowired
	private BaseConnectionService baseConnectionService;
	
	private static final Logger logger = LoggerFactory.getLogger(CSVFileWriterServiceImpl.class);
	
	private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'kk:mm:ss.SSS'Z'");
	
	private static final SimpleDateFormat dateFormatterForExport = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss a");
	
	public BaseConnectionService getBaseConnectionService() {
		return baseConnectionService;
	}
	
	public void generateCSVReport(String traceId, List<String> headerKeys, List<Map<String, Object>> rowList, String fileAbsolutePath, String delimiter, Boolean isNewFile) throws FileNotFoundException {

		if(StringUtils.isBlank(delimiter)) {
			delimiter = APIConstants.COMMA;
		}
		PrintStream stream = null;
		try {
			stream = new PrintStream(new BufferedOutputStream(new FileOutputStream(new File(fileAbsolutePath), true)));
			
			if (isNewFile) {
				// print header row
				Iterator<String> itr = headerKeys.iterator();
				String header = null;
				while(itr.hasNext()) {
					header = itr.next();
					header = getBaseConnectionService().getColumnListFromCache(CassandraRowKeys.EXPORT_FIELDS.CassandraRowKey()).getStringValue(header, header);
					header = appendDQ(header).toString();
					stream.print(header);
					if(itr.hasNext()) {
						stream.print(delimiter);
					}
				}
				// print new line
				stream.println(APIConstants.EMPTY);
			}
			
			StringBuilder rowLine = new StringBuilder(); 
			for (Map<String, Object> row : rowList) {
				for(String headerKey : headerKeys) {
					Object key = row.get(headerKey) == null || row.get(headerKey).equals("") || row.get(headerKey).equals(" ") ? APIConstants.NOT_APPLICABLE : row.get(headerKey);
					if(!key.equals(APIConstants.NOT_APPLICABLE)) {
						if(headerKey.matches(APIConstants.FIELDS_TO_TIME_FORMAT)) {
							key = DateTime.convertMillisecondsToTime(((Number)key).longValue());
						}
						if(headerKey.matches(APIConstants.FIELDS_TO_FORMAT_DATE)) {
							key = dateFormatterForExport.format(dateFormatter.parse(key.toString()));
						}
					}
					key = appendDQ(key);
					rowLine = (rowLine.length() == 0 ? rowLine.append(key) : rowLine.append(delimiter).append(key));
				}
				stream.print(rowLine);
				rowLine.setLength(APIConstants.ZERO);
				
				// print new line
				stream.println(APIConstants.EMPTY);
				stream.flush();
			}
		} catch(Exception e) {
			InsightsLogger.error(traceId, ErrorConstants.EXCEPTION_IN.replace(ErrorConstants.REPLACER,ErrorConstants.FILE_WRITER_EXCEPTION),e);
		} finally {
			stream.close();
		}
	}

	private String appendDQ(Object key) {
		key = (APIConstants.EMPTY + key).replace("\"", "\'");
		return "\"" + key + "\"";
	}
	
	public void removeExpiredFile() {
		File parentDir = new File(getBaseConnectionService().getRealRepoPath());
		Date date = new Date();
		
		for(File file : parentDir.listFiles()) {
			try {
				if(file.isFile()) {
					long diffInMilliSec = date.getTime() - file.lastModified();
					long diffInHours = (diffInMilliSec / (60 * 60 * 1000));
					if(diffInHours > 24){
						file.delete();
					}
				}
			}
			catch(Exception e) {
				logger.error(ErrorConstants.REMOVING_EXPIRED_FILE, e);
			}
		}
	}
}

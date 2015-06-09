package org.gooru.insights.services;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.gooru.insights.builders.utils.DateTime;
import org.gooru.insights.builders.utils.InsightsLogger;
import org.gooru.insights.constants.APIConstants;
import org.gooru.insights.constants.CassandraConstants.CassandraRowKeys;
import org.gooru.insights.constants.ErrorConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ExcelWriterServiceImpl implements ExcelWriterService {

	@Autowired
	private BaseConnectionService baseConnectionService;
	
	public BaseConnectionService getBaseConnectionService() {
		return baseConnectionService;
	}
	
	public void generateExcelReport(String traceId, List<String> headerKeys, List<Map<String, Object>> rowList, String fileAbsolutePath, boolean isNewFile) {
		try {
			XSSFWorkbook workbook = null;
		    XSSFSheet sheet = null;
		    int rowNumber = 0;
		    if(isNewFile) {
		    	workbook = new XSSFWorkbook();
		    	sheet = workbook.createSheet("Report");
		    	Row headerRow = sheet.createRow(rowNumber++);
		    	int cellNumber = 0;
		    	for(String headerKey : headerKeys) {
		    		String header = getBaseConnectionService().getColumnListFromCache(CassandraRowKeys.EXPORT_FIELDS.CassandraRowKey()).getStringValue(headerKey, headerKey);
		    		headerRow.createCell(cellNumber).setCellStyle(getXSSFCellStyle(workbook));
		    		headerRow.getCell(cellNumber++).setCellValue(header);
		    	}
		    } else {
		    	FileInputStream in = new FileInputStream(new File(fileAbsolutePath));
		    	workbook = new XSSFWorkbook(in);
		    	sheet = workbook.getSheetAt(0);
		    	rowNumber = sheet.getLastRowNum() + 1;
		    }
		    createSheet(headerKeys, rowList, sheet, rowNumber);
		    FileOutputStream out =  new FileOutputStream(new File(fileAbsolutePath));
		    workbook.write(out);
		    out.close();
	    } catch (Exception e) {
	    	InsightsLogger.error(traceId, ErrorConstants.EXCEPTION_IN.replace(ErrorConstants.REPLACER,ErrorConstants.CSV_WRITER_EXCEPTION),e);
	    }
	}
	
	private void createSheet(List<String> headerKeys, List<Map<String, Object>> rowList, XSSFSheet sheet, int rowNumber) throws Exception{
		for(Map<String, Object> row : rowList) {
	    	System.out.println(row.toString());
	    	Row header = sheet.createRow(rowNumber++);
	    	int cellNumber = 0;
	    	for(String headerKey : headerKeys) {
	    		Object key = row.get(headerKey) == null || row.get(headerKey).equals("") || row.get(headerKey).equals(" ") ? APIConstants.NOT_APPLICABLE : row.get(headerKey);
				if(headerKey.matches(APIConstants.FIELDS_TO_TIME_FORMAT) && !key.equals(APIConstants.NOT_APPLICABLE)) {
					key = DateTime.convertMillisecondsToTime(((Number)key).longValue());
				}
				header.createCell(cellNumber++).setCellValue(key.toString());
	    	}
	    }
	}
	
	private XSSFCellStyle getXSSFCellStyle(XSSFWorkbook workbook) {
		XSSFCellStyle cellStyle = workbook.createCellStyle();
		cellStyle.setFillBackgroundColor(IndexedColors.BLUE.getIndex());
		cellStyle.setFillPattern(CellStyle.ALIGN_FILL);
		cellStyle.setAlignment(CellStyle.ALIGN_CENTER);
		cellStyle.setFont(getXSSFFont(workbook));
		cellStyle.setWrapText(true);
		return cellStyle;
	}
	
	private XSSFFont getXSSFFont(XSSFWorkbook workbook) {
		XSSFFont font= workbook.createFont();
	    font.setFontHeightInPoints((short)10);
	    font.setFontName("Arial");
	    font.setColor(IndexedColors.WHITE.getIndex());
	    font.setBold(true);
	    font.setItalic(false);
	    return font;
	}
}

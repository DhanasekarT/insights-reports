package org.gooru.insights.services;

import java.awt.image.RenderedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Resource;
import javax.imageio.ImageIO;

import org.apache.poi.hssf.usermodel.HSSFClientAnchor;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFPatriarch;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Comment;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.Picture;
import org.apache.poi.ss.usermodel.RichTextString;
import org.apache.poi.ss.usermodel.Row;
import org.springframework.stereotype.Service;

import com.google.common.collect.Lists;

@Service
public class ExcelBuilderServiceImpl implements ExcelBuilderService{

	@Resource(name = "filePath")
	private Properties filePath;

	public String exportXlsReport(List<Map<String, Object>> listOfMap, String fileName, boolean isNewFile) throws ParseException, IOException {

		HSSFWorkbook workbook = null;
		HSSFSheet sheet = null;
		Picture picture;
		Map<Integer, Integer> imageMap = new HashMap<Integer, Integer>();
		boolean hasImage = false;
		int sheetRowCount = 0;
		int rownum = 0;
		boolean needHeader = true;

		if (fileName == null) {
			fileName = "export";
		}
		if (isNewFile) {
			workbook = new HSSFWorkbook();
			sheet = workbook.createSheet("sheet");
		} else {
			FileInputStream file = new FileInputStream(new File(setFilePath(fileName)));
			workbook = new HSSFWorkbook(file);
			sheet = workbook.getSheetAt(0);
			rownum = sheet.getLastRowNum() + 2;
			sheetRowCount = sheet.getLastRowNum() + 2;
		}
		for (Map<String, Object> resultSet : listOfMap) {
			int cellnum = 0;
			Row row = sheet.createRow(rownum++);
			Set<String> keyset = resultSet.keySet();

			if (needHeader) {
				for (String key : keyset) {
					Cell cell = row.createCell(cellnum++);
					if (key.toString().contains("<img-image tag was disabled for this release")) {
						hasImage = true;
						String[] richText = key.toString().split("-");
						String value = this.checkImage(key.toString());
						Drawing drawing = null;
						HSSFClientAnchor anchor2 = new HSSFClientAnchor();
						CreationHelper helper = workbook.getCreationHelper();
						int imageCount =0;
						for (String url : value.split(",")) {
							imageCount++;
							try {
								URL imageURL = new URL(url);
								RenderedImage image = ImageIO.read(imageURL);
								ByteArrayOutputStream baos = new ByteArrayOutputStream();
								ImageIO.write((RenderedImage) image, "png", baos);
								CellStyle headerStyle = workbook.createCellStyle();
								
								if(imageCount == 1)
								headerStyle.setAlignment(headerStyle.ALIGN_LEFT);
								else if(imageCount == 2)
								headerStyle.setAlignment(headerStyle.ALIGN_CENTER);
								else if(imageCount == 3)
									headerStyle.setAlignment(headerStyle.ALIGN_RIGHT);
								byte[] b = baos.toByteArray();
								int pictureIdx = workbook.addPicture(b, HSSFWorkbook.PICTURE_TYPE_JPEG);
								cell.setCellStyle(headerStyle);
								drawing = null;
								helper = workbook.getCreationHelper();
								drawing = sheet.createDrawingPatriarch();
								
								workbook.getSheetAt(0).setColumnWidth(cell.getColumnIndex(), 5000);
								row.setHeight((short) 1000);
								
								anchor2 = new HSSFClientAnchor();
								anchor2.setAnchor((short) cell.getColumnIndex(), row.getRowNum(), 0, 0, (short) (cell.getColumnIndex() + 1), row.getRowNum(), 0, 0);
								anchor2.setAnchorType(2);
								
								int index = workbook.addPicture(b, HSSFWorkbook.PICTURE_TYPE_PNG);
								HSSFPatriarch patriarch = sheet.createDrawingPatriarch();
								picture = patriarch.createPicture(anchor2, index);
								
								imageMap.put(cell.getColumnIndex(), row.getRowNum());
								picture.resize(1.0);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
						anchor2 = new HSSFClientAnchor();
						anchor2.setRow1(row.getRowNum());
						anchor2.setCol1(cell.getColumnIndex());
						
						CellStyle headerStyle = workbook.createCellStyle();
						headerStyle.setAlignment(headerStyle.ALIGN_CENTER);
						Comment comment = drawing.createCellComment(anchor2);
						RichTextString str = helper.createRichTextString(richText[richText.length - 1]);
						comment.setString(str);
						cell.setCellComment(comment);
					} else {

						cell.setCellValue(key);
					}
				}
				needHeader = false;
				row = sheet.createRow(rownum++);
				cellnum = 0;
			}
			for (String key : keyset) {
				Object values = resultSet.get(key);
				Cell cell = row.createCell(cellnum++);
				if (values instanceof Date)
					cell.setCellValue((Date) values);
				else if (values instanceof Boolean)
					cell.setCellValue((Boolean) values);
				else if (values instanceof String)
					cell.setCellValue((String) values);
				else if (values instanceof Double)
					cell.setCellValue((Double) values);
				else if (values instanceof Long)
					cell.setCellValue((Long) values);
				else if (values instanceof Integer)
					cell.setCellValue((Integer) values);

			}

		}
		int currentSheetCount = sheet.getLastRowNum();

		for (int row = sheetRowCount; row <= currentSheetCount; row++) {
			if (sheet.getRow(row).getPhysicalNumberOfCells() != 0) {
				for (int i = 0; i < sheet.getRow(row).getPhysicalNumberOfCells(); i++) {
					if (hasImage) {
						if (!imageMap.containsKey(i) && !imageMap.containsKey(row)) {
							sheet.autoSizeColumn(i);
						}
					} else {
						sheet.autoSizeColumn(i);
					}
				}
			}
		}
		try {
			FileOutputStream out = new FileOutputStream(new File(setFilePath(fileName)));
			workbook.write(out);
			out.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return setFilePath(fileName);
	}

	public String generateExcelReport(String startDate, String endDate, Integer partnerIpdId, List<Map<String, String>> resultSet, String fileName) throws ParseException, IOException {

		int headerCells = 0;
		int reportCount = 1;
		int rowCount = 1;
		int avoidDate = 0;
		int changeColumn = 0;
		boolean headerColumns = false;
		boolean firstEntry = false;
		// /Set output File
		File file = new File(setFilePath(fileName));
		FileOutputStream fileOut = new FileOutputStream(file);

		// create workbook
		HSSFWorkbook workbook = new HSSFWorkbook();
		HSSFSheet sheet = workbook.createSheet("Page" + reportCount);

		// create header row
		HSSFRow header = sheet.createRow(0);
		CellStyle style = this.getStyle(workbook, sheet);

		for (List<Map<String, String>> dataList : Lists.partition(resultSet, 10002)) {
			// Pagination
			if (rowCount >= 10000) {
				reportCount++;
				sheet = workbook.createSheet("Page" + reportCount);
				style = this.getStyle(workbook, sheet);
				header = sheet.createRow(0);
				rowCount = 1;
				headerCells = 0;
				headerColumns = false;
				avoidDate = 0;
				firstEntry = false;
				changeColumn = 0;
			}
			for (Map<String, String> map : dataList) {

				// Including Header
				if (!headerColumns) {
					for (Map.Entry<String, String> entry : map.entrySet()) {
						header.createCell(headerCells).setCellValue(entry.getKey());
						header.getCell(headerCells).setCellStyle(style);
						headerCells++;
						if (entry.getKey().equalsIgnoreCase("startDate") || entry.getKey().equalsIgnoreCase("endDate") || entry.getKey().equalsIgnoreCase("date")) {
							avoidDate++;
						}
					}
					headerColumns = true;
				}

				// /create a new Row
				HSSFRow aRow = sheet.createRow(rowCount++);

				// insert data to created row
				for (Map.Entry<String, String> entry : map.entrySet()) {

					// check for correct header
					if (!firstEntry) {
						firstEntry = true;
					}

					for (int headerRow = changeColumn; headerRow < headerCells; headerRow++) {

						if (header.getCell(headerRow).toString().equalsIgnoreCase(entry.getKey())) {
							aRow.createCell(headerRow).setCellValue(entry.getValue().toString());
						}

					}
				}
				changeColumn = avoidDate;

			}

			// writing to output file
			workbook.write(fileOut);
			fileOut.flush();
		}
		fileOut.close();
		// done

		return getFilePath(fileName);
	}

	public String generateExcelFile(List<Map<String, String>> parentList, List<Map<String, String>> childList, Integer parentHeaderStartRow, Integer parentHeaderStartColumn,
			Integer childHeaderStartRow, Integer childHeaderStartColumn, Integer spaceNeed, String fileName, String sheetName) throws ParseException, IOException {

		int headerCells = 0;
		int rowCount = 1;
		int changeColumn = 0;
		boolean headerColumns = false;
		boolean firstEntry = false;

		// /Set output File
		File file = new File(setFilePath(fileName));
		FileOutputStream fileOut = new FileOutputStream(file);

		// create workbook
		HSSFWorkbook workbook = new HSSFWorkbook();
		HSSFSheet sheet = workbook.createSheet(sheetName);

		// create parent header row
		HSSFRow header = sheet.createRow(parentHeaderStartRow);
		CellStyle style = this.getStyle(workbook, sheet);

		for (Map<String, String> map : parentList) {

			// Including Header
			if (!headerColumns) {
				headerCells = parentHeaderStartColumn != null ? parentHeaderStartColumn : headerCells;
				changeColumn = parentHeaderStartColumn != null ? parentHeaderStartColumn : headerCells;
				for (Map.Entry<String, String> entry : map.entrySet()) {
					header.createCell(headerCells).setCellValue(entry.getKey());
					header.getCell(headerCells).setCellStyle(style);
					headerCells++;
				}
				headerColumns = true;
			}

			// /create a new Row
			HSSFRow aRow = sheet.createRow(rowCount++);

			// insert data to created row
			for (Map.Entry<String, String> entry : map.entrySet()) {

				// check for correct header
				if (!firstEntry) {
					firstEntry = true;
				}

				for (int headerRow = changeColumn; headerRow < headerCells; headerRow++) {

					if (header.getCell(headerRow).toString().equalsIgnoreCase(entry.getKey())) {
						aRow.createCell(headerRow).setCellValue(entry.getValue().toString());
					}

				}
			}

		}
		// writing to output file
		workbook.write(fileOut);
		fileOut.flush();

		// parent list inserted processing for child data

		rowCount = rowCount + childHeaderStartRow + spaceNeed;
		headerCells = 0;
		changeColumn = 0;
		headerColumns = false;
		firstEntry = false;

		// create child header row
		header = sheet.createRow(rowCount);

		for (Map<String, String> map : childList) {

			// Including Header
			if (!headerColumns) {
				headerCells = childHeaderStartColumn != null ? childHeaderStartColumn : headerCells;
				changeColumn = childHeaderStartColumn != null ? childHeaderStartColumn : headerCells;
				for (Map.Entry<String, String> entry : map.entrySet()) {
					header.createCell(headerCells).setCellValue(entry.getKey());
					header.getCell(headerCells).setCellStyle(style);
					headerCells++;
				}
				headerColumns = true;
			}

			// /create a new Row
			HSSFRow aRow = sheet.createRow(rowCount++);

			// insert data to created row
			for (Map.Entry<String, String> entry : map.entrySet()) {

				// check for correct header
				if (!firstEntry) {
					firstEntry = true;
				}

				for (int headerRow = changeColumn; headerRow < headerCells; headerRow++) {

					if (header.getCell(headerRow).toString().equalsIgnoreCase(entry.getKey())) {
						aRow.createCell(headerRow).setCellValue(entry.getValue().toString());
					}

				}
			}

		}

		// writing to output file
		workbook.write(fileOut);
		fileOut.flush();
		fileOut.close();
		// done

		return getFilePath(fileName);
	}

	// style for sheets
	public CellStyle getStyle(HSSFWorkbook workbook, HSSFSheet sheet) {

		sheet.setDefaultColumnWidth(30);
		// create style for header cells
		CellStyle style = workbook.createCellStyle();
		HSSFFont font = workbook.createFont();
		font.setFontName("Arial");
		style.setFillForegroundColor(HSSFColor.BLUE.index);
		style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		font.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);
		font.setColor(HSSFColor.WHITE.index);
		style.setFont(font);
		return style;
	}

	public Properties getFilePath() {
		return filePath;
	}

	public void setFilePath(Properties filePath) {
		this.filePath = filePath;
	}

	public String setFilePath(String file) {

		String fileName = this.getFilePath().getProperty("insights.file.real.path");
		if (file != null && (!file.isEmpty())) {
			fileName += file;

		} else {
			fileName += "insights";
		}
		return fileName;
	}

	public String getFilePath(String file) {

		String fileName = this.getFilePath().getProperty("insights.file.app.path");

		if (file != null && (!file.isEmpty())) {
			fileName += file;

		} else {
			fileName += "insights";
		}
		return fileName;
	}

	public String checkImage(String value) {
		String image = "";
		String[] val = value.split("src=\"");
		String[] url = new String[val.length];
		String[] exactUrl = new String[url.length];
		int j = 0;
		for (int i = 0; i < val.length; i++) {
			if (val[i].startsWith("http")) {
				url[j] = val[i];
				j++;
			}
		}
		String[] sub = new String[j + 1];
		j = 0;
		for (int i = 0; i < url.length; i++) {
			if (url[i] != null) {
				sub = url[i].split("\">");
				if (sub.length >= 0) {
					exactUrl[j] = sub[0];
					j++;
				}
			}
		}
		int k = 0;
		for (int i = 0; i < exactUrl.length; i++) {
			if (exactUrl[i] != null) {
				if (k == 0) {
					image = exactUrl[i];
				} else {
					image += "," + exactUrl[i];
				}
				k++;
			}
		}
		return image;
	}
}

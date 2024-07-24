package app.trigyn.utility.dbUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellUtil;
import org.apache.poi.ss.util.RegionUtil;
import org.apache.poi.xssf.usermodel.DefaultIndexedColorMap;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class Publisher {
	private DBCompareStructure	dbCompareStructure		= null;
	private Workbook			workbook				= new XSSFWorkbook();
	private Properties			applicationProperties	= null;

	public Publisher(DBCompareStructure a_dbCompareStructure, Properties a_applicationProperties) {
		dbCompareStructure = a_dbCompareStructure;
		applicationProperties = a_applicationProperties;
	}

	public void publish() throws Throwable {
		publishSummary();
		publishTableSummary();
		publishTableDetails();
		publishTableIndexes();
		publishOtherObjects();
		saveWorkbook();
	}
	
	private void publishOtherObjects() throws Throwable {
		// create a sheet in the workbook(you can give it a name)
		Sheet			sheet			= null;

		// create a row in the sheet
		Row				row				= null;

		// add cells in the sheet
		Cell			cell			= null, tempCell = null;

		XSSFCellStyle	cellHeaderStyle	= getHeaderStyle();

		sheet = workbook.createSheet("Other Objects");
		row		= sheet.createRow(2);

		cell	= row.createCell(2);
		cell.setCellValue("Object Name");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(2, 20 * 256);

		cell = row.createCell(3);
		cell.setCellValue("Type");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(3, 15 * 256);
		
		cell = row.createCell(4);
		cell.setCellValue("Source");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(4, 8 * 256);
		
		sheet.setColumnWidth(5, 2 * 256);

		cell = row.createCell(6);
		cell.setCellValue("Target");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(6, 8 * 256);
		
		int rowCounter = 3;
		Map<String, Integer> columnIndex = new HashMap<String, Integer>();
		
		for(Map.Entry<String, String> sourceObjects : dbCompareStructure.sourceObjects.entrySet()) {
			row		= sheet.createRow(rowCounter++);
			cell	= row.createCell(2);
			cell.setCellValue(sourceObjects.getKey());
			CellUtil.setVerticalAlignment(cell, VerticalAlignment.CENTER);
			
			cell	= row.createCell(3);
			cell.setCellValue(sourceObjects.getValue());
			columnIndex.put(sourceObjects.getKey(), rowCounter - 1);
			
			cell	= row.createCell(4);
			cell.setCellValue("☑");
			cell.setCellStyle(getCenterStyle());
			CellUtil.setAlignment(cell, HorizontalAlignment.CENTER);
			
			if(dbCompareStructure.targetObjects.containsKey(sourceObjects.getKey()) == false) {
				cell	= row.createCell(6);
				cell.setCellValue("☒");
				cell.setCellStyle(getCenterStyleError());
				CellUtil.setAlignment(cell, HorizontalAlignment.CENTER);
			}
		}
		
		for(Map.Entry<String, String> targetObjects : dbCompareStructure.targetObjects.entrySet()) {
			if (columnIndex.containsKey(targetObjects.getKey())) {
				row = sheet.getRow(columnIndex.get(targetObjects.getKey()));
			} else {
				row = sheet.createRow(rowCounter++);
			}
			
			
			
			cell	= row.createCell(6);
			cell.setCellValue("☑");
			cell.setCellStyle(getCenterStyle());
			CellUtil.setAlignment(cell, HorizontalAlignment.CENTER);
			
			if (columnIndex.containsKey(targetObjects.getKey()) == false) {
				cell	= row.createCell(2);
				cell.setCellValue(targetObjects.getKey());
				CellUtil.setVerticalAlignment(cell, VerticalAlignment.CENTER);
				
				cell	= row.createCell(3);
				cell.setCellValue(targetObjects.getValue());
				
				cell	= row.createCell(4);
				cell.setCellValue("☒");
				cell.setCellStyle(getCenterStyleError());
				CellUtil.setAlignment(cell, HorizontalAlignment.CENTER);
			}
		}
		
		sheet.autoSizeColumn(2);
	}
	
	private void publishTableIndexes() throws Throwable {
		// create a sheet in the workbook(you can give it a name)
		Sheet			sheet		= null;

		// create a row in the sheet
		Row				row			= null;

		// add cells in the sheet
		Cell			cell		= null, tempCell = null;

		XSSFCellStyle	cellHeaderStyle	= getHeaderStyle();
		
		sheet	= workbook.createSheet("Table Index Details");
		
		row		= sheet.createRow(1);
		cell	= row.createCell(4);
		cell.setCellValue("Source");
		cell.setCellStyle(cellHeaderStyle);
		sheet.addMergedRegion(new CellRangeAddress(1, 1, 4, 11));
		RegionUtil.setBorderBottom(BorderStyle.THICK, new CellRangeAddress(1, 1, 4, 11), sheet);
		
		row		= sheet.createRow(2);

		cell	= row.createCell(2);
		cell.setCellValue("Table Name");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(2, 15 * 256);

		cell = row.createCell(3);
		cell.setCellValue("Column Name");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(3, 15 * 256);
		
		cell = row.createCell(4);
		cell.setCellValue("Unique");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(4, 8 * 256);
		
		cell = row.createCell(5);
		cell.setCellValue("Type");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(5, 10 * 256);

		cell = row.createCell(6);
		cell.setCellValue("Sort");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(6, 5 * 256);

		cell = row.createCell(7);
		cell.setCellValue("PK Table");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(7, 12 * 256);

		cell = row.createCell(8);
		cell.setCellValue("PK Column");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(8, 12 * 256);

		cell = row.createCell(9);
		cell.setCellValue("Update");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(9, 10 * 256);
		
		cell = row.createCell(10);
		cell.setCellValue("Delete");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(10, 10 * 256);

		cell = row.createCell(11);
		cell.setCellValue("Defered");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(11, 12 * 256);
		
		
		//12 is separator
		sheet.setColumnWidth(12, 2 * 256);
		
		row		= sheet.getRow(1);
		cell	= row.createCell(13);
		cell.setCellValue("Target");
		cell.setCellStyle(cellHeaderStyle);
		sheet.addMergedRegion(new CellRangeAddress(1, 1, 13, 20));
		RegionUtil.setBorderBottom(BorderStyle.THICK, new CellRangeAddress(1, 1, 13, 20), sheet);
		
		row		= sheet.getRow(2);
		
		cell = row.createCell(13);
		cell.setCellValue("Unique");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(13, 8 * 256);
		
		cell = row.createCell(14);
		cell.setCellValue("Type");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(14, 10 * 256);

		cell = row.createCell(15);
		cell.setCellValue("Sort");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(15, 5 * 256);

		cell = row.createCell(16);
		cell.setCellValue("PK Table");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(16, 12 * 256);

		cell = row.createCell(17);
		cell.setCellValue("PK Column");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(17, 12 * 256);

		cell = row.createCell(18);
		cell.setCellValue("Update");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(18, 10 * 256);
		
		cell = row.createCell(19);
		cell.setCellValue("Delete");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(19, 10 * 256);

		cell = row.createCell(20);
		cell.setCellValue("Defered");
		cell.setCellStyle(cellHeaderStyle);
		sheet.setColumnWidth(20, 12 * 256);
		
		int columnCount, rowCounter = 3, currentRow;
		Map<String, Integer> columnIndex = new HashMap<String, Integer>();
		
		Set<String> tables = new HashSet<String>();
		
		tables.addAll(dbCompareStructure.sourceTables.keySet());
		tables.addAll(dbCompareStructure.targetTables.keySet());
		
		for (String a_sourceTable : tables) {
			
			if(dbCompareStructure.sourceTables.keySet().contains(a_sourceTable) == false || 
					dbCompareStructure.targetTables.keySet().contains(a_sourceTable) == false) {
				//no need to compare as the table is not present only
				continue;
			}
			
			columnIndex.clear();
			
			if (DBCompareStructure.compareIndexes(dbCompareStructure, a_sourceTable) == false) {
				System.err.println(a_sourceTable + " isn't same");
				
				columnCount = dbCompareStructure.sourceTableColumnIndexes.get(a_sourceTable).size();
				for(DBColumnIndex tempCol : dbCompareStructure.sourceTableColumnIndexes.get(a_sourceTable).values()) {
					row		= sheet.createRow(rowCounter++);
					cell	= row.createCell(2);
					cell.setCellValue(a_sourceTable);
					CellUtil.setVerticalAlignment(cell, VerticalAlignment.CENTER);
					
					cell	= row.createCell(3);
					cell.setCellValue(tempCol.getColumnName());
					columnIndex.put(tempCol.getColumnName(), rowCounter - 1);
					cell.setCellStyle(getStyleError());
					
					cell	= row.createCell(4);
					cell.setCellValue(tempCol.isNonUnique() ? "☒" : "☑");
					cell.setCellStyle(getCenterStyle());
					CellUtil.setAlignment(cell, HorizontalAlignment.CENTER);
					
					cell	= row.createCell(5);
					cell.setCellValue(getIndexTypeText(tempCol.getType()));
					
					cell	= row.createCell(6);
					cell.setCellValue(tempCol.getSorting());
					
					cell	= row.createCell(7);
					cell.setCellValue(tempCol.getPKTable());
					
					cell	= row.createCell(8);
					cell.setCellValue(tempCol.getPKColumn());
					
					cell	= row.createCell(9);
					cell.setCellValue(getRuleText(tempCol.getUpdateRule()));
					
					cell	= row.createCell(10);
					cell.setCellValue(getRuleText(tempCol.getDeleteRule()));
					
					cell	= row.createCell(11);
					cell.setCellValue(getDeferrabilityText(tempCol.getDeferrability()));
				}
				
				boolean targetOnly = false;
				if (dbCompareStructure.targetTableColumnIndexes.containsKey(a_sourceTable)) {
					for(DBColumnIndex tempCol : dbCompareStructure.targetTableColumnIndexes.get(a_sourceTable).values()) {
						 targetOnly = false;
						 
						if(columnIndex.containsKey(tempCol.getColumnName())) {
							XSSFCellStyle	cellStyle	= ((XSSFWorkbook) workbook).createCellStyle();
							cellStyle.setVerticalAlignment(VerticalAlignment.CENTER);
							
							currentRow = columnIndex.get(tempCol.getColumnName());
							row		= sheet.getRow(currentRow);
							cell	= row.getCell(3);
							cell.setCellStyle(cellStyle);
						}else {
							targetOnly = true;
							columnCount++;
							
							currentRow = rowCounter++;
							row		= sheet.createRow(currentRow);
							
							columnIndex.put(tempCol.getColumnName(), rowCounter - 1);
							
							cell	= row.createCell(2);
							cell.setCellValue(a_sourceTable);
							CellUtil.setVerticalAlignment(cell, VerticalAlignment.CENTER);
							
							cell	= row.createCell(3);
							cell.setCellValue(tempCol.getColumnName());
							columnIndex.put(tempCol.getColumnName(), rowCounter - 1);
							cell.setCellStyle(getStyleError());
						}
						
						cell	= row.createCell(13);
						cell.setCellValue(tempCol.isNonUnique() ? "☒" : "☑");
						cell.setCellStyle(getCenterStyle());
						CellUtil.setAlignment(cell, HorizontalAlignment.CENTER);
						
						if(targetOnly == false) {
							tempCell = row.getCell(4);
							if(cell.getStringCellValue().equalsIgnoreCase(tempCell.getStringCellValue()) == false) {
								cell.setCellStyle(getCenterStyleError());
							}
						}
						
						
						cell	= row.createCell(14);
						cell.setCellValue(getIndexTypeText(tempCol.getType()));
						
						if(targetOnly == false) {
							tempCell = row.getCell(5);
							if(cell.getStringCellValue().equalsIgnoreCase(tempCell.getStringCellValue()) == false) {
								cell.setCellStyle(getStyleError());
							}
						}
						
						cell	= row.createCell(15);
						cell.setCellValue(tempCol.getSorting());
						
						if(targetOnly == false) {
							tempCell = row.getCell(6);
							if(cell.getStringCellValue().equalsIgnoreCase(tempCell.getStringCellValue()) == false) {
								cell.setCellStyle(getStyleError());
							}
						}
	
						
						cell	= row.createCell(16);
						cell.setCellValue(tempCol.getPKTable());
						
						if (targetOnly == false) {
							tempCell = row.getCell(7);
							if (cell.getStringCellValue().equalsIgnoreCase(tempCell.getStringCellValue()) == false) {
								cell.setCellStyle(getStyleError());
							}
						}					
						cell	= row.createCell(17);
						cell.setCellValue(tempCol.getPKColumn());
						if (targetOnly == false) {
							tempCell = row.getCell(8);
							if(cell.getStringCellValue().equalsIgnoreCase(tempCell.getStringCellValue()) == false) {
								cell.setCellStyle(getStyleError());
							}
						}
						
						cell	= row.createCell(18);
						cell.setCellValue(getRuleText(tempCol.getUpdateRule()));
						
						if (targetOnly == false) {
							tempCell = row.getCell(9);
							if(cell.getStringCellValue().equalsIgnoreCase(tempCell.getStringCellValue()) == false) {
								cell.setCellStyle(getStyleError());
							}
						}
						
						cell	= row.createCell(19);
						cell.setCellValue(getRuleText(tempCol.getDeleteRule()));
						
						if (targetOnly == false) {
							tempCell = row.getCell(10);
							if(cell.getStringCellValue().equalsIgnoreCase(tempCell.getStringCellValue()) == false) {
								cell.setCellStyle(getStyleError());
							}
						}
						
						cell	= row.createCell(20);
						cell.setCellValue(getDeferrabilityText(tempCol.getDeferrability()));
						if (targetOnly == false) {
							tempCell = row.getCell(11);
							if(cell.getStringCellValue().equalsIgnoreCase(tempCell.getStringCellValue()) == false) {
								cell.setCellStyle(getStyleError());
							}
						}
					}
				}
//				System.out.println("Row counter : " + rowCounter + " & columnCount " + columnCount);
				if(columnCount > 1) {
					sheet.addMergedRegion(new CellRangeAddress(rowCounter - columnCount, rowCounter - 1, 2, 2));	
				}
				
//				rowCounter++;
				
				row = sheet.createRow(rowCounter);
				cell = row.createCell(2);
				cell.setCellStyle(getStyleSeparator());
				sheet.addMergedRegion(new CellRangeAddress(rowCounter, rowCounter, 2, 20));
				
				rowCounter++;
			}
		}
		
		sheet.autoSizeColumn(2);
		sheet.autoSizeColumn(3);
	}
	
	private static String getIndexTypeText(int type) {
        switch (type) {
            case 0:
                return "Statistic";
            case 1:
                return "Clustered";
            case 2:
                return "Hashed";
            case 3:
                return "Other";
            case -1:
                return "";
            default:
                return "unknown";
        }
    }
	
	private static String getRuleText(int rule) {
        switch (rule) {
            case 0:
                return "NoAction";
            case 1:
                return "Cascade";
            case 2:
                return "SetNull";
            case 3:
                return "Restrict";
            case 4:
                return "Default";
            case -1:
                return "";
            default:
                return "unknown";
        }
    }
	
	private static String getDeferrabilityText(int deferrability) {
        switch (deferrability) {
            case 5:
                return "Deferred";
            case 6:
                return "Immediate";
            case 7:
                return "NotDeferrable";
            case -1:
                return "";
            default:
                return "unknown";
        }
    }

	private void publishTableDetails() throws Throwable {
		System.out.println("Publisher.publishTableDetails()");
		
		ResultSet rs = null;

		// create a sheet in the workbook(you can give it a name)
		Sheet			sheet		= null;

		// create a row in the sheet
		Row				row			= null;

		// add cells in the sheet
		Cell			cell		= null, tempCell = null;

		XSSFCellStyle	cellStyle	= getHeaderStyle();

		sheet	= workbook.createSheet("Table Details");
		for(int colNumber = 4; colNumber <= 18; colNumber++) {
			sheet.setColumnWidth(colNumber, 10 * 256);	
		}
		sheet.setColumnWidth(2, 25 * 256);
		sheet.setColumnWidth(3, 25 * 256);
		sheet.setColumnWidth(7, 12 * 256);
		sheet.setColumnWidth(8, 18 * 256);
		
		sheet.setColumnWidth(10, 2 * 256);
		sheet.setColumnWidth(14, 12 * 256);
		sheet.setColumnWidth(15, 18 * 256);
		
		row		= sheet.createRow(1);
		cell	= row.createCell(4);
		cell.setCellValue("Source");
		cell.setCellStyle(cellStyle);
		sheet.addMergedRegion(new CellRangeAddress(1, 1, 4, 9));
		RegionUtil.setBorderBottom(BorderStyle.THICK, new CellRangeAddress(1, 1, 4, 9), sheet);
		
		row		= sheet.createRow(2);

		cell	= row.createCell(2);
		cell.setCellValue("Table Name");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(3);
		cell.setCellValue("Column Name");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(4);
		cell.setCellValue("Type");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(5);
		cell.setCellValue("Precision");
		cell.setCellStyle(cellStyle);
		
		cell = row.createCell(6);
		cell.setCellValue("Scale");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(7);
		cell.setCellValue("Mandatory");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(8);
		cell.setCellValue("Auto Increament");
		cell.setCellStyle(cellStyle);
		
		cell = row.createCell(9);
		cell.setCellValue("Primary");
		cell.setCellStyle(cellStyle);
		
		//Target section
		
		row		= sheet.getRow(1);
		cell	= row.createCell(11);
		cell.setCellValue("Target");
		cell.setCellStyle(cellStyle);
		sheet.addMergedRegion(new CellRangeAddress(1, 1, 11, 16));
		RegionUtil.setBorderBottom(BorderStyle.THICK, new CellRangeAddress(1, 1, 11, 16), sheet);

		row		= sheet.getRow(2);
		
		cell = row.createCell(11);
		cell.setCellValue("Type");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(12);
		cell.setCellValue("Precision");
		cell.setCellStyle(cellStyle);
		
		cell = row.createCell(13);
		cell.setCellValue("Scale");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(14);
		cell.setCellValue("Mandatory");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(15);
		cell.setCellValue("Auto Increament");
		cell.setCellStyle(cellStyle);
		
		cell = row.createCell(16);
		cell.setCellValue("Primary");
		cell.setCellStyle(cellStyle);

		int columnCount, rowCounter = 3, totalColumnCount = 0;
		ResultSetMetaData sourceMetadata = null, targetMetaData = null;
		
		XSSFCellStyle	cellStyleMerge	= ((XSSFWorkbook) workbook).createCellStyle();
		cellStyle.setVerticalAlignment(VerticalAlignment.CENTER);
		cellStyle.setAlignment(HorizontalAlignment.CENTER);
		
		Map<String, Integer> columnIndex = new HashMap<String, Integer>();
		for (Map.Entry<String, ResultSetMetaData> a_sourceTable : dbCompareStructure.sourceTables.entrySet()) {
			totalColumnCount = 0;
			sourceMetadata = a_sourceTable.getValue();
			columnIndex.clear();
			
			if (dbCompareStructure.targetTables.containsKey(a_sourceTable.getKey()) == false) {
				// ignore if the table isn't present in target database
				continue;
			}
			
			targetMetaData = dbCompareStructure.targetTables.get(a_sourceTable.getKey());

			if (DBCompareStructure.compareDBTable(sourceMetadata,
					dbCompareStructure.targetTables.get(a_sourceTable.getKey()), 
					dbCompareStructure) == false) {
				
				System.out.println(a_sourceTable.getKey() + " is not equal");
				
				columnCount = sourceMetadata.getColumnCount();
				for(int columnCounter = 1; columnCounter <= columnCount; columnCounter++) {
					totalColumnCount++;
					row		= sheet.createRow(rowCounter++);
					
					cell	= row.createCell(2);
					cell.setCellValue(a_sourceTable.getKey());
					cell.setCellStyle(cellStyleMerge);
					CellUtil.setVerticalAlignment(cell, VerticalAlignment.CENTER);
					
					cell	= row.createCell(3);
					cell.setCellValue(sourceMetadata.getColumnName(columnCounter));
					columnIndex.put(sourceMetadata.getColumnName(columnCounter), rowCounter - 1);
					cell.setCellStyle(getStyleError());
					
					cell	= row.createCell(4);
					cell.setCellValue(sourceMetadata.getColumnTypeName(columnCounter));
					
					cell	= row.createCell(5);
					cell.setCellValue(sourceMetadata.getPrecision(columnCounter));
					
					cell	= row.createCell(6);
					cell.setCellValue(sourceMetadata.getScale(columnCounter));
					
					cell	= row.createCell(7);
					cell.setCellStyle(getCenterStyle());
					cell.setCellValue(sourceMetadata.isNullable(columnCounter) == 1 ? "☒" : "☑");
					
					cell	= row.createCell(8);
					cell.setCellStyle(getCenterStyle());
					cell.setCellValue(sourceMetadata.isAutoIncrement(columnCounter) ? "☑" : "☒");
					
					cell	= row.createCell(9);
					cell.setCellStyle(getCenterStyle());
					cell.setCellValue("☒");
				}//for loop of columns of source db
				
				columnCount = targetMetaData.getColumnCount();
				
				for(int columnCounter = 1; columnCounter <= columnCount; columnCounter++) {
					int currentRow = -1;
					
					if(columnIndex.containsKey(targetMetaData.getColumnName(columnCounter))) {
						currentRow = columnIndex.get(targetMetaData.getColumnName(columnCounter));
						row		= sheet.getRow(currentRow);
						
						cell	= row.getCell(3);
						
						XSSFCellStyle	cellStyleNew	= ((XSSFWorkbook) workbook).createCellStyle();
						cellStyle.setVerticalAlignment(VerticalAlignment.CENTER);
						cell.setCellStyle(cellStyleNew);
					}else {
						totalColumnCount++;
						currentRow = rowCounter++;
						row		= sheet.createRow(currentRow);
						
						columnIndex.put(targetMetaData.getColumnName(columnCounter), rowCounter - 1);
						
						cell	= row.createCell(2);
						cell.setCellValue(a_sourceTable.getKey());
						cell.setCellStyle(cellStyleMerge);
						
						cell	= row.createCell(3);
						cell.setCellValue(targetMetaData.getColumnName(columnCounter));
						columnIndex.put(targetMetaData.getColumnName(columnCounter), rowCounter - 1);
						cell.setCellStyle(getStyleError());
					}
					
					cell	= row.createCell(11);
					cell.setCellValue(targetMetaData.getColumnTypeName(columnCounter));
					
					tempCell = row.getCell(4);
					if(tempCell != null && cell.getStringCellValue().equalsIgnoreCase(tempCell.getStringCellValue()) == false) {
						cell.setCellStyle(getStyleError());
					}
					
					cell	= row.createCell(12);
					cell.setCellValue(targetMetaData.getPrecision(columnCounter));
					tempCell = row.getCell(5);
					if(tempCell != null && cell.getNumericCellValue() != tempCell.getNumericCellValue()) {
						cell.setCellStyle(getStyleError());
					}
					
					cell	= row.createCell(13);
					cell.setCellValue(targetMetaData.getScale(columnCounter));
					tempCell = row.getCell(6);
					if(tempCell != null && cell.getNumericCellValue() != tempCell.getNumericCellValue()) {
						cell.setCellStyle(getStyleError());
					}
					
					cell	= row.createCell(14);
					cell.setCellStyle(getCenterStyle());
					cell.setCellValue(targetMetaData.isNullable(columnCounter) == 1 ? "☒" : "☑");
					
					tempCell = row.getCell(7);
					if(tempCell != null && cell.getStringCellValue().equalsIgnoreCase(tempCell.getStringCellValue()) == false) {
						cell.setCellStyle(getCenterStyleError());
					}
					
					cell	= row.createCell(15);
					cell.setCellStyle(getCenterStyle());
					cell.setCellValue(targetMetaData.isAutoIncrement(columnCounter) ? "☑" : "☒");
					
					tempCell = row.getCell(8);
					if(tempCell != null && cell.getStringCellValue().equalsIgnoreCase(tempCell.getStringCellValue()) == false) {
						cell.setCellStyle(getCenterStyleError());
					}
					
					cell	= row.createCell(16);
					cell.setCellStyle(getCenterStyle());
					cell.setCellValue("☒");
				}//for loop of columns of target db
				
				if(totalColumnCount > 1) {
					sheet.addMergedRegion(new CellRangeAddress(rowCounter - totalColumnCount, rowCounter - 1, 2, 2));	
				}
				
				rs = dbCompareStructure.sourceDBMetaData.getPrimaryKeys(dbCompareStructure.sourceConnection.getSchemaName(), dbCompareStructure.sourceConnection.getSchemaName(), a_sourceTable.getKey());
				while(rs.next()) {
					String colName = rs.getString("COLUMN_NAME");
					if(columnIndex.containsKey(colName)) {
						int colRow = columnIndex.get(colName);
						row = sheet.getRow(colRow);
						cell	= row.createCell(9);
						cell.setCellStyle(getCenterStyle());
						cell.setCellValue("☑");
					}else {
						System.err.println(colName + " not present in column list");
					}
				}
				
				rs.close();
				
				rs = dbCompareStructure.sourceDBMetaData.getPrimaryKeys(dbCompareStructure.sourceConnection.getSchemaName(), dbCompareStructure.sourceConnection.getSchemaName(), a_sourceTable.getKey());
				while(rs.next()) {
					String colName = rs.getString("COLUMN_NAME");
					if(columnIndex.containsKey(colName)) {
						int colRow = columnIndex.get(colName);
						row = sheet.getRow(colRow);
						cell	= row.createCell(16);
						cell.setCellStyle(getCenterStyle());
						cell.setCellValue("☑");
						
						tempCell = row.getCell(9);
						if(cell.getStringCellValue().equalsIgnoreCase(tempCell.getStringCellValue()) == false) {
							cell.setCellStyle(getCenterStyleError());
						}
					}else {
						System.err.println(colName + " not present in column list");
					}
				}
				
				
				row = sheet.createRow(rowCounter);
				cell = row.createCell(2);
				cell.setCellStyle(getStyleSeparator());
				sheet.addMergedRegion(new CellRangeAddress(rowCounter, rowCounter, 2, 16));
				
				rowCounter++;
			}//if check
		}

		System.out.println("Publisher.publishTableDetails(All tables are scanned)");
	}

	private void publishTableSummary() throws Throwable {
		System.out.println("Publisher.publishTableSummary()");

		// create a sheet in the workbook(you can give it a name)
		Sheet			sheet		= null;

		// create a row in the sheet
		Row				row			= null;

		// add cells in the sheet
		Cell			cell		= null;

		XSSFCellStyle	cellStyle	= getHeaderStyle();

		sheet = workbook.createSheet("Table Summary");
		sheet.setColumnWidth(1, 25 * 256);
		sheet.setColumnWidth(2, 10 * 256);
		sheet.setColumnWidth(3, 10 * 256);
		sheet.setColumnWidth(4, 10 * 256);
		sheet.setColumnWidth(5, 3 * 256);
		sheet.setColumnWidth(6, 10 * 256);
		sheet.setColumnWidth(7, 10 * 256);
		sheet.setColumnWidth(8, 10 * 256);
		
		row		= sheet.createRow(1);
		cell	= row.createCell(2);
		cell.setCellValue("Source");
		cell.setCellStyle(cellStyle);
		sheet.addMergedRegion(new CellRangeAddress(1, 1, 2, 4));
		RegionUtil.setBorderBottom(BorderStyle.THICK, new CellRangeAddress(1, 1, 2, 4), sheet);
		
		cell	= row.createCell(6);
		cell.setCellValue("Target");
		cell.setCellStyle(cellStyle);
		sheet.addMergedRegion(new CellRangeAddress(1, 1, 6, 8));
		RegionUtil.setBorderBottom(BorderStyle.THICK, new CellRangeAddress(1, 1, 6, 8), sheet);

		row		= sheet.createRow(2);

		cell	= row.createCell(1);
		cell.setCellValue("Table Name");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(2);
		cell.setCellValue("Presence");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(3);
		cell.setCellValue("Columns");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(4);
		cell.setCellValue("Rows");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(6);
		cell.setCellValue("Presence");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(7);
		cell.setCellValue("Columns");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(8);
		cell.setCellValue("Rows");
		cell.setCellStyle(cellStyle);

		int rowCounter = 3;
		
		for (String tableName : dbCompareStructure.sourceTables.keySet()) {
			row		= sheet.createRow(rowCounter++);

			cell	= row.createCell(1);
			cell.setCellValue(tableName);

			cell = row.createCell(2);
			cell.setCellValue("☑");
			cell.setCellStyle(getCenterStyle());

			cell = row.createCell(3);
			cell.setCellValue(dbCompareStructure.sourceTables.get(tableName).getColumnCount());

			cell = row.createCell(4);
			cell.setCellValue(dbCompareStructure.sourceTablesRow.get(tableName));

			cell = row.createCell(6);

			if (dbCompareStructure.targetTables.containsKey(tableName)) {
				cell.setCellValue("☑");
				cell.setCellStyle(getCenterStyle());

				cell = row.createCell(7);
				cell.setCellValue(dbCompareStructure.targetTables.get(tableName).getColumnCount());

				cell = row.createCell(8);
				cell.setCellValue(dbCompareStructure.targetTablesRow.get(tableName));
			} else {
				cell.setCellValue("☒");
				cell.setCellStyle(getCenterStyleError());
			}
		}

		for (String tableName : dbCompareStructure.targetTables.keySet()) {
			if (dbCompareStructure.sourceTables.containsKey(tableName)) {
				continue;
			}

			row		= sheet.createRow(rowCounter++);

			cell	= row.createCell(1);
			cell.setCellValue(tableName);

			cell = row.createCell(6);
			cell.setCellValue("☑");
			cell.setCellStyle(getCenterStyle());

			cell = row.createCell(7);
			cell.setCellValue(dbCompareStructure.targetTables.get(tableName).getColumnCount());

			cell = row.createCell(8);
			cell.setCellValue(dbCompareStructure.targetTablesRow.get(tableName));

			cell = row.createCell(2);
			cell.setCellValue("☒");
			cell.setCellStyle(getCenterStyleError());

		}
	}
	
	private XSSFCellStyle getStyleSeparator() {
		XSSFCellStyle	cellStyle	= ((XSSFWorkbook) workbook).createCellStyle();
		XSSFColor		color		= new XSSFColor(new java.awt.Color(251, 226, 213), new DefaultIndexedColorMap());
		cellStyle.setFillForegroundColor(color);
		cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

		return cellStyle;
	}
	
	private XSSFCellStyle getStyleError() {
		XSSFFont errorFont = ((XSSFWorkbook) workbook).createFont();
		errorFont.setColor(new XSSFColor(new java.awt.Color(156, 0, 6), new DefaultIndexedColorMap()));
		XSSFCellStyle	cellStyle	= ((XSSFWorkbook) workbook).createCellStyle();
		XSSFColor		color		= new XSSFColor(new java.awt.Color(255, 199, 206), new DefaultIndexedColorMap());
		cellStyle.setFillForegroundColor(color);
		cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		cellStyle.setFont(errorFont);

		return cellStyle;
	}

	private XSSFCellStyle getCenterStyleError() {
		XSSFFont errorFont = ((XSSFWorkbook) workbook).createFont();
		errorFont.setColor(new XSSFColor(new java.awt.Color(156, 0, 6), new DefaultIndexedColorMap()));
		XSSFCellStyle	cellStyle	= ((XSSFWorkbook) workbook).createCellStyle();
		XSSFColor		color		= new XSSFColor(new java.awt.Color(255, 199, 206), new DefaultIndexedColorMap());
		cellStyle.setFillForegroundColor(color);
		cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		cellStyle.setAlignment(HorizontalAlignment.CENTER);
		cellStyle.setFont(errorFont);

		return cellStyle;
	}

	private XSSFCellStyle getCenterStyle() {
		XSSFCellStyle cellStyle = ((XSSFWorkbook) workbook).createCellStyle();
		cellStyle.setAlignment(HorizontalAlignment.CENTER);

		return cellStyle;
	}

	private XSSFCellStyle getHeaderStyle() {
		XSSFFont headerFont = ((XSSFWorkbook) workbook).createFont();
		headerFont.setFontHeightInPoints((short) 12);
		headerFont.setColor(IndexedColors.BLACK.getIndex());
		headerFont.setBold(true);
		headerFont.setItalic(false);

		XSSFColor		color		= new XSSFColor(IndexedColors.GREY_25_PERCENT, null);
		XSSFCellStyle	cellStyle	= ((XSSFWorkbook) workbook).createCellStyle();
		cellStyle.setFillForegroundColor(color);
		cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		cellStyle.setAlignment(HorizontalAlignment.CENTER);
		cellStyle.setVerticalAlignment(VerticalAlignment.CENTER);
		cellStyle.setFont(headerFont);

		return cellStyle;
	}

	private void publishSummary() throws Throwable {
		System.out.println("Publisher.publishSummary()");

		// create a sheet in the workbook(you can give it a name)
		Sheet			sheet		= null;

		// create a row in the sheet
		Row				row			= null;

		// add cells in the sheet
		Cell			cell		= null;

		XSSFCellStyle	cellStyle	= getHeaderStyle();

		sheet = workbook.createSheet("Summary");
		sheet.setColumnWidth(5, 20 * 256);
		sheet.setColumnWidth(6, 40 * 256);
		sheet.setColumnWidth(7, 40 * 256);

		row		= sheet.createRow(8);

		cell	= row.createCell(6);
		cell.setCellValue("Source");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(7);
		cell.setCellValue("Target");
		cell.setCellStyle(cellStyle);

		row		= sheet.createRow(9);
		cell	= row.createCell(5);
		cell.setCellValue("Connection String");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(6);
		cell.setCellValue(dbCompareStructure.sourceConnection.getConnectionString());

		cell = row.createCell(7);
		cell.setCellValue(dbCompareStructure.targetConnection.getConnectionString());

		row		= sheet.createRow(10);
		cell	= row.createCell(5);
		cell.setCellValue("UserName");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(6);
		cell.setCellValue(dbCompareStructure.sourceConnection.getUserName());

		cell = row.createCell(7);
		cell.setCellValue(dbCompareStructure.targetConnection.getUserName());

		row		= sheet.createRow(11);
		cell	= row.createCell(5);
		cell.setCellValue("Password");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(6);
		cell.setCellValue(dbCompareStructure.sourceConnection.getPassword());

		cell = row.createCell(7);
		cell.setCellValue(dbCompareStructure.targetConnection.getPassword());

		row		= sheet.createRow(11);
		cell	= row.createCell(5);
		cell.setCellValue("Password");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(6);
		cell.setCellValue(dbCompareStructure.sourceConnection.getPassword());

		cell = row.createCell(7);
		cell.setCellValue(dbCompareStructure.targetConnection.getPassword());

		row		= sheet.createRow(12);
		cell	= row.createCell(5);
		cell.setCellValue("Table Count");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(6);
		cell.setCellValue(dbCompareStructure.sourceTables.size());

		cell = row.createCell(7);
		cell.setCellValue(dbCompareStructure.targetTables.size());

		row		= sheet.createRow(13);
		cell	= row.createCell(5);
		cell.setCellValue("Additional Tables");
		cell.setCellStyle(cellStyle);

		cell = row.createCell(6);
		cell.setCellValue(dbCompareStructure.onlySourceTables.size());

		cell = row.createCell(7);
		cell.setCellValue(dbCompareStructure.onlyTargetTables.size());
	}

	private void saveWorkbook() throws Throwable {
		String				timeStamp	= "";
		
		if("true".equalsIgnoreCase(applicationProperties.getProperty("addTimeStamp"))) {
			Calendar			now			= Calendar.getInstance();
			timeStamp	= "" + now.get(Calendar.YEAR) + (now.get(Calendar.MONTH) + 1)
					+ now.get(Calendar.DAY_OF_MONTH) + now.get(Calendar.HOUR_OF_DAY) + now.get(Calendar.MINUTE);
		}

		FileOutputStream	out			= new FileOutputStream(
				new File(applicationProperties.getProperty("reportPath") + "dbCompare" + timeStamp + ".xlsx"));
		workbook.write(out);
		out.close();
		System.out.println("Publisher.saveWorkbook(" + applicationProperties.getProperty("reportPath") + applicationProperties.getProperty("reportPath") + "dbCompare" + timeStamp + ".xlsx)");
	}

	public DBCompareStructure getDbCompareStructure() {
		return dbCompareStructure;
	}

	public void setDbCompareStructure(DBCompareStructure a_dbCompareStructure) {
		dbCompareStructure = a_dbCompareStructure;
	}
}
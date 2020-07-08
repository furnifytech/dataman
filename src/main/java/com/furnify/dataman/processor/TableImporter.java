package com.furnify.dataman.processor;

import com.furnify.dataman.constants.Constants;
import com.furnify.dataman.dao.TableDao;
import com.furnify.dataman.model.Column;
import com.furnify.dataman.model.Meta;
import com.furnify.dataman.model.Table;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TableImporter {
	
	private static final Logger logger = Logger.getLogger(TableImporter.class.getName());

	private TableImporter() {  }

	public static void processTable(Connection connection, Table table, String profile) {
		// Validate if Table Name contains Schema
		if (!table.getName().contains(".")) {
			logger.log(Level.SEVERE, "Table Name does not contain Schema Name. Skipping Table - " + table.getName());
			return;
		}

		String[] tableNameArr = table.getName().split("\\.");
		String schemaName = tableNameArr[0];
		String tableName = tableNameArr[1];

		try {
			// Check if the Table Exists
			if(TableDao.tableExists(connection, schemaName, tableName)) {
				Meta meta = table.getMeta();
				String[] keys = meta.getPk().split(",");
				String[] columns = meta.getCols().split(",");

				Meta metaType = table.getMetaType();
				String[] keyType = metaType.getPk().split(",");
				String[] columnType = metaType.getCols().split(",");

				if (keys.length != keyType.length) {
					logger.log(Level.SEVERE, "Primary Key Column count and Data Type count does not match");
					return;
				}

				if (columns.length != columnType.length) {
					logger.log(Level.SEVERE, "Column count and Data Type count does not match");
					return;
				}

				String exceptionColumns = table.getExceptionList();
				List exceptionList = new ArrayList();
				if (exceptionColumns != null) {
					String[] exceptionArr = exceptionColumns.split(",");
					exceptionList = Arrays.asList(exceptionArr);
				}

				Map<String, String> dataTypes = new HashMap<>();
				for (int i = 0; i < keys.length; i++) {
					dataTypes.put(keys[i], keyType[i]);
				}
				for (int i = 0; i < columns.length; i++) {
					dataTypes.put(columns[i], columnType[i]);
				}

				List<Column> defaultCols = table.getDefaultValue().getCol();
				Map<String, String> defaultValues = new HashMap<>();
				if (defaultCols != null && !defaultCols.isEmpty()) {
					for (Column column : defaultCols) {
						defaultValues.put(column.getName(), column.getValue());
					}
				}

				for (Map<String, String> row : table.getRows()) {
					if (row.get(Constants.PROFILE) != null && profile != null &&
							!profile.equalsIgnoreCase(row.get(Constants.PROFILE))) {
						continue;
					}

					StringBuilder stringBuilder = new StringBuilder();
					stringBuilder.append("SELECT * FROM ");
					stringBuilder.append(table.getName());

					appendWhereCondition(keys, row, stringBuilder);

					boolean recordExists = TableDao.recordExists(connection, stringBuilder.toString());
					stringBuilder = new StringBuilder();
					if (recordExists) {
						if (!table.isUpdatable()) {
							continue;
						}
						// Update Record
						stringBuilder.append("UPDATE ");
						stringBuilder.append(table.getName());
						stringBuilder.append(" SET ");

						boolean isFirst = true;
						for (String column : columns) {
							if (exceptionList.contains(column)) {
								continue;
							}
							if (!isFirst) {
								stringBuilder.append(",");
							}
							stringBuilder.append(column + " = ");

							boolean requiredQuotes = "varchar".equalsIgnoreCase(dataTypes.get(column));
							String data = row.get(column) == null ? defaultValues.get(column) : row.get(column);

							if (requiredQuotes && data != null) {
								stringBuilder.append("'");
							}

							stringBuilder.append(data);

							if (requiredQuotes && data != null) {
								stringBuilder.append("'");
							}
							isFirst = false;
						}

						appendWhereCondition(keys, row, stringBuilder);

						if (!isFirst) {
							TableDao.executeUpdate(connection, stringBuilder.toString());
						}
					} else {
						// Insert New Record
						stringBuilder.append("INSERT INTO ");
						stringBuilder.append(table.getName());
						stringBuilder.append(" ( ");

						String values = "";

						boolean isFirst = true;
						for (String key : keys) {
							if (!isFirst) {
								stringBuilder.append(",");
								values += ",";
							}
							stringBuilder.append(key);

							String data = row.get(key);
							if ("varchar".equalsIgnoreCase(dataTypes.get(key)) && data != null) {
								values += ("'"+data+"'");
							} else {
								values += (data);
							}

							isFirst = false;
						}

						for (String column : columns) {
							if (!isFirst) {
								stringBuilder.append("," + column);
							}

							String data = row.get(column) != null ? row.get(column) : defaultValues.get(column);

							if ("varchar".equalsIgnoreCase(dataTypes.get(column)) && data != null) {
								values += (",'" + data +"'");
							} else {
								values += ("," + data);
							}
						}

						stringBuilder.append(" ) VALUES ( ");
						stringBuilder.append(values + ")");

						TableDao.executeInsert(connection, stringBuilder.toString());
					}
				}
			} else {
				logger.log(Level.SEVERE, "Table does not Exist. Skipping Table - " + table.getName());
			}
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "Exception in Processing Table - " + table.getName(), e);
		}

		// Process
	}

	private static void appendWhereCondition(String[] keys, Map<String, String> row, StringBuilder stringBuilder) {
		stringBuilder.append(" WHERE ");

		boolean isFirst = true;
		for (String key : keys) {
			if (!isFirst) {
				stringBuilder.append(" AND ");
			}
			stringBuilder.append(key + " = '" + row.get(key) + "'");
			isFirst = false;
		}
	}

}

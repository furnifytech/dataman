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

	private final Connection connection;
	private final String profile;
	private final TableDao tableDao;

	public TableImporter(Connection connection, String profile) {
		this.connection = connection;
		this.profile = profile;
		this.tableDao = new TableDao(this.connection);
	}

	public void processTable(Table table) {
		try {
			String tableName = table.getName();
			String schemaName = connection.getCatalog();

			if (table.getName().contains(".")) {
				String[] tableNameArr = table.getName().split("\\.");
				schemaName = tableNameArr[0];
				tableName = tableNameArr[1];
			}

			if (logger.isLoggable(Level.INFO)) {
				logger.log(Level.INFO, String.format("Schema Name %s %s", schemaName, tableName));
			}

			// Check if the Table Exists
			if(tableDao.tableExists(schemaName, tableName)) {
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

				List<String> exceptionList = getExceptionList(table);

				Map<String, String> dataTypes = getDataTypes(keys, columns, keyType, columnType);

				Map<String, String> defaultValues = getDefaultColumns(table);

				for (Map<String, String> row : table.getRows(profile)) {
					processRow(table, keys, columns, exceptionList, dataTypes, defaultValues, row);
				}
			} else {
				if (logger.isLoggable(Level.SEVERE)) {
					logger.log(Level.SEVERE, String.format("Table does not Exist. Skipping Table - %s", table.getName()));
				}
			}
		} catch (SQLException e) {
			logger.log(Level.SEVERE, String.format("Exception in Processing Table %s ", table.getName()), e);
		}
	}

	private void processRow(Table table, String[] keys, String[] columns, List<String> exceptionList, Map<String, String> dataTypes, Map<String, String> defaultValues, Map<String, String> row) throws SQLException {
		if (row.get(Constants.PROFILE) != null && profile != null &&
				!profile.equalsIgnoreCase(row.get(Constants.PROFILE))) {
			return;
		}

		boolean recordExists = checkRecordExists(table, keys, row);

		if (recordExists) {
			if (table.isUpdatable()) {
				updateRecord(table, keys, columns, exceptionList, dataTypes, defaultValues, row);
			}
		} else {
			insertRecord(table, keys, columns, dataTypes, defaultValues, row);
		}
	}

	private void insertRecord(Table table, String[] keys, String[] columns, Map<String, String> dataTypes, Map<String, String> defaultValues, Map<String, String> row) throws SQLException {
		StringBuilder query = new StringBuilder();
		StringBuilder values = new StringBuilder();

		query.append("INSERT INTO " + table.getName() + " ( ");

		boolean isFirst = true;
		for (String key : keys) {
			if (!isFirst) {
				query.append(",");
				values.append(",");
			}

			query.append(key);

			String data = row.get(key);

			appendColumn(dataTypes, values, key, data);

			isFirst = false;
		}

		for (String column : columns) {
			if (!isFirst) {
				query.append(",");
				values.append(",");
			}

			query.append(column);

			String data = row.get(column) != null ? row.get(column) : defaultValues.get(column);

			appendColumn(dataTypes, values, column, data);

			isFirst = false;
		}

		query.append(" ) VALUES ( ");
		query.append(values.toString() + ")");

		tableDao.executeInsert(query.toString());
	}

	private void appendColumn(Map<String, String> dataTypes, StringBuilder values, String column, String data) {
		if (Constants.VARCHAR.equalsIgnoreCase(dataTypes.get(column)) && data != null) {
			values.append("'" + data + "'");
		} else {
			values.append(data);
		}
	}

	private void updateRecord(Table table, String[] keys, String[] columns, List<String> exceptionList, Map<String, String> dataTypes, Map<String, String> defaultValues, Map<String, String> row) throws SQLException {
		StringBuilder stringBuilder = new StringBuilder();
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

			boolean requiredQuotes = Constants.VARCHAR.equalsIgnoreCase(dataTypes.get(column));
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
			tableDao.executeUpdate(stringBuilder.toString());
		}
	}

	private boolean checkRecordExists(Table table, String[] keys, Map<String, String> row) throws SQLException {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("SELECT * FROM ");
		stringBuilder.append(table.getName());

		appendWhereCondition(keys, row, stringBuilder);

		return tableDao.recordExists(stringBuilder.toString());
	}

	private Map<String, String> getDefaultColumns(Table table) {
		if (table.getDefaultValue() == null || table.getDefaultValue().getCol().isEmpty()) {
			return new HashMap<>();
		}

		List<Column> defaultCols = table.getDefaultValue().getCol();
		Map<String, String> defaultValues = new HashMap<>();
		if (defaultCols != null && !defaultCols.isEmpty()) {
			for (Column column : defaultCols) {
				defaultValues.put(column.getName(), column.getValue());
			}
		}
		return defaultValues;
	}

	private Map<String, String> getDataTypes(String[] keys, String[] columns, String[] keyType, String[] columnType) {
		Map<String, String> dataTypes = new HashMap<>();
		for (int i = 0; i < keys.length; i++) {
			dataTypes.put(keys[i], keyType[i]);
		}
		for (int i = 0; i < columns.length; i++) {
			dataTypes.put(columns[i], columnType[i]);
		}
		return dataTypes;
	}

	private List<String> getExceptionList(Table table) {
		String exceptionColumns = table.getExceptionList();
		List<String> exceptionList = new ArrayList<>();
		if (exceptionColumns != null) {
			String[] exceptionArr = exceptionColumns.split(",");
			exceptionList = Arrays.asList(exceptionArr);
		}
		return exceptionList;
	}

	private void appendWhereCondition(String[] keys, Map<String, String> row, StringBuilder stringBuilder) {
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

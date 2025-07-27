import java.sql.*;
import java.io.*;
import java.util.*;

public class DbUser extends DbBasic {

	/**
	 * Constructor which receives the database's name
	 */
	DbUser(String dbName) {
		super(dbName);
	}

	/**
	 * Method to generate SQL backup of the database
	 */
	public void generateSqlBackup(String outputFilePath) {
		// Check if the database connection is valid
		if (con == null) {
			System.err.println("Database connection is not available.");
			return;
		}

		try {
			// Create a file to store the SQL statements
			FileWriter fw = new FileWriter(outputFilePath);
			BufferedWriter bw = new BufferedWriter(fw);

			// Get all table names
			DatabaseMetaData metaData = con.getMetaData();
			ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"});

			// Map to store table names and their foreign key dependencies
			Map<String, Set<String>> tableDependencies = new HashMap<>();
			List<String> tableNames = new ArrayList<>();

			// Collect table names and dependencies
			while (tables.next()) {
				String tableName = tables.getString("TABLE_NAME");
				// Check if table name is valid
				if (!isValidTableName(tableName)) {
					System.err.println("Invalid table name: " + tableName + ". Skipping this table.");
					continue;
				}
				tableNames.add(tableName);
				tableDependencies.put(tableName, getForeignKeyDependencies(tableName));
			}

			// Sort tables based on dependencies to ensure correct order
			List<String> sortedTableNames = sortTablesByDependencies(tableNames, tableDependencies);

			// Write CREATE TABLE statements
			for (String tableName : sortedTableNames) {
				writeCreateTable(bw, tableName);
			}

			// Write INSERT statements
			for (String tableName : sortedTableNames) {
				writeInsertStatements(bw, tableName);
			}

			// Write CREATE INDEX statements
			writeCreateIndexStatements(bw);

			// Close the file writer
			bw.close();
			System.out.println("SQL backup file generated successfully: " + outputFilePath);
		} catch (IOException e) {
			System.err.println("Error writing to file: " + e.getMessage());
		} catch (SQLException e) {
			System.err.println("Database error: " + e.getMessage());
		} catch (Exception e) {
			System.err.println("Unexpected error: " + e.getMessage());
		}
	}

	/**
	 * Write CREATE TABLE statement for a given table
	 */
	private void writeCreateTable(BufferedWriter bw, String tableName) throws IOException, SQLException {
		DatabaseMetaData metaData = con.getMetaData();
		ResultSet columns = metaData.getColumns(null, null, tableName, "%");

		bw.write("CREATE TABLE " + tableName + " (\n");
		List<String> columnDefs = new ArrayList<>();

		while (columns.next()) {
			String columnName = columns.getString("COLUMN_NAME");
			String columnType = columns.getString("TYPE_NAME");
			int dataSize = columns.getInt("COLUMN_SIZE");
			boolean isPrimaryKey = isPrimaryKey(tableName, columnName);

			String columnDef = columnName + " " + columnType;
			// Only append size for types that require it (like VARCHAR)
			if (columnType.equalsIgnoreCase("VARCHAR") && dataSize > 0) {
				columnDef += "(" + dataSize + ")";
			}
			if (isPrimaryKey) {
				columnDef += " PRIMARY KEY";
			}
			columnDefs.add(columnDef);
		}

		// Add primary key constraints
		ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, tableName);
		List<String> primaryKeyColumns = new ArrayList<>();
		while (primaryKeys.next()) {
			primaryKeyColumns.add(primaryKeys.getString("COLUMN_NAME"));
		}
		if (!primaryKeyColumns.isEmpty()) {
			columnDefs.add("PRIMARY KEY (" + String.join(", ", primaryKeyColumns) + ")");
		}

		// Add foreign key constraints
		Set<String> foreignKeys = getForeignKeyConstraints(tableName);
		columnDefs.addAll(foreignKeys);

		bw.write(String.join(",\n    ", columnDefs));
		bw.write("\n);\n\n");
	}

	/**
	 * Write INSERT statements for a given table
	 */
	private void writeInsertStatements(BufferedWriter bw, String tableName) throws IOException, SQLException {
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);

		ResultSetMetaData metaData = rs.getMetaData();
		int columnCount = metaData.getColumnCount();

		bw.write("-- Data for table " + tableName + "\n");
		while (rs.next()) {
			bw.write("INSERT INTO " + tableName + " VALUES (");
			for (int i = 1; i <= columnCount; i++) {
				Object value = rs.getObject(i);
				String columnType = metaData.getColumnTypeName(i);

				if (value == null) {
					bw.write("NULL");
				} else if (value instanceof String) {
					String stringValue = value.toString().replace("'", "''"); // Handle single quotes in strings
					bw.write("'" + stringValue + "'");
				} else if ("BLOB".equalsIgnoreCase(columnType)) {
					// Handle Binary Large Object
					InputStream inputStream = rs.getBinaryStream(i);
					if (inputStream != null) {
						byte[] buffer = new byte[1024];
						int bytesRead;
						ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
						while ((bytesRead = inputStream.read(buffer)) != -1) {
							byteArrayOutputStream.write(buffer, 0, bytesRead);
						}
						byte[] blobData = byteArrayOutputStream.toByteArray();
						bw.write("0x" + bytesToHex(blobData)); // Convert to hex string
					} else {
						bw.write("NULL");
					}
				} else {
					bw.write(value.toString());
				}

				if (i < columnCount) {
					bw.write(", ");
				}
			}
			bw.write(");\n");
		}
		bw.write("\n");
	}

	/**
	 * Convert byte array to hexadecimal string
	 */
	private String bytesToHex(byte[] bytes) {
		StringBuilder hexString = new StringBuilder();
		for (byte b : bytes) {
			String hex = Integer.toHexString(0xff & b);
			if (hex.length() == 1) {
				hexString.append('0');
			}
			hexString.append(hex);
		}
		return hexString.toString().toUpperCase();
	}

	/**
	 * Write CREATE INDEX statements
	 */
	private void writeCreateIndexStatements(BufferedWriter bw) throws IOException, SQLException {
		DatabaseMetaData metaData = con.getMetaData();
		ResultSet indexes = metaData.getIndexInfo(null, null, "%", false, false);

		bw.write("-- Indexes\n");
		while (indexes.next()) {
			String indexName = indexes.getString("INDEX_NAME");
			String tableName = indexes.getString("TABLE_NAME");
			String columnName = indexes.getString("COLUMN_NAME");
			bw.write("CREATE INDEX " + indexName + " ON " + tableName + " (" + columnName + ");\n");
		}
		bw.write("\n");
	}

	/**
	 * Check if a column is a primary key
	 */
	private boolean isPrimaryKey(String tableName, String columnName) throws SQLException {
		DatabaseMetaData metaData = con.getMetaData();
		ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, tableName);
		while (primaryKeys.next()) {
			if (primaryKeys.getString("COLUMN_NAME").equals(columnName)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Get foreign key dependencies for a table
	 */
	private Set<String> getForeignKeyDependencies(String tableName) throws SQLException {
		Set<String> dependencies = new HashSet<>();
		DatabaseMetaData metaData = con.getMetaData();
		ResultSet foreignKeys = metaData.getImportedKeys(null, null, tableName);


		while (foreignKeys.next()) {
			String referencedTable = foreignKeys.getString("PKTABLE_NAME");
			dependencies.add(referencedTable);
		}
		return dependencies;
	}

	/**
	 * Get foreign key constraints for a table
	 */
	private Set<String> getForeignKeyConstraints(String tableName) throws SQLException {
		Set<String> constraints = new HashSet<>();
		DatabaseMetaData metaData = con.getMetaData();
		ResultSet foreignKeys = metaData.getImportedKeys(null, null, tableName);
		while (foreignKeys.next()) {
			String columnName = foreignKeys.getString("FKCOLUMN_NAME");
			String referencedTable = foreignKeys.getString("PKTABLE_NAME");
			String referencedColumnName = foreignKeys.getString("PKCOLUMN_NAME");
			constraints.add("FOREIGN KEY (" + columnName + ") REFERENCES " + referencedTable + "(" + referencedColumnName + ")");
		}
		return constraints;
	}

	/**
	 * Sort tables based on foreign key dependencies
	 */
	private List<String> sortTablesByDependencies(List<String> tableNames, Map<String, Set<String>> tableDependencies) {
		List<String> sortedTables = new ArrayList<>();
		Set<String> visited = new HashSet<>();

		for (String table : tableNames) {
			if (!visited.contains(table)) {
				topologicalSort(table, tableDependencies, visited, sortedTables);
			}
		}

		return sortedTables;
	}

	private void topologicalSort(String table, Map<String, Set<String>> tableDependencies, Set<String> visited, List<String> sortedTables) {
		visited.add(table);
		for (String dependency : tableDependencies.get(table)) {
			if (!visited.contains(dependency)) {
				topologicalSort(dependency, tableDependencies, visited, sortedTables);
			}
		}
		sortedTables.add(table);
	}

	/**
	 * Validate table name
	 */
	private boolean isValidTableName(String tableName) {
		// Check if table name is null or empty
		if (tableName == null || tableName.trim().isEmpty()) {
			return false;
		}
		// Check if table name contains spaces or invalid characters
		if (!tableName.matches("^[a-zA-Z0-9_]+$")) {
			return false;
		}
		// Check if table name length exceeds the database limit (e.g., 64 characters for MySQL)
		if (tableName.length() > 64) {
			return false;
		}
		return true;
	}
}
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.jdbc4sparql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.impl.DataTable;
import org.xenei.jdbc4sparql.meta.MetaCatalog;
import org.xenei.jdbc4sparql.meta.MetaSchema;

public class J4SDatabaseMetaData implements DatabaseMetaData
{
	private final J4SConnection connection;
	private final J4SDriver driver;
	private static MetaCatalog metaCatalog;
	private static MetaSchema metaSchema;
	private final List<Catalog> catalogs;
	private static DataTable CATALOGS_TABLE;

	static
	{
		J4SDatabaseMetaData.metaCatalog = new MetaCatalog();
		J4SDatabaseMetaData.metaSchema = (MetaSchema) J4SDatabaseMetaData.metaCatalog
				.getSchema(MetaSchema.LOCAL_NAME);
		J4SDatabaseMetaData.CATALOGS_TABLE = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.CATALOGS_TABLE);
		J4SDatabaseMetaData.CATALOGS_TABLE
				.addData(new Object[] { J4SDatabaseMetaData.metaCatalog
						.getLocalName() });
	}

	public J4SDatabaseMetaData( final J4SConnection connection,
			final J4SDriver driver )
	{
		this.connection = connection;
		this.driver = driver;
		this.catalogs = new ArrayList<Catalog>();
	}

	public void addCatalog( final Catalog catalog )
	{
		J4SDatabaseMetaData.CATALOGS_TABLE.addData(new Object[] { catalog
				.getLocalName() });
		catalogs.add(J4SDatabaseMetaData.metaCatalog);
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deletesAreDetected( final int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ResultSet getAttributes( final String catalog,
			final String schemaPattern, final String typePattern,
			final String attributeNamePattern ) throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.ATTRIBUTES_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getBestRowIdentifier( final String arg0,
			final String arg1, final String arg2, final int arg3,
			final boolean arg4 ) throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.BEST_ROW_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getCatalogs() throws SQLException
	{
		return J4SDatabaseMetaData.CATALOGS_TABLE.getResultSet();
	}

	@Override
	public String getCatalogSeparator() throws SQLException
	{
		return ".";
	}

	@Override
	public String getCatalogTerm() throws SQLException
	{
		return "Catalog";
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.CLIENT_INFO_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getColumnPrivileges( final String arg0, final String arg1,
			final String arg2, final String arg3 ) throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.COLUMN_PRIVILIGES_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getColumns( final String catalogPattern,
			final String schemaPattern, final String tableNamePattern,
			final String columnNamePattern ) throws SQLException
	{

		final DataTable colTbl = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.COLUMNS_TABLE);

		for (final Catalog catalog : new NameFilter<Catalog>(catalogPattern,
				catalogs))
		{
			for (final Schema schema : new NameFilter<Schema>(schemaPattern,
					catalog.getSchemas()))
			{
				for (final Table table : new NameFilter<Table>(
						tableNamePattern, schema.getTables()))
				{
					for (final Column column : new NameFilter<Column>(
							columnNamePattern, table.getColumns()))
					{
						final Object[] data = new Object[] {
								catalog.getLocalName(), // TABLE_CAT
								schema.getLocalName(), // TABLE_SCHEMA
								table.getLocalName(), // TABLE_NAME
								column.getLocalName(), // COLUMN_NAME
								column.getType(), // DATA_TYPE
								null, // TYPE_NAME
								column.getDisplaySize(), // COLUMN_SIZE
								null, // BUFFER_LENGTH
								column.getPrecision(), // DECIMAL_DIGITS
								10, // NUM_PREC_RADIX
								column.getNullable(), // NULLABLE
								null, // REMARKS
								null, // COLUMN_DEF
								null, // SQL_DATA_TYPE
								null, // SQL_DATETIME_SUB
								1, // CHAR_OCTET_LENGTH
								table.getColumnIndex(column) + 1, // ORDINAL_POSITION
								"", // IS_NULLABLE
								null, // SCOPE_CATLOG
								null, // SCOPE_SCHEMA
								null, // SCOPE_TABLE
								null, // SOURCE_DATA_TYPE
								(column.isAutoIncrement() ? "YES" : "NO"), // IS_AUTOINCREMENT
						};
						colTbl.addData(data);
					}
				}
			}
		}
		return colTbl.getResultSet();
	}

	@Override
	public Connection getConnection() throws SQLException
	{
		return connection;
	}

	@Override
	public ResultSet getCrossReference( final String parentCatalog,
			final String parentSchema, final String parentTable,
			final String foreignCatalog, final String foreignSchema,
			final String foreignTable ) throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.XREF_TABLE);

		// TODO populate table here.
		return table.getResultSet();

	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException
	{
		return 1;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException
	{
		return 0;
	}

	@Override
	public String getDatabaseProductName() throws SQLException
	{
		return "JDBC4SPARQL";
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException
	{
		return "1";
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException
	{
		return java.sql.Connection.TRANSACTION_NONE;
	}

	@Override
	public int getDriverMajorVersion()
	{
		return driver.getMajorVersion();
	}

	@Override
	public int getDriverMinorVersion()
	{
		return driver.getMinorVersion();
	}

	@Override
	public String getDriverName() throws SQLException
	{
		return driver.getName();
	}

	@Override
	public String getDriverVersion() throws SQLException
	{
		return String.format("%s v%s.%s", driver.getName(),
				driver.getMajorVersion(), driver.getMinorVersion());
	}

	@Override
	public ResultSet getExportedKeys( final String arg0, final String arg1,
			final String arg2 ) throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.EXPORTED_KEYS_TABLE);
		// TODO populate table here.
		return table.getResultSet();

	}

	@Override
	public String getExtraNameCharacters() throws SQLException
	{
		return "";
	}

	@Override
	public ResultSet getFunctionColumns( final String arg0, final String arg1,
			final String arg2, final String arg3 ) throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.FUNCTION_COLUMNS_TABLE);
		// TODO populate table here.
		return table.getResultSet();

	}

	@Override
	public ResultSet getFunctions( final String arg0, final String arg1,
			final String arg2 ) throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.FUNCTIONS_TABLE);
		// TODO populate table here.
		return table.getResultSet();

	}

	@Override
	public String getIdentifierQuoteString() throws SQLException
	{
		return " ";
	}

	@Override
	public ResultSet getImportedKeys( final String arg0, final String arg1,
			final String arg2 ) throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.IMPORTED_KEYS_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getIndexInfo( final String arg0, final String arg1,
			final String arg2, final boolean arg3, final boolean arg4 )
			throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.INDEXINFO_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxConnections() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxRowSize() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxStatementLength() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxStatements() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getNumericFunctions() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getPrimaryKeys( final String arg0, final String arg1,
			final String arg2 ) throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.PRIMARY_KEY_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getProcedureColumns( final String arg0, final String arg1,
			final String arg2, final String arg3 ) throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.PROCEDURE_COLUMNS_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getProcedures( final String arg0, final String arg1,
			final String arg2 ) throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.PROCEDURES_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public String getProcedureTerm() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getPseudoColumns( final String arg0, final String arg1,
			final String arg2, final String arg3 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getResultSetHoldability() throws SQLException
	{
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getSchemas() throws SQLException
	{
		return getSchemas(null, null);
	}

	@Override
	public ResultSet getSchemas( final String catalogPattern,
			final String schemaPattern ) throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.SCHEMAS_TABLE);
		for (final Catalog catalog : new NameFilter<Catalog>(catalogPattern,
				catalogs))
		{
			for (final Schema schema : new NameFilter<Schema>(schemaPattern,
					catalog.getSchemas()))
			{
				table.addData(new Object[] { schema.getLocalName(),
						catalog.getLocalName() });
			}
		}
		return table.getResultSet();
	}

	@Override
	public String getSchemaTerm() throws SQLException
	{
		return "schema";
	}

	@Override
	public String getSearchStringEscape() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSQLKeywords() throws SQLException
	{
		return "";
	}

	@Override
	public int getSQLStateType() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getStringFunctions() throws SQLException
	{
		return "";
	}

	@Override
	public ResultSet getSuperTables( final String arg0, final String arg1,
			final String arg2 ) throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.SUPER_TABLES_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getSuperTypes( final String arg0, final String arg1,
			final String arg2 ) throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.SUPER_TYPES_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public String getSystemFunctions() throws SQLException
	{
		return "";
	}

	@Override
	public ResultSet getTablePrivileges( final String catalogPattern,
			final String schemaPattern, final String tablePattern )
			throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.TABLE_PRIVILEGES_TABLE);
		for (final Catalog catalog : new NameFilter<Catalog>(catalogPattern,
				catalogs))
		{
			for (final Schema schema : new NameFilter<Schema>(schemaPattern,
					catalog.getSchemas()))
			{
				for (final Table tbl : new NameFilter<Table>(tablePattern,
						schema.getTables()))
				{
					// TODO populate table here.
				}
			}
		}

		return table.getResultSet();
	}

	@Override
	public ResultSet getTables( final String catalogPattern,
			final String schemaPattern, final String tableNamePattern,
			final String[] types ) throws SQLException
	{
		final List<String> typeList = types == null ? null : Arrays
				.asList(types);

		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.TABLES_TABLE);
		for (final Catalog catalog : new NameFilter<Catalog>(catalogPattern,
				catalogs))
		{
			for (final Schema schema : new NameFilter<Schema>(schemaPattern,
					catalog.getSchemas()))
			{
				for (final Table tbl : new NameFilter<Table>(tableNamePattern,
						schema.getTables()))
				{
					if ((typeList == null) || typeList.contains(tbl.getType()))
					{
						final Object[] data = { catalog.getLocalName(), // TABLE_CAT
								schema.getLocalName(), // TABLE_SCHEM
								tbl.getLocalName(), // TABLE_NAME
								tbl.getType(), // TABLE_TYPE String => table
												// type. Typical types are
												// "TABLE", "VIEW",
												// "SYSTEM TABLE",
												// "GLOBAL TEMPORARY",
												// "LOCAL TEMPORARY", "ALIAS",
												// "SYNONYM".
								"", // REMARKS
								null, // TYPE_CAT
								null, // TYPE_SCHEM
								null, // TYPE_NAME
								null, // SELF_REFERENCING_COL_NAME
								null, // REF_GENERATION String
						};
						table.addData(data);
					}
				}
			}
		}
		return table.getResultSet();
	}

	@Override
	public ResultSet getTableTypes() throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.getTable(MetaSchema.TABLE_TYPES_TABLE);
		if (table.isEmpty())
		{
			for (final Catalog catalog : catalogs)
			{
				for (final Schema schema : catalog.getSchemas())
				{
					for (final Table tbl : schema.getTables())
					{
						table.addData(new Object[] { tbl.getType() });
					}
				}
			}
		}
		return table.getResultSet();
	}

	@Override
	public String getTimeDateFunctions() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.TYPEINFO_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getUDTs( final String arg0, final String arg1,
			final String arg2, final int[] arg3 ) throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.UDT_TABLES);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public String getURL() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getUserName() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getVersionColumns( final String arg0, final String arg1,
			final String arg2 ) throws SQLException
	{
		final DataTable table = (DataTable) J4SDatabaseMetaData.metaSchema
				.newTable(MetaSchema.VERSION_COLUMNS_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public boolean insertsAreDetected( final int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isReadOnly() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isWrapperFor( final Class<?> iface ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean othersDeletesAreVisible( final int arg0 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean othersInsertsAreVisible( final int arg0 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible( final int arg0 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean ownDeletesAreVisible( final int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean ownInsertsAreVisible( final int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean ownUpdatesAreVisible( final int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsConvert() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsConvert( final int arg0, final int arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions()
			throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly()
			throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsResultSetConcurrency( final int arg0, final int arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsResultSetHoldability( final int arg0 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsResultSetType( final int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsTransactionIsolationLevel( final int arg0 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsTransactions() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsUnion() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T unwrap( final Class<T> iface ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean updatesAreDetected( final int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean usesLocalFiles() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

}

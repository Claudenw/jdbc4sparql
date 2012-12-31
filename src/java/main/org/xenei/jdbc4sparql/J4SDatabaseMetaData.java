package org.xenei.jdbc4sparql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.impl.DataTable;
import org.xenei.jdbc4sparql.meta.MetaCatalog;
import org.xenei.jdbc4sparql.meta.MetaColumn;
import org.xenei.jdbc4sparql.meta.MetaSchema;

public class J4SDatabaseMetaData implements DatabaseMetaData
{
	private J4SConnection connection;
	private J4SDriver driver;
	private static MetaCatalog metaCatalog;
	private static MetaSchema metaSchema;
	private List<Catalog> catalogs;
	private static DataTable CATALOGS_TABLE;

	
	static {
		metaCatalog = new MetaCatalog();
		metaSchema = (MetaSchema) metaCatalog.getSchema( MetaSchema.LOCAL_NAME);
		CATALOGS_TABLE = (DataTable) metaSchema.newTable( MetaSchema.CATALOGS_TABLE );
		CATALOGS_TABLE.addData( new Object[] { metaCatalog.getLocalName() });
	}
	
	public J4SDatabaseMetaData(J4SConnection connection, J4SDriver driver)
	{
		this.connection = connection;
		this.driver = driver;
		this.catalogs= new ArrayList<Catalog>();
	}
	
	public void addCatalog(Catalog catalog)
	{
		CATALOGS_TABLE.addData(new Object[] { catalog.getLocalName() });
		catalogs.add( metaCatalog );
	}

	@Override
	public boolean isWrapperFor( Class<?> iface ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T unwrap( Class<T> iface ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
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
	public boolean deletesAreDetected( int arg0 ) throws SQLException
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
	public ResultSet getAttributes( String catalog, String schemaPattern, String typePattern,
			String attributeNamePattern ) throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.ATTRIBUTES_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getBestRowIdentifier( String arg0, String arg1,
			String arg2, int arg3, boolean arg4 ) throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.BEST_ROW_TABLE);
		// TODO populate table here.
		return table.getResultSet();
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
	public ResultSet getCatalogs() throws SQLException
	{
		return CATALOGS_TABLE.getResultSet();
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.CLIENT_INFO_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getColumnPrivileges( String arg0, String arg1,
			String arg2, String arg3 ) throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.COLUMN_PRIVILIGES_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}
	
	@Override
	public ResultSet getColumns( String catalogPattern, String schemaPattern, String tableNamePattern,
			String columnNamePattern ) throws SQLException
	{
		
		DataTable colTbl = (DataTable) metaSchema.newTable(MetaSchema.COLUMNS_TABLE);
		
		for (Catalog catalog : new NameFilter<Catalog>( catalogPattern, catalogs ))
		{
			for (Schema schema : new NameFilter<Schema>( schemaPattern, catalog.getSchemas()))
			{
				for (Table table : new NameFilter<Table>( tableNamePattern, schema.getTables()))
				{
					for (Column column : new NameFilter<Column>(columnNamePattern, table.getColumns()))
					{
						Object[] data = new Object[] {
								catalog.getLocalName(), // TABLE_CAT
								schema.getLocalName(), // TABLE_SCHEMA
								table.getLocalName(), // TABLE_NAME
								column.getLocalName(), // COLUMN_NAME
								column.getType(), //DATA_TYPE
								null, // TYPE_NAME
								column.getDisplaySize(), //COLUMN_SIZE
								null, // BUFFER_LENGTH
								column.getPrecision(), //DECIMAL_DIGITS
								10, // NUM_PREC_RADIX
								column.getNullable(), // NULLABLE
								null, //REMARKS
								null, // COLUMN_DEF
								null, // SQL_DATA_TYPE
								null, // SQL_DATETIME_SUB
								1, // CHAR_OCTET_LENGTH
								table.getColumnIndex(column)+1, //ORDINAL_POSITION
								"", // IS_NULLABLE
								null, // SCOPE_CATLOG
								null, // SCOPE_SCHEMA
								null, // SCOPE_TABLE
								null, // SOURCE_DATA_TYPE
								(column.isAutoIncrement()?"YES":"NO"), //IS_AUTOINCREMENT							
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
	public ResultSet getCrossReference( String parentCatalog, String parentSchema, 
			String parentTable,
			String foreignCatalog, String foreignSchema, String foreignTable ) throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.XREF_TABLE);

		
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
		return String.format( "%s v%s.%s", driver.getName(), driver.getMajorVersion(), driver.getMinorVersion() );
	}

	@Override
	public ResultSet getExportedKeys( String arg0, String arg1, String arg2 )
			throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.EXPORTED_KEYS_TABLE);
		// TODO populate table here.
		return table.getResultSet();

	}

	@Override
	public String getExtraNameCharacters() throws SQLException
	{
		return "";
	}

	@Override
	public ResultSet getFunctionColumns( String arg0, String arg1, String arg2,
			String arg3 ) throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.FUNCTION_COLUMNS_TABLE);
		// TODO populate table here.
		return table.getResultSet();

	}

	@Override
	public ResultSet getFunctions( String arg0, String arg1, String arg2 )
			throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.FUNCTIONS_TABLE);
		// TODO populate table here.
		return table.getResultSet();

	}

	@Override
	public String getIdentifierQuoteString() throws SQLException
	{
		return " ";
	}

	@Override
	public ResultSet getImportedKeys( String arg0, String arg1, String arg2 )
			throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.IMPORTED_KEYS_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getIndexInfo( String arg0, String arg1, String arg2,
			boolean arg3, boolean arg4 ) throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.INDEXINFO_TABLE);
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
	public ResultSet getPrimaryKeys( String arg0, String arg1, String arg2 )
			throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.PRIMARY_KEY_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getProcedureColumns( String arg0, String arg1,
			String arg2, String arg3 ) throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.PROCEDURE_COLUMNS_TABLE);
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
	public ResultSet getProcedures( String arg0, String arg1, String arg2 )
			throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.PROCEDURES_TABLE);
		// TODO populate table here.
		return table.getResultSet();
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
	public String getSchemaTerm() throws SQLException
	{
		return "schema";
	}

	@Override
	public ResultSet getSchemas() throws SQLException
	{
		return getSchemas(null,null);
	}

	@Override
	public ResultSet getSchemas( String catalogPattern, String schemaPattern ) throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable( MetaSchema.SCHEMAS_TABLE );
		for (Catalog catalog : new NameFilter<Catalog>( catalogPattern, catalogs ))
		{
			for (Schema schema : new NameFilter<Schema>(schemaPattern, catalog.getSchemas()))
			{
				table.addData( new Object[] { schema.getLocalName(), catalog.getLocalName() } );
			}
		}
		return table.getResultSet();
	}

	@Override
	public String getSearchStringEscape() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getStringFunctions() throws SQLException
	{
		return "";
	}

	@Override
	public ResultSet getSuperTables( String arg0, String arg1, String arg2 )
			throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.SUPER_TABLES_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getSuperTypes( String arg0, String arg1, String arg2 )
			throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.SUPER_TYPES_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public String getSystemFunctions() throws SQLException
	{
		return "";
	}

	@Override
	public ResultSet getTablePrivileges( String catalogPattern, String schemaPattern, String tablePattern )
			throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.TABLE_PRIVILEGES_TABLE);
		for (Catalog catalog : new NameFilter<Catalog>( catalogPattern, catalogs ))
		{
			for (Schema schema : new NameFilter<Schema>( schemaPattern, catalog.getSchemas()))
			{
				for (Table tbl : new NameFilter<Table>( tablePattern, schema.getTables()))
				{
					// TODO populate table here.
				}
			}
		}
		
		return table.getResultSet();
	}

	@Override
	public ResultSet getTableTypes() throws SQLException
	{
		DataTable table = (DataTable) metaSchema.getTable(MetaSchema.TABLE_TYPES_TABLE);
		if (table.isEmpty())
		{
			for (Catalog catalog : catalogs )
			{
				for (Schema schema : catalog.getSchemas())
				{
					for (Table tbl :schema.getTables())
					{
						table.addData( new Object[] { tbl.getType() } );
					}
				}
			}
		}
		return table.getResultSet();
	}

	@Override
	public ResultSet getTables( String catalogPattern, String schemaPattern, String tableNamePattern,
			String[] types ) throws SQLException
	{
		List<String> typeList = types==null?null:Arrays.asList(types);
		
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.TABLES_TABLE);
		for (Catalog catalog : new NameFilter<Catalog>( catalogPattern, catalogs ))
		{
			for (Schema schema : new NameFilter<Schema>( schemaPattern, catalog.getSchemas()))
			{
				for (Table tbl : new NameFilter<Table>( tableNamePattern, schema.getTables()))
				{
					if (typeList==null || typeList.contains(tbl.getType()))
					{
						Object[] data = {
								catalog.getLocalName(), //TABLE_CAT
								schema.getLocalName(), // TABLE_SCHEM
								tbl.getLocalName(), // TABLE_NAME
								tbl.getType(), //	TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
								"", //REMARKS
								null, //TYPE_CAT
								null, // TYPE_SCHEM 
								null, //TYPE_NAME
								null, //SELF_REFERENCING_COL_NAME
								null, // REF_GENERATION String	
						};
						table.addData( data );
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
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.TYPEINFO_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getUDTs( String arg0, String arg1, String arg2, int[] arg3 )
			throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.UDT_TABLES);
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
	public ResultSet getVersionColumns( String arg0, String arg1, String arg2 )
			throws SQLException
	{
		DataTable table = (DataTable) metaSchema.newTable(MetaSchema.VERSION_COLUMNS_TABLE);
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public boolean insertsAreDetected( int arg0 ) throws SQLException
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
	public boolean othersDeletesAreVisible( int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean othersInsertsAreVisible( int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible( int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean ownDeletesAreVisible( int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean ownInsertsAreVisible( int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean ownUpdatesAreVisible( int arg0 ) throws SQLException
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
	public boolean supportsConvert( int arg0, int arg1 ) throws SQLException
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
	public boolean supportsResultSetConcurrency( int arg0, int arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsResultSetHoldability( int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsResultSetType( int arg0 ) throws SQLException
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
	public boolean supportsTransactionIsolationLevel( int arg0 )
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
	public boolean updatesAreDetected( int arg0 ) throws SQLException
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

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ResultSet getPseudoColumns( String arg0, String arg1, String arg2,
			String arg3 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	
}

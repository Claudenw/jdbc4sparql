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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Key;
import org.xenei.jdbc4sparql.iface.KeySegment;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.DataTable;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.meta.MetaCatalogBuilder;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.SparqlResultSet;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlParserImpl;

public class J4SDatabaseMetaData implements DatabaseMetaData
{
	private final J4SConnection connection;
	private final J4SDriver driver;
	private final Catalog metaCatalog;
	private final Schema metaSchema;
	private final Map<String, Catalog> catalogs;
	private static Logger LOG = LoggerFactory
			.getLogger(J4SDatabaseMetaData.class);

	public J4SDatabaseMetaData( final J4SConnection connection,
			final J4SDriver driver )
	{
		this.connection = connection;
		this.driver = driver;
		this.catalogs = new HashMap<String, Catalog>(connection.getCatalogs());
		metaCatalog = catalogs.get(MetaCatalogBuilder.LOCAL_NAME);
		metaSchema =  metaCatalog
				.getSchema(MetaCatalogBuilder.SCHEMA_NAME);
		if (metaSchema == null)
		{
			throw new IllegalStateException( String.format( "Metadata schema '%s' not defined", MetaCatalogBuilder.SCHEMA_NAME));
		}
	}
	
	// TODO remvoe this
	public Catalog getCatalog( String name )
	{
		return catalogs.get(name);
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

	private String escapeString( final String s )
	{
		return s.replace("\\'", "'").replace("'", "\\'");
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
		final DataTable table = new DataTable(
				metaSchema.getTable(MetaCatalogBuilder.ATTRIBUTES_TABLE));
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getBestRowIdentifier( final String arg0,
			final String arg1, final String arg2, final int arg3,
			final boolean arg4 ) throws SQLException
	{
		final DataTable table = new DataTable(
				metaSchema.getTable(MetaCatalogBuilder.BEST_ROW_TABLE));
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getCatalogs() throws SQLException
	{
		J4SDatabaseMetaData.LOG.debug("Getting catalogs");
		final RdfTable table = (RdfTable) metaSchema
				.getTable(MetaCatalogBuilder.CATALOGS_TABLE);

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
	public ResultSet getClientInfoProperties() throws SQLException
	{
		final DataTable table = new DataTable(
				metaSchema.getTable(MetaCatalogBuilder.CLIENT_INFO_TABLE));
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getColumnPrivileges( final String arg0, final String arg1,
			final String arg2, final String arg3 ) throws SQLException
	{
		final DataTable table = new DataTable(
				metaSchema.getTable(MetaCatalogBuilder.COLUMN_PRIVILIGES_TABLE));
		// TODO populate table here.
		return table.getResultSet();
	}

	private String getCatalogName(String pattern)
	{
		if (pattern == null)
		{
			throw new IllegalArgumentException( "Catalog name may not be null");
		}
		if (StringUtils.isBlank(pattern))
		{
			return escapeString("");
		}
		return escapeString(pattern);
	}
	
	private String getSchemaName(String pattern)
	{
		if (pattern == null)
		{
			throw new IllegalArgumentException( "Schema name may not be null");
		}
		if (StringUtils.isBlank(pattern))
		{
			return escapeString("");
		}
		return escapeString(pattern);
	}
	
	@Override
	public ResultSet getColumns( final String catalogPattern,
			final String schemaPattern, final String tableNamePattern,
			final String columnNamePattern ) throws SQLException
	{
		if (J4SDatabaseMetaData.LOG.isDebugEnabled())
		{
			J4SDatabaseMetaData.LOG.debug(String.format(
					"Getting columns %s.%s.%s.%s", catalogPattern,
					schemaPattern, tableNamePattern, columnNamePattern));
		}
		final RdfTable table = (RdfTable) metaSchema
				.getTable(MetaCatalogBuilder.COLUMNS_TABLE);
		if ((catalogPattern != null) || (schemaPattern != null)
				|| (tableNamePattern != null) || (columnNamePattern != null))
		{

			boolean hasWhere = false;
			final StringBuilder query = new StringBuilder().append(String
					.format("SELECT * FROM %s WHERE ", table.getSQLName()));
			if (catalogPattern != null)
			{
				query.append(String.format("TABLE_CAT LIKE '%s'",
						getCatalogName(catalogPattern)));
				hasWhere = true;
			}
			if (schemaPattern != null)
			{
				query.append(hasWhere ? " AND " : "").append(
						String.format("TABLE_SCHEM LIKE '%s'",
								getSchemaName(schemaPattern)));
				hasWhere = true;
			}
			if (tableNamePattern != null)
			{
				query.append(hasWhere ? " AND " : "").append(
						String.format("TABLE_NAME LIKE '%s'",
								escapeString(tableNamePattern)));
				hasWhere = true;
			}
			if (columnNamePattern != null)
			{
				query.append(hasWhere ? " AND " : "").append(
						String.format("COLUMN_NAME LIKE '%s'",
								escapeString(columnNamePattern)));
				hasWhere = true;
			}
			final SparqlParser parser = new SparqlParserImpl();

			final SparqlQueryBuilder sqb = parser.parse(table.getCatalog(), table.getSchema(),
					query.toString()).setKey(table.getKey());
			return new SparqlResultSet(table, sqb.build());
		}
		else
		{
			return table.getResultSet();
		}
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
		final DataTable table = new DataTable(
				metaSchema.getTable(MetaCatalogBuilder.XREF_TABLE));

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
		final DataTable table = new DataTable(
				metaSchema.getTable(MetaCatalogBuilder.EXPORTED_KEYS_TABLE));
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
		final DataTable table = new DataTable(
				metaSchema.getTable(MetaCatalogBuilder.FUNCTION_COLUMNS_TABLE));
		// TODO populate table here.
		return table.getResultSet();

	}

	@Override
	public ResultSet getFunctions( final String arg0, final String arg1,
			final String arg2 ) throws SQLException
	{
		final DataTable table = new DataTable(
				metaSchema.getTable(MetaCatalogBuilder.FUNCTIONS_TABLE));
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
		final DataTable table = new DataTable(
				metaSchema.getTable(MetaCatalogBuilder.IMPORTED_KEYS_TABLE));
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getIndexInfo( final String arg0, final String arg1,
			final String arg2, final boolean arg3, final boolean arg4 )
			throws SQLException
	{
		final DataTable table = new DataTable(
				metaSchema.getTable(MetaCatalogBuilder.INDEXINFO_TABLE));
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
	public ResultSet getPrimaryKeys( final String catalogPattern,
			final String schemaPattern, final String tableNamePattern )
			throws SQLException
	{
		final DataTable table = new DataTable(
				metaSchema.getTable(MetaCatalogBuilder.PRIMARY_KEY_TABLE));
		for (final Catalog catalog : new NameFilter<Catalog>(catalogPattern,
				catalogs.values()))
		{
			for (final Schema schema : new NameFilter<Schema>(schemaPattern,
					catalog.getSchemas()))
			{
				for (final Table tbl : new NameFilter<Table>(tableNamePattern,
						schema.getTables()))
				{
					final TableDef tableDef = tbl.getTableDef();
					if (tableDef.getPrimaryKey() != null)
					{
						final Key pk = tableDef.getPrimaryKey();
						for (final KeySegment seg : pk.getSegments())
						{
							final Object[] data = { catalog.getName(), // TABLE_CAT
									schema.getName(), // TABLE_SCHEM
									tbl.getName(), // TABLE_NAME
									tbl.getColumn(seg.getIdx()).getName(), // COLUMN_NAME
									new Short((short) (seg.getIdx() + 1)), // KEY_SEQ
									pk.getKeyName() // PK_NAME
							};
							table.addData(data);
						}
					}
				}
			}
		}
		return table.getResultSet();
	}

	@Override
	public ResultSet getProcedureColumns( final String arg0, final String arg1,
			final String arg2, final String arg3 ) throws SQLException
	{
		final DataTable table = new DataTable(
				metaSchema.getTable(MetaCatalogBuilder.PROCEDURE_COLUMNS_TABLE));
		// TODO populate table here.
		return table.getResultSet();
	}

	@Override
	public ResultSet getProcedures( final String arg0, final String arg1,
			final String arg2 ) throws SQLException
	{
		final DataTable table = new DataTable(
				metaSchema.getTable(MetaCatalogBuilder.PROCEDURES_TABLE));
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
		if (J4SDatabaseMetaData.LOG.isDebugEnabled())
		{
			J4SDatabaseMetaData.LOG.debug(String.format(
					"getting schemas %s.%s", catalogPattern, schemaPattern));
		}
		final RdfTable table = (RdfTable) metaSchema
				.getTable(MetaCatalogBuilder.SCHEMAS_TABLE);
		if ((catalogPattern != null) || (schemaPattern != null))
		{
			boolean hasWhere = false;
			final StringBuilder query = new StringBuilder().append(String
					.format("SELECT * FROM %s WHERE ", table.getSQLName()));
			if (catalogPattern != null)
			{
				query.append(String.format("TABLE_CATALOG LIKE '%s'",
						getCatalogName(catalogPattern)));
				hasWhere = true;
			}
			if (schemaPattern != null)
			{
				query.append(hasWhere ? " AND " : "").append(
						String.format("TABLE_SCHEM LIKE '%s'",
								getSchemaName(schemaPattern)));
				hasWhere = true;
			}

			final SparqlParser parser = new SparqlParserImpl();
			final SparqlQueryBuilder sqb = parser.parse(table.getCatalog(), table.getSchema(),
					query.toString()).setKey(table.getKey());
			return new SparqlResultSet(table, sqb.build());
		}
		else
		{
			return table.getResultSet();
		}
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
	public ResultSet getSuperTables( final String catalogP,
			final String schemaP, final String tableNameP ) throws SQLException
	{
		String catalogPattern = catalogP;
		String schemaPattern = schemaP;
		String tableNamePattern = tableNameP;
		if (tableNameP.contains("."))
		{
			final String[] parts = tableNameP.split("\\.");
			if (parts.length > 3)
			{
				throw new SQLException(String.format("Invalid tableName: %s",
						tableNameP));
			}
			if (parts.length == 3)
			{
				catalogPattern = parts[0];
				schemaPattern = parts[1];
				tableNamePattern = parts[2];
			}
			if (parts.length == 2)
			{
				schemaPattern = parts[0];
				tableNamePattern = parts[1];
			}
		}
		final DataTable table = new DataTable(
				metaSchema.getTable(MetaCatalogBuilder.SUPER_TABLES_TABLE));

		for (final Catalog catalog : new NameFilter<Catalog>(catalogPattern,
				catalogs.values()))
		{
			for (final Schema schema : new NameFilter<Schema>(schemaPattern,
					catalog.getSchemas()))
			{
				for (final Table tbl : new NameFilter<Table>(tableNamePattern,
						schema.getTables()))
				{
					if (tbl.getSuperTable() != null)
					{
						final Object[] data = { catalog.getName(), // TABLE_CAT
								schema.getName(), // TABLE_SCHEM
								tbl.getName(), // TABLE_NAME
								tbl.getSuperTable().getName(), // SUPERTABLE_NAME
						};
						table.addData(data);
					}
				}
			}
		}
		return table.getResultSet();
	}

	@Override
	public ResultSet getSuperTypes( final String arg0, final String arg1,
			final String arg2 ) throws SQLException
	{
		final DataTable table = new DataTable(
				metaSchema.getTable(MetaCatalogBuilder.SUPER_TYPES_TABLE));
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
		final DataTable table = new DataTable(
				metaSchema.getTable(MetaCatalogBuilder.TABLE_PRIVILEGES_TABLE));
		for (final Catalog catalog : new NameFilter<Catalog>(catalogPattern,
				catalogs.values()))
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
		if (J4SDatabaseMetaData.LOG.isDebugEnabled())
		{
			J4SDatabaseMetaData.LOG.debug(String.format(
					"getting tables %s.%s.%s types %s", catalogPattern,
					schemaPattern, tableNamePattern, types));
		}
		final RdfTable table = (RdfTable) metaSchema
				.getTable(MetaCatalogBuilder.TABLES_TABLE);
		if ((catalogPattern != null) || (schemaPattern != null)
				|| (tableNamePattern != null) || (types != null))
		{
			boolean hasWhere = false;
			final StringBuilder query = new StringBuilder().append(String
					.format("SELECT * FROM %s WHERE ", table.getSQLName()));
			if (catalogPattern != null)
			{
				query.append(String.format("TABLE_CAT LIKE '%s'",
						getCatalogName(catalogPattern)));
				hasWhere = true;
			}
			if (schemaPattern != null)
			{
				query.append(hasWhere ? " AND " : "").append(
						String.format("TABLE_SCHEM LIKE '%s'",
								getSchemaName(schemaPattern)));
				hasWhere = true;
			}
			if (tableNamePattern != null)
			{
				query.append(hasWhere ? " AND " : "").append(
						String.format("TABLE_NAME LIKE '%s'",
								escapeString(tableNamePattern)));
				hasWhere = true;
			}
			if ((types != null) && (types.length > 0))
			{
				query.append(hasWhere ? " AND (" : "(");
				for (int i = 0; i < types.length; i++)
				{
					query.append(i > 0 ? " OR " : "")
							.append(types.length > 1 ? "(" : "")
							.append(String.format("TABLE_TYPE LIKE '%s'",
									escapeString(types[i])))
							.append(types.length > 1 ? ")" : "");
				}
				query.append(")");

				hasWhere = true;
			}

			final SparqlParser parser = new SparqlParserImpl();
			final SparqlQueryBuilder sqb = parser.parse(table.getCatalog(), table.getSchema(),
					query.toString()).setKey(table.getKey());
			return new SparqlResultSet(table, sqb.build());
		}
		else
		{
			return table.getResultSet();
		}
	}

	@Override
	public ResultSet getTableTypes() throws SQLException
	{
		return ((RdfTable) metaSchema
				.getTable(MetaCatalogBuilder.TABLE_TYPES_TABLE)).getResultSet();
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
		return ((RdfTable) metaSchema
				.getTable(MetaCatalogBuilder.TYPEINFO_TABLE)).getResultSet();
	}

	@Override
	public ResultSet getUDTs( final String catalog, final String schemaPattern,
			final String typeNamePattern, final int[] types )
			throws SQLException
	{
		final DataTable table = new DataTable(
				metaSchema.getTable(MetaCatalogBuilder.UDT_TABLES));
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
		final DataTable table = new DataTable(
				metaSchema.getTable(MetaCatalogBuilder.VERSION_COLUMNS_TABLE));
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
		return true;
	}

	@Override
	public boolean isReadOnly() throws SQLException
	{
		return true;
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
		return true;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException
	{
		return true;
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
		return true;
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
		return true;
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
		switch (arg0) {
			case ResultSet.TYPE_FORWARD_ONLY:
			case ResultSet.CONCUR_READ_ONLY:
			case ResultSet.TYPE_SCROLL_INSENSITIVE:
			case ResultSet.HOLD_CURSORS_OVER_COMMIT:
				return true;
			
			case ResultSet.CLOSE_CURSORS_AT_COMMIT:
			case ResultSet.CONCUR_UPDATABLE:
			case ResultSet.TYPE_SCROLL_SENSITIVE:
			default:
				return false;
		}
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
		LOG.debug( "supportsSchemasInDataManipulation: true ");
		return true;
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
		LOG.debug( "supportsSchemasInTableDefinitions: true ");
		return true;
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

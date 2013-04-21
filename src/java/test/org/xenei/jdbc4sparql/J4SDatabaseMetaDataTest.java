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
 */package org.xenei.jdbc4sparql;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.meta.MetaCatalog;
import org.xenei.jdbc4sparql.meta.MetaSchema;
import org.xenei.jdbc4sparql.mock.MockConnection;
import org.xenei.jdbc4sparql.mock.MockDriver;

public class J4SDatabaseMetaDataTest
{

	private J4SDatabaseMetaData metadata;

	private void columnChecking( final String tableName,
			final String[] columnNames ) throws SQLException
	{
		final ResultSet rs = metadata.getColumns(null, null, tableName, null);
		for (int i = 0; i < columnNames.length; i++)
		{
			Assert.assertTrue(rs.next());
			Assert.assertEquals(MetaCatalog.LOCAL_NAME, rs.getString(1)); // TABLE_CAT
			Assert.assertEquals(MetaSchema.LOCAL_NAME, rs.getString(2)); // TABLE_SCHEM
			Assert.assertEquals(tableName, rs.getString(3)); // TABLE_NAME
			if (!columnNames[i].equals("reserved"))
			{
				Assert.assertEquals(columnNames[i], rs.getString(4)); // COLUMN_NAME
			}
		}
		Assert.assertFalse(rs.next());
	}

	@Before
	public void setup() throws IOException
	{
		final MockDriver driver = new MockDriver();
		metadata = new J4SDatabaseMetaData(new MockConnection(driver, null),
				driver);
		metadata.addCatalog(new MetaCatalog());

	}

	@Test
	public void testAttributesDef() throws SQLException
	{
		final String[] names = { "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
				"ATTR_NAME", "DATA_TYPE", "ATTR_TYPE_NAME", "ATTR_SIZE",
				"DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS",
				"ATTR_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
				"CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
				"SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
				"SOURCE_DATA_TYPE",

		};
		columnChecking(MetaSchema.ATTRIBUTES_TABLE, names);
	}

	@Test
	public void testBestRowIdentifierDef() throws SQLException
	{
		final String[] names = { "SCOPE", "COLUMN_NAME", "DATA_TYPE",
				"TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
				"PSEUDO_COLUMN", };
		columnChecking(MetaSchema.BEST_ROW_TABLE, names);
	}

	@Test
	public void testCatalogsDef() throws SQLException
	{
		final String[] names = { "TABLE_CAT", };
		columnChecking(MetaSchema.CATALOGS_TABLE, names);
	}

	@Test
	public void testClientInfoPropertiesDef() throws SQLException
	{
		final String[] names = { "NAME", "MAX_LEN", "DEFAULT_VALUE",
				"DESCRIPTION",

		};
		columnChecking(MetaSchema.CLIENT_INFO_TABLE, names);
	}

	@Test
	public void testColumnPrivilegesDef() throws SQLException
	{
		final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"COLUMN_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE",
				"IS_GRANTABLE", };
		columnChecking(MetaSchema.COLUMN_PRIVILIGES_TABLE, names);
	}

	@Test
	public void testColumnsDef() throws SQLException
	{
		final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE",
				"BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
				"NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE",
				"SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
				"IS_NULLABLE", "SCOPE_CATLOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
				"SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", };
		columnChecking(MetaSchema.COLUMNS_TABLE, names);
	}

	@Test
	public void testCrossReferenceDef() throws SQLException
	{
		final String[] names = { "PKTABLE_CAT", "PKTABLE_SCHEM",
				"PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
				"FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ",
				"UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME",
				"DEFERRABILITY", };
		columnChecking(MetaSchema.XREF_TABLE, names);
	}

	@Test
	public void testExportedKeysDef() throws SQLException
	{
		final String[] names = { "PKTABLE_CAT", "PKTABLE_SCHEM",
				"PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
				"FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ",
				"UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME",
				"DEFERRABILITY", };
		columnChecking(MetaSchema.EXPORTED_KEYS_TABLE, names);
	}

	@Test
	public void testFunctionColumnsDef() throws SQLException
	{
		final String[] names = { "FUNCTION_CAT", "FUNCTION_SCHEM",
				"FUNCTION_NAME", "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE",
				"TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX",
				"NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
				"IS_NULLABLE", "SPECIFIC_NAME", };
		columnChecking(MetaSchema.FUNCTION_COLUMNS_TABLE, names);
	}

	@Test
	public void testFunctionsDef() throws SQLException
	{
		final String[] names = { "FUNCTION_CAT", "FUNCTION_SCHEM",
				"FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME",

		};
		columnChecking(MetaSchema.FUNCTIONS_TABLE, names);
	}

	@Test
	public void testGetAllTables() throws SQLException
	{
		final String[] names = { "Attributes", "BestRow", "Catalogs",
				"ClientInfo", "ColumnPriviliges", "Columns", "ExportedKeys",
				"FunctionColumns", "Functions", "ImportedKeys", "IndexInfo",
				"PrimaryKeys", "ProcedureColumns", "Procedures", "Schemas",
				"SuperTables", "SuperTypes", "TablePriv", "TableTypes",
				"Tables", "TypeInfo", "UDTs", "Version", "XrefKeys" };

		final ResultSet rs = metadata.getTables(null, null, null, null);

		for (final String name : names)
		{
			Assert.assertTrue(rs.next());
			Assert.assertEquals(MetaCatalog.LOCAL_NAME, rs.getString(1)); // TABLE_CAT
			Assert.assertEquals(MetaSchema.LOCAL_NAME, rs.getString(2)); // TABLE_SCHEM
			Assert.assertEquals(name, rs.getString(3)); // TABLE_NAME
			Assert.assertEquals("TABLE", rs.getString(4)); // TABLE_TYPE
			Assert.assertEquals("", rs.getString(5)); // REMARKS
		}
		Assert.assertFalse(rs.next());

	}

	@Test
	public void testImportedKeysDef() throws SQLException
	{
		final String[] names = { "PKTABLE_CAT", "PKTABLE_SCHEM",
				"PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
				"FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ",
				"UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME",
				"DEFERRABILITY", };
		columnChecking(MetaSchema.IMPORTED_KEYS_TABLE, names);
	}

	@Test
	public void testIndexInfoDef() throws SQLException
	{
		final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME", "TYPE",
				"ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
				"CARDINALITY", "PAGES", "FILTER_CONDITION", };
		columnChecking(MetaSchema.INDEXINFO_TABLE, names);
	}

	@Test
	public void testOneTable() throws SQLException
	{
		final ResultSet rs = metadata.getTables(null, null,
				MetaSchema.COLUMNS_TABLE, null);
		Assert.assertTrue(rs.next());
		Assert.assertEquals(MetaCatalog.LOCAL_NAME, rs.getString(1)); // TABLE_CAT
		Assert.assertEquals(MetaSchema.LOCAL_NAME, rs.getString(2)); // TABLE_SCHEM
		Assert.assertEquals(MetaSchema.COLUMNS_TABLE, rs.getString(3)); // TABLE_NAME
		Assert.assertEquals("TABLE", rs.getString(4)); // TABLE_TYPE
		Assert.assertEquals("", rs.getString(5)); // REMARKS
		Assert.assertFalse(rs.next());
	}

	@Test
	public void testPrimaryKeysDef() throws SQLException
	{
		final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"COLUMN_NAME", "KEY_SEQ", "PK_NAME", };
		columnChecking(MetaSchema.PRIMARY_KEY_TABLE, names);
	}

	@Test
	public void testProcedureColumnsDef() throws SQLException
	{
		final String[] names = { "PROCEDURE_CAT", "PROCEDURE_SCHEM",
				"PROCEDURE_NAME", "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE",
				"TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX",
				"NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE",
				"SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
				"IS_NULLABLE", "SPECIFIC_NAME", };
		columnChecking(MetaSchema.PROCEDURE_COLUMNS_TABLE, names);
	}

	@Test
	public void testProceduresDef() throws SQLException
	{
		final String[] names = { "PROCEDURE_CAT", "PROCEDURE_SCHEM",
				"PROCEDURE_NAME", "reserved", "reserved", "reserved",
				"REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME", };
		columnChecking(MetaSchema.PROCEDURES_TABLE, names);
	}

	@Test
	public void testSchemasDef() throws SQLException
	{
		final String[] names = { "TABLE_SCHEM", "TABLE_CATALOG", };
		columnChecking(MetaSchema.SCHEMAS_TABLE, names);
	}

	@Test
	public void testSuperTablesDef() throws SQLException
	{
		final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"SUPERTABLE_NAME",

		};
		columnChecking(MetaSchema.SUPER_TABLES_TABLE, names);
	}

	@Test
	public void testSuperTypesDef() throws SQLException
	{
		final String[] names = { "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
				"SUPERTYPE_CAT", "SUPERTYPE_SCHEM", "SUPERTYPE_NAME",

		};
		columnChecking(MetaSchema.SUPER_TYPES_TABLE, names);
	}

	@Test
	public void testTablePrivilegesDef() throws SQLException
	{
		final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE", };
		columnChecking(MetaSchema.TABLE_PRIVILEGES_TABLE, names);
	}

	@Test
	public void testTablesDef() throws SQLException
	{
		final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
				"SELF_REFERENCING_COL_NAME", "REF_GENERATION", };
		columnChecking(MetaSchema.TABLES_TABLE, names);
	}

	@Test
	public void testTableTypesDef() throws SQLException
	{
		final String[] names = { "TABLE_TYPE", };
		columnChecking(MetaSchema.TABLE_TYPES_TABLE, names);
	}

	@Test
	public void testTypeInfoDef() throws SQLException
	{
		final String[] names = { "TYPE_NAME", "DATA_TYPE", "PRECISION",
				"LITERAL_PREFIX", "LITERAL_SUFFIX", "CREATE_PARAMS",
				"NULLABLE", "CASE_SENSITIVE", "SEARCHABLE",
				"UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT",
				"LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE",
				"SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX", };
		columnChecking(MetaSchema.TYPEINFO_TABLE, names);
	}

	@Test
	public void testUDTDef() throws SQLException
	{
		final String[] names = { "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
				"CLASS_NAME", "DATA_TYPE", "REMARKS", "BASE_TYPE", };
		columnChecking(MetaSchema.UDT_TABLES, names);
	}

	@Test
	public void testVersionColumnsDef() throws SQLException
	{
		final String[] names = { "SCOPE", "COLUMN_NAME", "DATA_TYPE",
				"TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
				"PSEUDO_COLUMN", };
		columnChecking(MetaSchema.VERSION_COLUMNS_TABLE, names);
	}
}
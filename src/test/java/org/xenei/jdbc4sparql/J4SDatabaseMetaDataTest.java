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

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.meta.MetaCatalogBuilder;

public class J4SDatabaseMetaDataTest
{
	private J4SConnection connection;
	private J4SDatabaseMetaData metadata;
	private Model model;

	private void columnChecking( final String tableName,
			final String[] columnNames ) throws SQLException
	{
		final ResultSet rs = metadata.getColumns(MetaCatalogBuilder.LOCAL_NAME,
				MetaCatalogBuilder.SCHEMA_LOCAL_NAME, tableName, null);
		for (int i = 0; i < columnNames.length; i++)
		{
			Assert.assertTrue("column " + i + " of table " + tableName
					+ " missing", rs.next());
			Assert.assertEquals(MetaCatalogBuilder.LOCAL_NAME, rs.getString(1)); // TABLE_CAT
			Assert.assertEquals(MetaCatalogBuilder.SCHEMA_LOCAL_NAME,
					rs.getString(2)); // TABLE_SCHEM
			Assert.assertEquals(tableName, rs.getString(3)); // TABLE_NAME
			if (!columnNames[i].equals("reserved"))
			{
				Assert.assertEquals(columnNames[i], rs.getString(4)); // COLUMN_NAME
			}
		}
		Assert.assertFalse("Extra column found in table " + tableName,
				rs.next());
	}

	@Before
	public void setup() throws IOException, InstantiationException,
			IllegalAccessException, ClassNotFoundException
	{
		final J4SDriver driver = new J4SDriver();
		model = ModelFactory.createDefaultModel();
		// Mockito.when( mockUrl.getType()).thenReturn("TURTLE");
		connection = Mockito.mock(J4SConnection.class);
		final Map<String, Catalog> catalogs = new HashMap<String, Catalog>();
		final Catalog cat = MetaCatalogBuilder.getInstance(model);
		catalogs.put(cat.getName(), cat);
		Mockito.when(connection.getCatalogs()).thenReturn(catalogs);
		metadata = new J4SDatabaseMetaData(connection, driver);
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
				"SOURCE_DATA_TYPE" };
		columnChecking(MetaCatalogBuilder.ATTRIBUTES_TABLE, names);
	}

	@Test
	public void testBestRowIdentifierDef() throws SQLException
	{
		final String[] names = { "SCOPE", "COLUMN_NAME", "DATA_TYPE",
				"TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
				"PSEUDO_COLUMN", };
		columnChecking(MetaCatalogBuilder.BEST_ROW_TABLE, names);
	}

	@Test
	public void testCatalogsDef() throws SQLException
	{
		final String[] names = { "TABLE_CAT", };
		columnChecking(MetaCatalogBuilder.CATALOGS_TABLE, names);
	}

	@Test
	public void testClientInfoPropertiesDef() throws SQLException
	{
		final String[] names = { "NAME", "MAX_LEN", "DEFAULT_VALUE",
				"DESCRIPTION",

		};
		columnChecking(MetaCatalogBuilder.CLIENT_INFO_TABLE, names);
	}

	@Test
	public void testColumnPrivilegesDef() throws SQLException
	{
		final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"COLUMN_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE",
				"IS_GRANTABLE", };
		columnChecking(MetaCatalogBuilder.COLUMN_PRIVILIGES_TABLE, names);
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
		columnChecking(MetaCatalogBuilder.COLUMNS_TABLE, names);
	}

	@Test
	public void testColumnsTable() throws SQLException
	{
		ResultSet rs = metadata.getColumns(null, null, null, null);
		Assert.assertTrue(rs.first());
		while (!rs.isAfterLast())
		{
			System.out
					.println(String.format("%s : %s : %s : %s : %d : %s",
							rs.getString("TABLE_CAT"),
							rs.getString("TABLE_SCHEM"),
							rs.getString("TABLE_NAME"),
							rs.getString("COLUMN_NAME"),
							rs.getInt("ORDINAL_POSITION"),
							rs.getString("IS_NULLABLE")));
			rs.next();
		}

		rs = metadata.getColumns(MetaCatalogBuilder.LOCAL_NAME,
				MetaCatalogBuilder.SCHEMA_LOCAL_NAME, "Columns", null);
		Assert.assertTrue(rs.first());
		while (!rs.isAfterLast())
		{
			System.out
					.println(String.format("%s : %s : %s : %s : %d : %s",
							rs.getString("TABLE_CAT"),
							rs.getString("TABLE_SCHEM"),
							rs.getString("TABLE_NAME"),
							rs.getString("COLUMN_NAME"),
							rs.getInt("ORDINAL_POSITION"),
							rs.getString("IS_NULLABLE")));
			rs.next();
		}

		rs = metadata.getColumns(MetaCatalogBuilder.LOCAL_NAME,
				MetaCatalogBuilder.SCHEMA_LOCAL_NAME, null, "TABLE_CAT");
		Assert.assertTrue(rs.first());
		while (!rs.isAfterLast())
		{
			System.out
					.println(String.format("%s : %s : %s : %s : %d : %s",
							rs.getString("TABLE_CAT"),
							rs.getString("TABLE_SCHEM"),
							rs.getString("TABLE_NAME"),
							rs.getString("COLUMN_NAME"),
							rs.getInt("ORDINAL_POSITION"),
							rs.getString("IS_NULLABLE")));
			rs.next();
		}

	}

	@Test
	public void testCrossReferenceDef() throws SQLException
	{
		final String[] names = { "PKTABLE_CAT", "PKTABLE_SCHEM",
				"PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
				"FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ",
				"UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME",
				"DEFERRABILITY", };
		columnChecking(MetaCatalogBuilder.XREF_TABLE, names);
	}

	@Test
	public void testExportedKeysDef() throws SQLException
	{
		final String[] names = { "PKTABLE_CAT", "PKTABLE_SCHEM",
				"PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
				"FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ",
				"UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME",
				"DEFERRABILITY", };
		columnChecking(MetaCatalogBuilder.EXPORTED_KEYS_TABLE, names);
	}

	@Test
	public void testFunctionColumnsDef() throws SQLException
	{
		final String[] names = { "FUNCTION_CAT", "FUNCTION_SCHEM",
				"FUNCTION_NAME", "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE",
				"TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX",
				"NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
				"IS_NULLABLE", "SPECIFIC_NAME", };
		columnChecking(MetaCatalogBuilder.FUNCTION_COLUMNS_TABLE, names);
	}

	@Test
	public void testFunctionsDef() throws SQLException
	{
		final String[] names = { "FUNCTION_CAT", "FUNCTION_SCHEM",
				"FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME",

		};
		columnChecking(MetaCatalogBuilder.FUNCTIONS_TABLE, names);
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
			Assert.assertTrue("No next when looking for " + name, rs.next());
			Assert.assertEquals(MetaCatalogBuilder.LOCAL_NAME, rs.getString(1)); // TABLE_CAT
			Assert.assertEquals(MetaCatalogBuilder.SCHEMA_LOCAL_NAME,
					rs.getString(2)); // TABLE_SCHEM
			Assert.assertEquals(name, rs.getString(3)); // TABLE_NAME
			Assert.assertEquals("SYSTEM TABLE", rs.getString(4)); // TABLE_TYPE
			Assert.assertEquals("", rs.getString(5)); // REMARKS
		}
		Assert.assertFalse("Extra column after all names were found", rs.next());

	}

	@Test
	public void testImportedKeysDef() throws SQLException
	{
		final String[] names = { "PKTABLE_CAT", "PKTABLE_SCHEM",
				"PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
				"FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ",
				"UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME",
				"DEFERRABILITY", };
		columnChecking(MetaCatalogBuilder.IMPORTED_KEYS_TABLE, names);
	}

	@Test
	public void testIndexInfoDef() throws SQLException
	{
		final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME", "TYPE",
				"ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
				"CARDINALITY", "PAGES", "FILTER_CONDITION", };
		columnChecking(MetaCatalogBuilder.INDEXINFO_TABLE, names);
	}

	// @Test
	// public void testPrimaryKeysDef() throws SQLException
	// {
	// final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
	// "COLUMN_NAME", "KEY_SEQ", "PK_NAME", };
	// columnChecking(MetaCatalogBuilder.PRIMARY_KEY_TABLE, names);
	//
	// final Catalog catalog = new
	// CatalogBuilder().setLocalName("MOCK_CATALOG").build(model);
	// final MockSchema schema = new MockSchema();
	// catalog.addSchema(schema);
	// metadata.addCatalog(catalog);
	//
	// final TableDefImpl tableDef = new SparqlTableDef(
	// "http://example.com/example/", "testTable", "SPARQL String",
	// null);
	//
	// final ColumnDef cd = ColumnDefImpl.Builder.getStringBuilder(
	// "http://example.com/example/", "testColumn").build();
	//
	// tableDef.add(cd);
	// schema.addTableDef(tableDef);
	//
	// ResultSet rs = metadata.getPrimaryKeys(catalog.getLocalName(),
	// schema.getLocalName(), "testTable");
	// Assert.assertFalse(rs.next());
	// final Key key = new Key();
	// key.addSegment(new KeySegment(0, cd));
	// tableDef.setPrimaryKey(key);
	//
	// rs = metadata.getPrimaryKeys(catalog.getLocalName(),
	// schema.getLocalName(), "testTable");
	// Assert.assertTrue(rs.next());
	// Assert.assertEquals(cd.getLabel(), rs.getString("COLUMN_NAME"));
	// Assert.assertEquals("key-" + key.getId(), rs.getString("PK_NAME"));
	// }

	@Test
	public void testOneTable() throws SQLException
	{
		final ResultSet rs = metadata.getTables(null, null,
				MetaCatalogBuilder.COLUMNS_TABLE, null);
		Assert.assertTrue("Missing table " + MetaCatalogBuilder.COLUMNS_TABLE,
				rs.next());
		Assert.assertEquals(MetaCatalogBuilder.LOCAL_NAME, rs.getString(1)); // TABLE_CAT
		Assert.assertEquals(MetaCatalogBuilder.SCHEMA_LOCAL_NAME,
				rs.getString(2)); // TABLE_SCHEM
		Assert.assertEquals(MetaCatalogBuilder.COLUMNS_TABLE, rs.getString(3)); // TABLE_NAME
		Assert.assertEquals("SYSTEM TABLE", rs.getString(4)); // TABLE_TYPE
		Assert.assertEquals("", rs.getString(5)); // REMARKS
		Assert.assertFalse("Extra table in search", rs.next());
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
		columnChecking(MetaCatalogBuilder.PROCEDURE_COLUMNS_TABLE, names);
	}

	@Test
	public void testProceduresDef() throws SQLException
	{
		final String[] names = { "PROCEDURE_CAT", "PROCEDURE_SCHEM",
				"PROCEDURE_NAME", "reserved", "reserved", "reserved",
				"REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME", };
		columnChecking(MetaCatalogBuilder.PROCEDURES_TABLE, names);
	}

	// @Test
	// public void testSuperTablesDef() throws SQLException
	// {
	// final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
	// "SUPERTABLE_NAME",
	//
	// };
	// columnChecking(MetaCatalogBuilder.SUPER_TABLES_TABLE, names);
	//
	// final MockCatalog catalog = new MockCatalog();
	// final MockSchema schema = new MockSchema();
	// catalog.addSchema(schema);
	// metadata.addCatalog(catalog);
	//
	// final TableDefImpl tableDef = new SparqlTableDef(
	// "http://example.com/example/", "testTable", "SPARQL String",
	// null);
	//
	// final ColumnDef cd = ColumnDefImpl.Builder.getStringBuilder(
	// "http://example.com/example/", "testColumn1").build();
	//
	// tableDef.add(cd);
	// schema.addTableDef(tableDef);
	//
	// final TableDefImpl tableDef2 = new SparqlTableDef(
	// "http://example.com/example/", "testTable2", "SPARQL String",
	// tableDef);
	//
	// final ColumnDef cd2 = ColumnDefImpl.Builder.getStringBuilder(
	// "http://example.com/example/", "testColumn2").build();
	//
	// tableDef2.add(cd2);
	// schema.addTableDef(tableDef2);
	//
	// ResultSet rs = metadata.getSuperTables(catalog.getLocalName(),
	// schema.getLocalName(), "testTable");
	// Assert.assertFalse(rs.next());
	//
	// rs = metadata.getSuperTables(catalog.getLocalName(),
	// schema.getLocalName(), "testTable2");
	// Assert.assertTrue(rs.next());
	// Assert.assertEquals("testTable", rs.getString("SUPERTABLE_NAME"));
	//
	// Assert.assertFalse(rs.next());
	// }

	@Test
	public void testSchemasDef() throws SQLException
	{
		final String[] names = { "TABLE_SCHEM", "TABLE_CATALOG", };
		columnChecking(MetaCatalogBuilder.SCHEMAS_TABLE, names);
	}

	@Test
	public void testSuperTypesDef() throws Exception
	{
		final String[] names = { "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
				"SUPERTYPE_CAT", "SUPERTYPE_SCHEM", "SUPERTYPE_NAME",
		};
		columnChecking(MetaCatalogBuilder.SUPER_TYPES_TABLE, names);
	}

	@Test
	public void testTablePrivilegesDef() throws SQLException
	{
		final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE", };
		columnChecking(MetaCatalogBuilder.TABLE_PRIVILEGES_TABLE, names);
	}

	@Test
	public void testTablesDef() throws SQLException
	{
		final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
				"SELF_REFERENCING_COL_NAME", "REF_GENERATION", };
		columnChecking(MetaCatalogBuilder.TABLES_TABLE, names);
	}

	@Test
	public void testTableTypesDef() throws SQLException
	{
		final String[] names = { "TABLE_TYPE", };
		columnChecking(MetaCatalogBuilder.TABLE_TYPES_TABLE, names);
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
		columnChecking(MetaCatalogBuilder.TYPEINFO_TABLE, names);
	}

	@Test
	public void testUDTDef() throws SQLException
	{
		final String[] names = { "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
				"CLASS_NAME", "DATA_TYPE", "REMARKS", "BASE_TYPE", };
		columnChecking(MetaCatalogBuilder.UDT_TABLES, names);
	}

	@Test
	public void testVersionColumnsDef() throws SQLException
	{
		final String[] names = { "SCOPE", "COLUMN_NAME", "DATA_TYPE",
				"TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
				"PSEUDO_COLUMN", };
		columnChecking(MetaCatalogBuilder.VERSION_COLUMNS_TABLE, names);
	}
}
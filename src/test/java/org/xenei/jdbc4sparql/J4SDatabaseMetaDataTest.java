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

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.config.MemDatasetProducer;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.DatasetProducer;
import org.xenei.jdbc4sparql.meta.MetaCatalogBuilder;

public class J4SDatabaseMetaDataTest
{
	private J4SConnection connection;
	private J4SDatabaseMetaData metadata;
	private DatasetProducer dp;
	private static Logger LOG = LoggerFactory
			.getLogger(J4SDatabaseMetaDataTest.class);

	@Test
	@Ignore( "used for debelopment analysis only" )
	public void arbitraryQuery() throws Exception
	{
		final String eol = System.getProperty("line.separator");
		final String queryStr =
		// "SELECT DISTINCT  ?TABLE_CAT ?TABLE_SCHEM ?TABLE_NAME ?COLUMN_NAME ?DATA_TYPE ?TYPE_NAME ?COLUMN_SIZE ?BUFFER_LENGTH ?DECIMAL_DIGITS ?NUM_PREC_RADIX ?NULLABLE ?REMARKS ?COLUMN_DEF ?SQL_DATA_TYPE ?SQL_DATETIME_SUB ?CHAR_OCTET_LENGTH ?ORDINAL_POSITION ?IS_NULLABLE ?SCOPE_CATLOG ?SCOPE_SCHEMA ?SCOPE_TABLE ?SOURCE_DATA_TYPE ?IS_AUTOINCREMENT"
		// +eol+
		"SELECT DISTINCT ?Schema·Columns ?TABLE_CAT ?Schema·Columns·TABLE_CAT"
				+ eol
				+ "		WHERE"
				+ eol
				+ "		  { { { ?Schema·Columns <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://org.xenei.jdbc4sparql/entity/Column> ."
				+ eol
				+ "		        ?Schema·Columns <http://org.xenei.jdbc4sparql/entity/Column#table> ?v_4a282887_afb0_4305_89b0_ab75f3798334 ."
				+ eol
				+ "		        ?v_42545462_74da_4c63_8827_4d8cd17374d3 <http://org.xenei.jdbc4sparql/entity/Schema#tables> ?v_4a282887_afb0_4305_89b0_ab75f3798334 ."
				+ eol
				+ "		        ?v_b99c917a_7a72_4d69_ab7a_1418c22b8838 <http://org.xenei.jdbc4sparql/entity/Catalog#schemas> ?v_42545462_74da_4c63_8827_4d8cd17374d3 ."
				+ eol
				+ "		        ?Schema·Columns <http://org.xenei.jdbc4sparql/entity/Column#columnDef> ?v_1e236430_7f5e_481c_9c03_36e341a1c04f ."
				+ eol
				+ "		        ?v_1e236430_7f5e_481c_9c03_36e341a1c04f <http://org.xenei.jdbc4sparql/entity/ColumnDef#autoIncrement> ?v_573bbcff_0682_48fc_b3e3_ef387e5025f8 ."
				+ eol
				+ "		        ?v_4a282887_afb0_4305_89b0_ab75f3798334 <http://org.xenei.jdbc4sparql/entity/Table#column> ?v_b1d4f410_76da_49cf_8d8d_b95e3792df8d ."
				+ eol
				+ "		        ?v_b99c917a_7a72_4d69_ab7a_1418c22b8838 <http://www.w3.org/2000/01/rdf-schema#label> ?Schema·Columns·TABLE_CAT ."
				+ eol
				+ "		        ?v_42545462_74da_4c63_8827_4d8cd17374d3 <http://www.w3.org/2000/01/rdf-schema#label> ?Schema·Columns·TABLE_SCHEM ."
				+ eol
				+ "		        ?v_4a282887_afb0_4305_89b0_ab75f3798334 <http://www.w3.org/2000/01/rdf-schema#label> ?Schema·Columns·TABLE_NAME ."
				+ eol
				+ "		        ?Schema·Columns <http://www.w3.org/2000/01/rdf-schema#label> ?Schema·Columns·COLUMN_NAME ."
				+ eol
				+ "		        ?v_1e236430_7f5e_481c_9c03_36e341a1c04f <http://org.xenei.jdbc4sparql/entity/ColumnDef#type> ?Schema·Columns·DATA_TYPE ."
				+ eol
				+ "		        ?v_1e236430_7f5e_481c_9c03_36e341a1c04f <http://org.xenei.jdbc4sparql/entity/ColumnDef#scale> ?Schema·Columns·COLUMN_SIZE ."
				+ eol
				+ "		        ?v_1e236430_7f5e_481c_9c03_36e341a1c04f <http://org.xenei.jdbc4sparql/entity/ColumnDef#nullable> ?Schema·Columns·NULLABLE ."
				+ eol
				+ "		        ?Schema·Columns <http://org.xenei.jdbc4sparql/entity/Column#remarks> ?Schema·Columns·REMARKS ."
				+ eol
				+ "		        ?v_4a282887_afb0_4305_89b0_ab75f3798334 <http://org.xenei.jdbc4sparql/entity/Table#column> _:b0 ."
				+ eol
				+ "		        _:b0 <http://jena.hpl.hp.com/ARQ/list#index> _:b1 ."
				+ eol
				+ "		        _:b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> ?v_ef10321d_574c_4efa_84e0_877c6e7dc93d ."
				+ eol
				+ "		        _:b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:b2 ."
				+ eol
				+ "		        _:b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> ?Schema·Columns ."
				+ eol
				+ "		        _:b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"
				+ eol
				+ "		        BIND(( ?v_ef10321d_574c_4efa_84e0_877c6e7dc93d + 1 ) AS ?Schema·Columns·ORDINAL_POSITION)"
				+ eol
				+ "		        ?v_1e236430_7f5e_481c_9c03_36e341a1c04f <http://org.xenei.jdbc4sparql/entity/ColumnDef#autoIncrement> ?v_573bbcff_0682_48fc_b3e3_ef387e5025f8"
				+ eol
				+ "		        BIND(if(?v_573bbcff_0682_48fc_b3e3_ef387e5025f8, \"YES\", \"NO\") AS ?Schema·Columns·IS_AUTOINCREMENT)"
				+ eol
				+ "		      }"
				+ eol
				+ "		      OPTIONAL"
				+ eol
				+ "		        { ?v_1e236430_7f5e_481c_9c03_36e341a1c04f <http://org.xenei.jdbc4sparql/entity/ColumnDef#typeName> ?Schema·Columns·TYPE_NAME }"
				+ eol
				+ "		      OPTIONAL"
				+ eol
				+ "		        { ?Schema·Columns <http://org.xenei.jdbc4sparql/entity/Table#null> ?Schema·Columns·BUFFER_LENGTH }"
				+ eol
				+ "		      OPTIONAL"
				+ eol
				+ "		        { ?Schema·Columns <http://org.xenei.jdbc4sparql/entity/Table#null> ?Schema·Columns·DECIMAL_DIGITS }"
				+ eol
				+ "		      OPTIONAL"
				+ eol
				+ "		        { ?Schema·Columns <http://org.xenei.jdbc4sparql/entity/Table#null> ?Schema·Columns·NUM_PREC_RADIX }"
				+ eol
				+ "		      OPTIONAL"
				+ eol
				+ "		        { ?Schema·Columns <http://org.xenei.jdbc4sparql/entity/Table#null> ?Schema·Columns·COLUMN_DEF }"
				+ eol
				+ "		      OPTIONAL"
				+ eol
				+ "		        { ?Schema·Columns <http://org.xenei.jdbc4sparql/entity/Table#null> ?Schema·Columns·SQL_DATA_TYPE }"
				+ eol
				+ "		      OPTIONAL"
				+ eol
				+ "		        { ?Schema·Columns <http://org.xenei.jdbc4sparql/entity/Table#null> ?Schema·Columns·SQL_DATETIME_SUB }"
				+ eol
				+ "		      OPTIONAL"
				+ eol
				+ "		        { ?Schema·Columns <http://org.xenei.jdbc4sparql/entity/Table#null> ?Schema·Columns·CHAR_OCTET_LENGTH }"
				+ eol
				+ "		      OPTIONAL"
				+ eol
				+ "		        { ?v_1e236430_7f5e_481c_9c03_36e341a1c04f <http://org.xenei.jdbc4sparql/entity/ColumnDef#nullable> ?v_893661d8_23d7_44ea_acbf_511d207c2431"
				+ eol
				+ "		          BIND(if(( ?v_893661d8_23d7_44ea_acbf_511d207c2431 = 1 ), \"YES\", if(( ?v_893661d8_23d7_44ea_acbf_511d207c2431 = 0 ), \"NO\", \"\")) AS ?Schema·Columns·IS_NULLABLE)"
				+ eol
				+ "		        }"
				+ eol
				+ "		      OPTIONAL"
				+ eol
				+ "		        { ?Schema·Columns <http://org.xenei.jdbc4sparql/entity/Table#null> ?Schema·Columns·SCOPE_CATLOG }"
				+ eol
				+ "		      OPTIONAL"
				+ eol
				+ "		        { ?Schema·Columns <http://org.xenei.jdbc4sparql/entity/Table#null> ?Schema·Columns·SCOPE_SCHEMA }"
				+ eol
				+ "		      OPTIONAL"
				+ eol
				+ "		        { ?Schema·Columns <http://org.xenei.jdbc4sparql/entity/Table#null> ?Schema·Columns·SCOPE_TABLE }"
				+ eol
				+ "		      OPTIONAL"
				+ eol
				+ "		        { ?v_1e236430_7f5e_481c_9c03_36e341a1c04f <http://org.xenei.jdbc4sparql/entity/ColumnDef#type> ?Schema·Columns·SOURCE_DATA_TYPE }"
				+ eol +
				// "		      FILTER checkTypeF(?Schema·Columns·COLUMN_DEF)" +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·ORDINAL_POSITION)"
				// +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·DECIMAL_DIGITS)"
				// +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·TABLE_NAME)" +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·COLUMN_NAME)"
				// +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·BUFFER_LENGTH)"
				// +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·SCOPE_TABLE)"
				// +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·COLUMN_SIZE)"
				// +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·IS_NULLABLE)"
				// +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·TABLE_CAT)" +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·SCOPE_CATLOG)"
				// +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·TABLE_SCHEM)"
				// +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·DATA_TYPE)" +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·REMARKS)" +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·CHAR_OCTET_LENGTH)"
				// +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·TYPE_NAME)" +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·SOURCE_DATA_TYPE)"
				// +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·SCOPE_SCHEMA)"
				// +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·NUM_PREC_RADIX)"
				// +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·NULLABLE)" +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·SQL_DATETIME_SUB)"
				// +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·SQL_DATA_TYPE)"
				// +eol+
				// "		      FILTER checkTypeF(?Schema·Columns·IS_AUTOINCREMENT)"
				// +eol+
				"		    }" + eol +
				// "		    FILTER ( ( ( ?Schema·Columns·TABLE_CAT = \"METADATA\" ) && ( ?Schema·Columns·TABLE_SCHEM = \"Schema\" ) ) && ( ?Schema·Columns·TABLE_NAME = \"Catalogs\" ) )"
				// +eol+
				"		  }" + eol +
				// "		ORDER BY ASC(?TABLE_CAT) ASC(?TABLE_SCHEM) ASC(?TABLE_NAME) ASC(?ORDINAL_POSITION)				"
				// +eol+
				"";
		final Query q = QueryFactory.create(queryStr);
		final Model model = dp.getMetaDatasetUnionModel();
		final QueryExecution qexec = QueryExecutionFactory.create(q, model);
		try
		{
			final List<QuerySolution> retval = WrappedIterator.create(
					qexec.execSelect()).toList();
			Assert.assertTrue(retval.size() > 0);
			for (final QuerySolution qs : retval)
			{
				final Iterator<String> iter = qs.varNames();
				while (iter.hasNext())
				{
					final String field = iter.next();
					System.out.print(String.format("%s[%s] ", field,
							qs.get(field)));
				}
				System.out.println();
			}

		}
		catch (final Exception e)
		{
			J4SDatabaseMetaDataTest.LOG.error("Error executing local query: "
					+ e.getMessage(), e);
			throw e;
		}
		finally
		{
			qexec.close();
		}
	}

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
		LoggingConfig.setConsole(Level.DEBUG);
		LoggingConfig.setRootLogger(Level.INFO);
		LoggingConfig.setLogger("com.hp.hpl.jena.", Level.INFO);
		LoggingConfig.setLogger("org.xenei.jdbc4sparql", Level.DEBUG);
		final J4SDriver driver = new J4SDriver();
		connection = Mockito.mock(J4SConnection.class);
		final Map<String, Catalog> catalogs = new HashMap<String, Catalog>();
		dp = new MemDatasetProducer();
		final Catalog cat = MetaCatalogBuilder.getInstance(dp);

		if (J4SDatabaseMetaDataTest.LOG.isDebugEnabled())
		{
			dp.getMetaDatasetUnionModel().write(
					new FileOutputStream("/tmp/cat.ttl"), "TURTLE");
		}
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
				"DESCRIPTION", };
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

		// read entire column suite
		ResultSet rs = metadata.getColumns(null, null, null, null);
		try
		{
			Assert.assertTrue(rs.first());
			String cat = null;
			String schema = null;
			String table = null;
			int ord = 0;
			while (!rs.isAfterLast())
			{
				if (table == null)
				{
					cat = rs.getString("TABLE_CAT");
					schema = rs.getString("TABLE_SCHEM");
					table = rs.getString("TABLE_NAME");
					ord = 1;
				}
				else if (!cat.equals(rs.getString("TABLE_CAT")))
				{
					// catalog changed
					Assert.assertTrue("Catalog out of sequence",
							cat.compareTo(rs.getString("TABLE_CAT")) < 0);
					cat = rs.getString("TABLE_CAT");
					schema = rs.getString("TABLE_SCHEM");
					table = rs.getString("TABLE_NAME");
					ord = 1;
				}
				else if (!schema.equals(rs.getString("TABLE_SCHEM")))
				{
					Assert.assertTrue("Schema out of sequence",
							schema.compareTo(rs.getString("TABLE_SCHEM")) < 0);
					schema = rs.getString("TABLE_SCHEM");
					table = rs.getString("TABLE_NAME");
					ord = 1;
				}
				else if (!table.equals(rs.getString("TABLE_NAME")))
				{
					Assert.assertTrue("Table out of sequence",
							table.compareTo(rs.getString("TABLE_NAME")) < 0);
					table = rs.getString("TABLE_NAME");
					ord = 1;
				}
				else
				{
					++ord;
				}
				Assert.assertEquals(cat, rs.getString("TABLE_CAT"));
				Assert.assertEquals(schema, rs.getString("TABLE_SCHEM"));
				Assert.assertEquals(table, rs.getString("TABLE_NAME"));
				Assert.assertEquals(ord, rs.getInt("ORDINAL_POSITION"));
				Assert.assertEquals(String.format(
						"Incorrect remarks for %s.%s.%s.%s", cat, schema,
						table, rs.getString("COLUMN_NAME")),
						MetaCatalogBuilder.REMARK, rs.getString("REMARKS"));
				rs.next();
			}
		}
		finally
		{
			rs.close();
		}

		// test the column names
		rs = metadata.getColumns(MetaCatalogBuilder.LOCAL_NAME,
				MetaCatalogBuilder.SCHEMA_LOCAL_NAME, "Columns", null);
		try
		{
			Assert.assertTrue(rs.first());
			int i = 0;

			while (!rs.isAfterLast())
			{
				Assert.assertEquals(MetaCatalogBuilder.LOCAL_NAME,
						rs.getString("TABLE_CAT"));
				Assert.assertEquals(MetaCatalogBuilder.SCHEMA_LOCAL_NAME,
						rs.getString("TABLE_SCHEM"));
				Assert.assertEquals("Columns", rs.getString("TABLE_NAME"));
				Assert.assertEquals(names[i++], rs.getString("COLUMN_NAME"));
				Assert.assertEquals(i, rs.getInt("ORDINAL_POSITION"));
				rs.next();
			}
		}
		finally
		{
			rs.close();
		}

		// test all columns with the name TABLE_CAT
		rs = metadata.getColumns(MetaCatalogBuilder.LOCAL_NAME,
				MetaCatalogBuilder.SCHEMA_LOCAL_NAME, null, "TABLE\\_CAT");
		try
		{
			Assert.assertTrue(rs.first());
			int ord = 0;
			while (!rs.isAfterLast())
			{
				Assert.assertEquals(MetaCatalogBuilder.LOCAL_NAME,
						rs.getString("TABLE_CAT"));
				Assert.assertEquals(MetaCatalogBuilder.SCHEMA_LOCAL_NAME,
						rs.getString("TABLE_SCHEM"));
				Assert.assertEquals("TABLE_CAT", rs.getString("COLUMN_NAME"));
				Assert.assertTrue("Ordinal out of sequence",
						ord <= rs.getInt("ORDINAL_POSITION"));
				ord = rs.getInt("ORDINAL_POSITION");
				rs.next();
			}
		}
		finally
		{
			rs.close();
		}

		// test all columns with the name TABLE_CAT (TABLE\\_C_T)
		rs = metadata.getColumns(MetaCatalogBuilder.LOCAL_NAME,
				MetaCatalogBuilder.SCHEMA_LOCAL_NAME, null, "TABLE\\_C_T");
		try
		{
			Assert.assertTrue(rs.first());
			int ord = 0;
			while (!rs.isAfterLast())
			{
				Assert.assertEquals(MetaCatalogBuilder.LOCAL_NAME,
						rs.getString("TABLE_CAT"));
				Assert.assertEquals(MetaCatalogBuilder.SCHEMA_LOCAL_NAME,
						rs.getString("TABLE_SCHEM"));
				Assert.assertEquals("TABLE_CAT", rs.getString("COLUMN_NAME"));
				Assert.assertTrue("Ordinal out of sequence",
						ord <= rs.getInt("ORDINAL_POSITION"));
				ord = rs.getInt("ORDINAL_POSITION");
				rs.next();
			}
		}
		finally
		{
			rs.close();
		}

		// test all columns with the name TABLE_CAT (TA%CAT)
		rs = metadata.getColumns(MetaCatalogBuilder.LOCAL_NAME,
				MetaCatalogBuilder.SCHEMA_LOCAL_NAME, null, "TAB%CAT");
		try
		{
			Assert.assertTrue(rs.first());
			int ord = 0;
			while (!rs.isAfterLast())
			{
				Assert.assertEquals(MetaCatalogBuilder.LOCAL_NAME,
						rs.getString("TABLE_CAT"));
				Assert.assertEquals(MetaCatalogBuilder.SCHEMA_LOCAL_NAME,
						rs.getString("TABLE_SCHEM"));
				Assert.assertEquals("TABLE_CAT", rs.getString("COLUMN_NAME"));
				Assert.assertTrue("Ordinal out of sequence",
						ord <= rs.getInt("ORDINAL_POSITION"));
				ord = rs.getInt("ORDINAL_POSITION");
				rs.next();
			}
		}
		finally
		{
			rs.close();
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
				"SUPERTYPE_CAT", "SUPERTYPE_SCHEM", "SUPERTYPE_NAME", };
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
		final String[] colNmes = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
				"SELF_REFERENCING_COL_NAME", "REF_GENERATION", };

		final String[] tblNames = { "Attributes", "BestRow", "Catalogs",
				"ClientInfo", "ColumnPriviliges", "Columns", "ExportedKeys",
				"FunctionColumns", "Functions", "ImportedKeys", "IndexInfo",
				"PrimaryKeys", "ProcedureColumns", "Procedures", "Schemas",
				"SuperTables", "SuperTypes", "TablePriv", "TableTypes",
				"Tables", "TypeInfo", "UDTs", "Version", "XrefKeys" };

		columnChecking(MetaCatalogBuilder.TABLES_TABLE, colNmes);

		// read entire column suite
		ResultSet rs = metadata.getTables(null, null, null, null);
		try
		{
			Assert.assertTrue(rs.first());
			String cat = null;
			String schema = null;
			String table = null;
			String type = null;
			while (!rs.isAfterLast())
			{
				if (type == null)
				{
					cat = rs.getString("TABLE_CAT");
					schema = rs.getString("TABLE_SCHEM");
					table = rs.getString("TABLE_NAME");
					type = rs.getString("TABLE_TYPE");
				}
				else if (!type.equals(rs.getString("TABLE_TYPE")))
				{
					Assert.assertTrue("Table type out of sequence",
							type.compareTo(rs.getString("TABLE_TYPE")) < 0);
					type = rs.getString("TABLE_TYPE");
					cat = rs.getString("TABLE_CAT");
					schema = rs.getString("TABLE_SCHEM");
					table = rs.getString("TABLE_NAME");
				}
				else if (!cat.equals(rs.getString("TABLE_CAT")))
				{
					// catalog changed
					Assert.assertTrue("Catalog out of sequence",
							cat.compareTo(rs.getString("TABLE_CAT")) < 0);
					cat = rs.getString("TABLE_CAT");
					schema = rs.getString("TABLE_SCHEM");
					table = rs.getString("TABLE_NAME");
				}
				else if (!schema.equals(rs.getString("TABLE_SCHEM")))
				{
					Assert.assertTrue("Schema out of sequence",
							schema.compareTo(rs.getString("TABLE_SCHEMA")) < 0);
					schema = rs.getString("TABLE_SCHEM");
					table = rs.getString("TABLE_NAME");
				}
				else if (!table.equals(rs.getString("TABLE_NAME")))
				{
					Assert.assertTrue("Table out of sequence",
							table.compareTo(rs.getString("TABLE_NAME")) < 0);
					table = rs.getString("TABLE_NAME");
				}

				Assert.assertEquals(type, rs.getString("TABLE_TYPE"));
				Assert.assertEquals(cat, rs.getString("TABLE_CAT"));
				Assert.assertEquals(schema, rs.getString("TABLE_SCHEM"));
				Assert.assertEquals(table, rs.getString("TABLE_NAME"));
				rs.next();
			}
		}
		finally
		{
			rs.close();
		}

		// test metadata tables
		rs = metadata.getTables(MetaCatalogBuilder.LOCAL_NAME,
				MetaCatalogBuilder.SCHEMA_LOCAL_NAME, null, null);
		try
		{
			for (final String name : tblNames)
			{
				Assert.assertTrue("No next when looking for " + name, rs.next());
				Assert.assertEquals(MetaCatalogBuilder.TABLE_TYPE,
						rs.getString("TABLE_TYPE"));
				Assert.assertEquals(MetaCatalogBuilder.LOCAL_NAME,
						rs.getString("TABLE_CAT"));
				Assert.assertEquals(MetaCatalogBuilder.SCHEMA_LOCAL_NAME,
						rs.getString("TABLE_SCHEM"));
				Assert.assertEquals(MetaCatalogBuilder.REMARK,
						rs.getString("REMARKS"));
				Assert.assertEquals(name, rs.getString("TABLE_NAME"));
			}
			Assert.assertFalse("Extra column after all names were found",
					rs.next());
		}
		finally
		{
			rs.close();
		}

		// test the column names
		rs = metadata.getTables(MetaCatalogBuilder.LOCAL_NAME,
				MetaCatalogBuilder.SCHEMA_LOCAL_NAME, "Columns",
				new String[] { MetaCatalogBuilder.TABLE_TYPE });
		try
		{
			Assert.assertTrue(rs.first());
			while (!rs.isAfterLast())
			{
				Assert.assertEquals(MetaCatalogBuilder.TABLE_TYPE,
						rs.getString("TABLE_TYPE"));
				Assert.assertEquals(MetaCatalogBuilder.LOCAL_NAME,
						rs.getString("TABLE_CAT"));
				Assert.assertEquals(MetaCatalogBuilder.SCHEMA_LOCAL_NAME,
						rs.getString("TABLE_SCHEM"));
				Assert.assertEquals("Columns", rs.getString("TABLE_NAME"));
				rs.next();
			}
		}
		finally
		{
			rs.close();
		}

		// test multi table type query
		rs = metadata.getTables(null, null, null, new String[] {
				MetaCatalogBuilder.TABLE_TYPE, "TABLE" });
		try
		{
			// just read the first entry to ensure that the query works. There
			// are no table types of TABLE
			Assert.assertTrue(rs.first());
		}
		finally
		{
			rs.close();
		}
	}

	@Test
	public void testTableTypesDef() throws SQLException
	{
		final String[] names = { "TABLE_TYPE", };
		columnChecking(MetaCatalogBuilder.TABLE_TYPES_TABLE, names);
		final ResultSet rs = metadata.getTableTypes();
		try
		{
			Assert.assertNotNull(rs);
			Assert.assertTrue(rs.next());
		}
		finally
		{
			rs.close();
		}
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
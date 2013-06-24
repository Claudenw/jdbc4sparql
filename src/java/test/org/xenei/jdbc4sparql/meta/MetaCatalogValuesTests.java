package org.xenei.jdbc4sparql.meta;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.sparql.SparqlResultSet;

public class MetaCatalogValuesTests
{
	private Model model;
	private RdfCatalog catalog;
	private final String queryString = "PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
			+ "SELECT ?tbl ?colName WHERE { ?tbl a <http://org.xenei.jdbc4sparql/entity/Table> ;"
			+ "<http://www.w3.org/2000/01/rdf-schema#label> '%s' ;"
			+ "<http://org.xenei.jdbc4sparql/entity/Table#column> ?list ."
			+ "?list rdf:rest*/rdf:first ?column ."
			+ "?column <http://www.w3.org/2000/01/rdf-schema#label> ?colName ; "
			+ " }";
	
	@Before
	public void setup()
	{
		model = ModelFactory.createDefaultModel();
		catalog = (RdfCatalog) MetaCatalogBuilder.getInstance(model);
	}

	@After
	public void tearDown() throws Exception
	{
		model.close();
	}

	@Test
	public void testAttributesTable()
	{
		final String[] names = { "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
				"ATTR_NAME", "DATA_TYPE", "ATTR_TYPE_NAME", "ATTR_SIZE",
				"DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS",
				"ATTR_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
				"CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
				"SCOPE_CATALOG", "SCOPE_SCHEMA", "SOURCE_DATA_TYPE" };
		verifyNames(MetaCatalogBuilder.ATTRIBUTES_TABLE, names);
		
	}

	@Test
	public void testBestRowTable()
	{
		final String[] names = { "SCOPE", "COLUMN_NAME", "DATA_TYPE",
				"TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
				"PSEUDO_COLUMN" };
		verifyNames(MetaCatalogBuilder.BEST_ROW_TABLE, names);
	}

	@Test
	public void testCatalogsTable()
	{
		final String[] names = { "TABLE_CAT" };
		verifyNames(MetaCatalogBuilder.CATALOGS_TABLE, names);
	}

	@Test
	public void testClientInfoTable()
	{
		final String[] names = { "NAME", "MAX_LEN", "DEFAULT_VALUE",
				"DESCRIPTION" };
		verifyNames(MetaCatalogBuilder.CLIENT_INFO_TABLE, names);
	}

	@Test
	public void testColumnPriviligesTable()
	{
		final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"COLUMN_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE",
				"IS_GRANTABLE" };
		verifyNames(MetaCatalogBuilder.COLUMN_PRIVILIGES_TABLE, names);
	}

	@Test
	public void testColumnsTable() throws SQLException
	{
		final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE",
				"BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
				"NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE",
				"SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
				"IS_NULLABLE", "SCOPE_CATLOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
				"SOURCE_DATA_TYPE", "IS_AUTOINCREMENT" };
		verifyNames(MetaCatalogBuilder.COLUMNS_TABLE, names);
		
		RdfSchema schema = catalog.getSchema( MetaCatalogBuilder.SCHEMA_LOCAL_NAME );
		RdfTable table = schema.getTable(MetaCatalogBuilder.COLUMNS_TABLE);
		SparqlResultSet rs = table.getResultSet();
		Assert.assertTrue( rs.first() );
		while ( ! rs.isAfterLast() )
		{
			System.out.println( String.format( "%s : %s : %s : %s : %d : %s", rs.getString( "TABLE_CAT"), 
					rs.getString( "TABLE_SCHEM"), rs.getString("TABLE_NAME"), rs.getString("COLUMN_NAME"), rs.getInt("ORDINAL_POSITION"), rs.getString("IS_NULLABLE")));
			rs.next();
		}
	}

	@Test
	public void testExportedKeysTable()
	{
		final String[] names = { "PKTABLE_CAT", "PKTABLE_SCHEM",
				"PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
				"FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ",
				"UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME",
				"DEFERRABILITY" };
		verifyNames(MetaCatalogBuilder.EXPORTED_KEYS_TABLE, names);
	}

	@Test
	public void testFunctionColumnsTable()
	{
		final String[] names = { "FUNCTION_CAT", "FUNCTION_SCHEM",
				"FUNCTION_NAME", "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE",
				"TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX",
				"NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
				"IS_NULLABLE", "SPECIFIC_NAME" };
		verifyNames(MetaCatalogBuilder.FUNCTION_COLUMNS_TABLE, names);
	}

	@Test
	public void testFunctionsTable()
	{
		final String[] names = { "FUNCTION_CAT", "FUNCTION_SCHEM",
				"FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME" };
		verifyNames(MetaCatalogBuilder.FUNCTIONS_TABLE, names);
	}

	@Test
	public void testImportedKeysTable()
	{
		final String[] names = { "PKTABLE_CAT", "PKTABLE_SCHEM",
				"PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
				"FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ",
				"UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME",
				"DEFERRABILITY" };
		verifyNames(MetaCatalogBuilder.IMPORTED_KEYS_TABLE, names);
	}

	@Test
	public void testIndexInfoTable()
	{
		final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME", "TYPE",
				"ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
				"CARDINALITY", "PAGES", "FILTER_CONDITION" };
		verifyNames(MetaCatalogBuilder.INDEXINFO_TABLE, names);
	}

	@Test
	public void testPrimaryKeyTable()
	{
		final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"COLUMN_NAME", "KEY_SEQ", "PK_NAME" };
		verifyNames(MetaCatalogBuilder.PRIMARY_KEY_TABLE, names);
	}

	@Test
	public void testProcedureColumnsTable()
	{
		final String[] names = { "PROCEDURE_CAT", "PROCEDURE_SCHEM",
				"PROCEDURE_NAME", "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE",
				"TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX",
				"NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE",
				"SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
				"IS_NULLABLE", "SPECIFIC_NAME" };
		verifyNames(MetaCatalogBuilder.PROCEDURE_COLUMNS_TABLE, names);
	}

	@Test
	public void testProceduresTable()
	{

		final String[] names = { "PROCEDURE_CAT", "PROCEDURE_SCHEM",
				"PROCEDURE_NAME", "FUTURE1", "FUTURE2", "FUTURE3", "REMARKS",
				"PROCEDURE_TYPE", "SPECIFIC_NAME" };
		verifyNames(MetaCatalogBuilder.PROCEDURES_TABLE, names);
	}

	@Test
	public void testSchemasTable()
	{
		final String[] names = { "TABLE_SCHEM", "TABLE_CATALOG" };
		verifyNames(MetaCatalogBuilder.SCHEMAS_TABLE, names);
	}

	@Test
	public void testSuperTablesTable()
	{
		final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"SUPERTABLE_NAME" };
		verifyNames(MetaCatalogBuilder.SUPER_TABLES_TABLE, names);
	}

	@Test
	public void testSuperTypesTable()
	{
		final String[] names = { "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
				"SUPERTYPE_CAT", "SUPERTYPE_SCHEM", "SUPERTYPE_NAME" };
		verifyNames(MetaCatalogBuilder.SUPER_TYPES_TABLE, names);
	}

	@Test
	public void testTablePrivilegesTable()
	{
		final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE" };
		verifyNames(MetaCatalogBuilder.TABLE_PRIVILEGES_TABLE, names);
	}

	@Test
	public void testTablesTable() throws SQLException
	{
		final String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
				"SELF_REFERENCING_COL_NAME", "REF_GENERATION" };
		verifyNames(MetaCatalogBuilder.TABLES_TABLE, names);
		
		RdfSchema schema = catalog.getSchema( MetaCatalogBuilder.SCHEMA_LOCAL_NAME );
		RdfTable table = schema.getTable(MetaCatalogBuilder.TABLES_TABLE);
		SparqlResultSet rs = table.getResultSet();
		Assert.assertTrue( rs.first() );
		while ( ! rs.isAfterLast() )
		{
			System.out.println( String.format( "%s : %s : %s", rs.getString( "TABLE_CAT"), rs.getString( "TABLE_SCHEM"), rs.getString("TABLE_NAME")));
			rs.next();
		}
		
		
	}

	@Test
	public void testTableTypesTable()
	{
		final String[] names = { "TABLE_TYPE" };
		verifyNames(MetaCatalogBuilder.TABLE_TYPES_TABLE, names);
	}

	@Test
	public void testTypeInfoTableTable()
	{
		final String[] names = { "TYPE_NAME", "DATA_TYPE", "PRECISION",
				"LITERAL_PREFIX", "LITERAL_SUFFIX", "CREATE_PARAMS",
				"NULLABLE", "CASE_SENSITIVE", "SEARCHABLE",
				"UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT",
				"LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE",
				"SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX" };
		verifyNames(MetaCatalogBuilder.TYPEINFO_TABLE, names);
	}

	@Test
	public void testUDTTable()
	{
		final String[] names = { "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
				"CLASS_NAME", "DATA_TYPE", "REMARKS", "BASE_TYPE" };
		verifyNames(MetaCatalogBuilder.UDT_TABLES, names);
	}

	@Test
	public void testVersionColumnsTable()
	{
		final String[] names = { "SCOPE", "COLUMN_NAME", "DATA_TYPE",
				"TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
				"PSEUDO_COLUMN" };
		verifyNames(MetaCatalogBuilder.VERSION_COLUMNS_TABLE, names);
	}

	@Test
	public void testXrefTable()
	{
		final String[] names = { "PKTABLE_CAT", "PKTABLE_SCHEM",
				"PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
				"FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ",
				"UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME",
				"DEFERRABILITY" };
		verifyNames(MetaCatalogBuilder.XREF_TABLE, names);
	}

	private void verifyNames( final String tblName, final String[] colNames )
	{
		final List<String> names = Arrays.asList(colNames);
		int count = 0;
		final Query query = QueryFactory.create(String.format(queryString,
				tblName));
		final QueryExecution qexec = QueryExecutionFactory.create(query, model);
		try
		{
			final ResultSet results = qexec.execSelect();

			for (; results.hasNext();)
			{
				count++;
				final QuerySolution soln = results.nextSolution();
				final Literal l = soln.getLiteral("colName");
				Assert.assertTrue(l.getString() + " is missing",
						names.contains(l.getString()));
				System.out.println(l.getString());
			}
			Assert.assertEquals(names.size(), count);

		}
		finally
		{
			qexec.close();
		}
	}

}

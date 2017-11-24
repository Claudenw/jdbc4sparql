package org.xenei.jdbc4sparql.meta;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.LoggingConfig;
import org.xenei.jdbc4sparql.config.MemDatasetProducer;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.DatasetProducer;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.sparql.SparqlResultSet;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlParserImpl;
import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.vocabulary.RDF;

public class MetaCatalogValuesTests {
	private Map<String, Catalog> catalogs;
	private SparqlParser parser;
	private DatasetProducer dpProducer;
	private RdfCatalog catalog;
	private SelectBuilder innerSelectBuilder = new SelectBuilder().addPrefix("rdf","http://www.w3.org/1999/02/22-rdf-syntax-ns#")
			.addWhere( "?tbl", RDF.type, "<http://org.xenei.jdbc4sparql/entity/Table>")
			.addWhere( "?tbl", "<http://www.w3.org/2000/01/rdf-schema#label>", "?lbl")
			.addWhere( "?tbl", "<http://org.xenei.jdbc4sparql/entity/Table#column>", "?list")
			.addWhere( "?list", "rdf:rest*/rdf:first", "?column")
			.addWhere( "?column", "<http://www.w3.org/2000/01/rdf-schema#label>", "?colName" );
	private SelectBuilder selectBuilder = new SelectBuilder().addVar( "?tbl").addVar( "?colName")
			.addGraph( "<http://org.xenei.jdbc4sparql/entity/Catalog/instance/NMETADATA>", innerSelectBuilder);


	@Before
	public void setup() throws FileNotFoundException, IOException {
		LoggingConfig.setConsole(Level.DEBUG);
		LoggingConfig.setRootLogger(Level.INFO);
		// LoggingConfig.setLogger("org.xenei.jdbc4sparql.sparql", Level.DEBUG);
		LoggingConfig.setLogger("org.apache.jena.", Level.INFO);
		LoggingConfig.setLogger("org.xenei.jdbc4sparql", Level.INFO);

		catalogs = new HashMap<String, Catalog>();
		dpProducer = new MemDatasetProducer();
		catalog = (RdfCatalog) MetaCatalogBuilder.getInstance(dpProducer);
		catalogs.put(catalog.getName().getShortName(), catalog);
		parser = new SparqlParserImpl();
	}

	@After
	public void tearDown() throws Exception {
		dpProducer.close();
	}

	@Test
	public void testAttributesTable() {
		final String[] names = {
				"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "ATTR_NAME",
				"DATA_TYPE", "ATTR_TYPE_NAME", "ATTR_SIZE", "DECIMAL_DIGITS",
				"NUM_PREC_RADIX", "NULLABLE", "REMARKS", "ATTR_DEF",
				"SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
				"ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG",
				"SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE"
		};
		verifyNames(MetaCatalogBuilder.ATTRIBUTES_TABLE, names);

	}

	@Test
	public void testBestRowTable() {
		final String[] names = {
				"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
				"COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
				"PSEUDO_COLUMN"
		};
		verifyNames(MetaCatalogBuilder.BEST_ROW_TABLE, names);
	}

	@Test
	public void testCatalogsTable() {
		final String[] names = {
				"TABLE_CAT"
		};
		verifyNames(MetaCatalogBuilder.CATALOGS_TABLE, names);
	}

	@Test
	public void testClientInfoTable() {
		final String[] names = {
				"NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION"
		};
		verifyNames(MetaCatalogBuilder.CLIENT_INFO_TABLE, names);
	}

	@Test
	public void testColumnPriviligesTable() {
		final String[] names = {
				"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
				"GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"
		};
		verifyNames(MetaCatalogBuilder.COLUMN_PRIVILIGES_TABLE, names);
	}

	@Test
	public void testColumnsTable() throws SQLException {
		final String[] names = {
				"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
				"DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
				"DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS",
				"COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
				"CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
				"SCOPE_CATLOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
				"SOURCE_DATA_TYPE", "IS_AUTOINCREMENT"
		};
		verifyNames(MetaCatalogBuilder.COLUMNS_TABLE, names);

		// final Schema schema =
		// catalog.getSchema(MetaCatalogBuilder.SCHEMA_NAME);
		// final Table table =
		// schema.getTable(MetaCatalogBuilder.COLUMNS_TABLE);
		// final SparqlResultSet rs = ((RdfTable) table).getResultSet(catalogs,
		// parser);
		// Assert.assertTrue(rs.first());
		// while (!rs.isAfterLast()) {
		// for (int i=0;i<names.length;i++)
		// {
		// System.out.print( String.format("%s=%s ", names[i],
		// rs.getString(i+1)));
		// }
		// System.out.println();
		// rs.next();
		// }
		// rs.close();
	}

	@Test
	public void testExportedKeysTable() {
		final String[] names = {
				"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME",
				"PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM",
				"FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE",
				"DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"
		};
		verifyNames(MetaCatalogBuilder.EXPORTED_KEYS_TABLE, names);
	}

	@Test
	public void testFunctionColumnsTable() {
		final String[] names = {
				"FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME",
				"COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME",
				"PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS",
				"CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
				"SPECIFIC_NAME"
		};
		verifyNames(MetaCatalogBuilder.FUNCTION_COLUMNS_TABLE, names);
	}

	@Test
	public void testFunctionsTable() {
		final String[] names = {
				"FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS",
				"FUNCTION_TYPE", "SPECIFIC_NAME"
		};
		verifyNames(MetaCatalogBuilder.FUNCTIONS_TABLE, names);
	}

	@Test
	public void testImportedKeysTable() {
		final String[] names = {
				"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME",
				"PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM",
				"FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE",
				"DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"
		};
		verifyNames(MetaCatalogBuilder.IMPORTED_KEYS_TABLE, names);
	}

	@Test
	public void testIndexInfoTable() {
		final String[] names = {
				"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE",
				"INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION",
				"COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES",
				"FILTER_CONDITION"
		};
		verifyNames(MetaCatalogBuilder.INDEXINFO_TABLE, names);
	}

	@Test
	public void testPrimaryKeyTable() {
		final String[] names = {
				"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
				"KEY_SEQ", "PK_NAME"
		};
		verifyNames(MetaCatalogBuilder.PRIMARY_KEY_TABLE, names);
	}

	@Test
	public void testProcedureColumnsTable() {
		final String[] names = {
				"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
				"COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME",
				"PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS",
				"COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
				"CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
				"SPECIFIC_NAME"
		};
		verifyNames(MetaCatalogBuilder.PROCEDURE_COLUMNS_TABLE, names);
	}

	@Test
	public void testProceduresTable() {

		final String[] names = {
				"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
				"FUTURE1", "FUTURE2", "FUTURE3", "REMARKS", "PROCEDURE_TYPE",
				"SPECIFIC_NAME"
		};
		verifyNames(MetaCatalogBuilder.PROCEDURES_TABLE, names);
	}

	@Test
	public void testSchemasTable() {
		final String[] names = {
				"TABLE_SCHEM", "TABLE_CATALOG"
		};
		verifyNames(MetaCatalogBuilder.SCHEMAS_TABLE, names);
	}

	@Test
	public void testSuperTablesTable() {
		final String[] names = {
				"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "SUPERTABLE_NAME"
		};
		verifyNames(MetaCatalogBuilder.SUPER_TABLES_TABLE, names);
	}

	@Test
	public void testSuperTypesTable() {
		final String[] names = {
				"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SUPERTYPE_CAT",
				"SUPERTYPE_SCHEM", "SUPERTYPE_NAME"
		};
		verifyNames(MetaCatalogBuilder.SUPER_TYPES_TABLE, names);
	}

	@Test
	public void testTablePrivilegesTable() {
		final String[] names = {
				"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "GRANTOR", "GRANTEE",
				"PRIVILEGE", "IS_GRANTABLE"
		};
		verifyNames(MetaCatalogBuilder.TABLE_PRIVILEGES_TABLE, names);
	}

	@Test
	public void testTablesTable() throws SQLException {
		final String[] names = {
				"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE",
				"REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
				"SELF_REFERENCING_COL_NAME", "REF_GENERATION"
		};
		verifyNames(MetaCatalogBuilder.TABLES_TABLE, names);

		final Schema schema = catalog.getSchema(MetaCatalogBuilder.SCHEMA_NAME);
		final Table table = schema.getTable(MetaCatalogBuilder.TABLES_TABLE);
		final SparqlResultSet rs = ((RdfTable) table).getResultSet(catalogs,
				parser);
		Assert.assertTrue(rs.first());
		while (!rs.isAfterLast()) {
			rs.next();
		}
		rs.close();
	}

	@Test
	public void testTableTypesTable() {
		final String[] names = {
				"TABLE_TYPE"
		};
		verifyNames(MetaCatalogBuilder.TABLE_TYPES_TABLE, names);
	}

	@Test
	public void testTypeInfoTableTable() {
		final String[] names = {
				"TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX",
				"LITERAL_SUFFIX", "CREATE_PARAMS", "NULLABLE",
				"CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE",
				"FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME",
				"MINIMUM_SCALE", "MAXIMUM_SCALE", "SQL_DATA_TYPE",
				"SQL_DATETIME_SUB", "NUM_PREC_RADIX"
		};
		verifyNames(MetaCatalogBuilder.TYPEINFO_TABLE, names);
	}

	@Test
	public void testUDTTable() {
		final String[] names = {
				"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME",
				"DATA_TYPE", "REMARKS", "BASE_TYPE"
		};
		verifyNames(MetaCatalogBuilder.UDT_TABLES, names);
	}

	@Test
	public void testVersionColumnsTable() {
		final String[] names = {
				"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
				"COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
				"PSEUDO_COLUMN"
		};
		verifyNames(MetaCatalogBuilder.VERSION_COLUMNS_TABLE, names);
	}

	@Test
	public void testXrefTable() {
		final String[] names = {
				"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME",
				"PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM",
				"FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE",
				"DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"
		};
		verifyNames(MetaCatalogBuilder.XREF_TABLE, names);
	}

	private void verifyNames(final String tblName, final String[] colNames) {
		final List<String> names = Arrays.asList(colNames);
		int count = 0;
		selectBuilder.setVar( "?lbl", tblName);
		final Query query = selectBuilder.build();	
		
		final QueryExecution qexec = dpProducer.getMetaDataEntityManager().getConnection().query(query);
				
		try {
			final ResultSet results = qexec.execSelect();

			for (; results.hasNext();) {
				count++;
				final QuerySolution soln = results.nextSolution();
				final Literal l = soln.getLiteral("colName");
				Assert.assertTrue(l.getString() + " is missing",
						names.contains(l.getString()));
			}
			Assert.assertEquals(names.size(), count);

		} finally {
			qexec.close();
		}
	}

	@Test
	public void arbitraryTest() {
		// String queryString =
		// "SELECT DISTINCT  ?TABLE_CAT ?TABLE_SCHEM ?TABLE_NAME ?COLUMN_NAME ?DATA_TYPE ?TYPE_NAME ?COLUMN_SIZE ?BUFFER_LENGTH ?DECIMAL_DIGITS ?NUM_PREC_RADIX ?NULLABLE ?REMARKS ?COLUMN_DEF ?SQL_DATA_TYPE ?SQL_DATETIME_SUB ?CHAR_OCTET_LENGTH ?ORDINAL_POSITION ?IS_NULLABLE ?SCOPE_CATLOG ?SCOPE_SCHEMA ?SCOPE_TABLE ?SOURCE_DATA_TYPE ?IS_AUTOINCREMENT"
		// +
		// "				WHERE" +
		// "				  { { { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://org.xenei.jdbc4sparql/entity/Column> ."
		// +
		// "				        ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Column#table> ?v_f13657e4_6674_4af2_8d4f_6de18d655197 ."
		// +
		// "				        ?v_208b578e_16ab_4d95_afd5_27dd8ec56a12 <http://org.xenei.jdbc4sparql/entity/Schema#tables> ?v_f13657e4_6674_4af2_8d4f_6de18d655197 ."
		// +
		// "				        ?v_0606127e_438b_49aa_bee9_4d882d7f998f <http://org.xenei.jdbc4sparql/entity/Catalog#schemas> ?v_208b578e_16ab_4d95_afd5_27dd8ec56a12 ."
		// +
		// "				        ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Column#columnDef> ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 ."
		// +
		// "				        ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#autoIncrement> ?v_9f33903c_1dfb_4e0c_b212_5fdcd232c99d ."
		// +
		// "				        ?v_f13657e4_6674_4af2_8d4f_6de18d655197 <http://org.xenei.jdbc4sparql/entity/Table#column> ?v_8f09a023_7155_43bb_810e_66727126a28c . "
		// +
		// "				        ?v_0606127e_438b_49aa_bee9_4d882d7f998f <http://www.w3.org/2000/01/rdf-schema#label> ?v_60396d95_05cb_3f65_ac33_76af9371d45a . "
		// +
		// "				        ?v_208b578e_16ab_4d95_afd5_27dd8ec56a12 <http://www.w3.org/2000/01/rdf-schema#label> ?v_131f86b0_fe97_3301_a6fd_4c518f5d0fa8 . "
		// +
		// "				        ?v_f13657e4_6674_4af2_8d4f_6de18d655197 <http://www.w3.org/2000/01/rdf-schema#label> ?v_3d2e3ea9_e4d9_3606_8d7b_e9c7aed93fe4 . "
		// +
		// "				        ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://www.w3.org/2000/01/rdf-schema#label> ?v_055e5fdf_9ffd_3734_b005_bfe91175ad41 . "
		// +
		// "				        ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#type> ?v_866904db_b993_3be1_8caf_9d38d89b2d58"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#typeName> ?v_4686423b_c7d8_3e2f_8419_2e8d666900bc}"
		// +
		// "				        ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#scale> ?v_90528c50_8dca_3922_b967_1b5f4bfbac16"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_caa2b141_2274_36c6_9358_ac218278a66e}"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_65190fd1_682f_34ca_b75c_f08a7d4c5e7d}"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_7d58e0e2_0d5e_38f1_b8e8_4d39ba9d492a}"
		// +
		// "				        ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#nullable> ?v_959bda57_4240_3bae_b500_a1cb3145adb3"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Column#remarks> ?v_8631fa67_8894_3282_8dc5_d6e426b90183}"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_cd150c95_4c31_3011_bd29_c0ea109ad4de}"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_1e2f7717_47d1_35cd_8135_a9b3b25b47c6}"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_a8c445fa_46a4_3a08_a3e3_0d165ddc0e6c}"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_a34bd851_fe93_3448_904e_058bcaa2a488}"
		// +
		// "				        { ?v_f13657e4_6674_4af2_8d4f_6de18d655197 <http://org.xenei.jdbc4sparql/entity/Table#column> _:b0 ."
		// +
		// "				          _:b0 <http://jena.hpl.hp.com/ARQ/list#index> _:b1 ." +
		// "				          _:b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> ?v_b444882e_dad7_4aa8_9f4d_fb7ba90bfc17 ."
		// +
		// "				          _:b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:b2 ."
		// +
		// "				          _:b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 ."
		// +
		// "				          _:b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"
		// +
		// "				          BIND(( ?v_b444882e_dad7_4aa8_9f4d_fb7ba90bfc17 + 1 ) AS ?v_fdcdadc6_1aca_3423_a36b_057fb163d115)"
		// +
		// "				        }" +
		// "				        OPTIONAL" +
		// "				          { ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#nullable> ?v_38f6b9fd_7328_4b19_8d45_54c6ec7a97e3"
		// +
		// "				            BIND(if(( ?v_38f6b9fd_7328_4b19_8d45_54c6ec7a97e3 = 1 ), \"YES\", if(( ?v_38f6b9fd_7328_4b19_8d45_54c6ec7a97e3 = 0 ), \"NO\", \"\")) AS ?v_754a86ff_b4f2_3218_b894_8855e1bd02a2)"
		// +
		// "				          }" +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_35d0f90a_8271_317c_b412_a6eec8629b8a}"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_c535c245_5a83_3d2b_9500_346d76834403}"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_37c9ca97_f24e_3c6e_b20b_97e86525a5b4}"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#type> ?v_b311cba9_6169_32fc_a57d_af9e98adab52}"
		// +
		// "				        { ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#autoIncrement> ?v_9f33903c_1dfb_4e0c_b212_5fdcd232c99d"
		// +
		// "				          BIND(if(?v_9f33903c_1dfb_4e0c_b212_5fdcd232c99d, \"YES\", \"NO\") AS ?v_9c1c939e_a918_3003_bf84_dc43bee6bb5c)"
		// +
		// "				        }" +
		// "				        FILTER ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( checkTypeF(?v_60396d95_05cb_3f65_ac33_76af9371d45a) && checkTypeF(?v_131f86b0_fe97_3301_a6fd_4c518f5d0fa8) ) && checkTypeF(?v_3d2e3ea9_e4d9_3606_8d7b_e9c7aed93fe4) ) && checkTypeF(?v_055e5fdf_9ffd_3734_b005_bfe91175ad41) ) && checkTypeF(?v_866904db_b993_3be1_8caf_9d38d89b2d58) ) && checkTypeF(?v_4686423b_c7d8_3e2f_8419_2e8d666900bc) ) && checkTypeF(?v_90528c50_8dca_3922_b967_1b5f4bfbac16) ) && checkTypeF(?v_caa2b141_2274_36c6_9358_ac218278a66e) ) && checkTypeF(?v_65190fd1_682f_34ca_b75c_f08a7d4c5e7d) ) && checkTypeF(?v_7d58e0e2_0d5e_38f1_b8e8_4d39ba9d492a) ) && checkTypeF(?v_959bda57_4240_3bae_b500_a1cb3145adb3) ) && checkTypeF(?v_8631fa67_8894_3282_8dc5_d6e426b90183) ) && checkTypeF(?v_cd150c95_4c31_3011_bd29_c0ea109ad4de) ) && checkTypeF(?v_1e2f7717_47d1_35cd_8135_a9b3b25b47c6) ) && checkTypeF(?v_a8c445fa_46a4_3a08_a3e3_0d165ddc0e6c) ) && checkTypeF(?v_a34bd851_fe93_3448_904e_058bcaa2a488) ) && checkTypeF(?v_fdcdadc6_1aca_3423_a36b_057fb163d115) ) && checkTypeF(?v_754a86ff_b4f2_3218_b894_8855e1bd02a2) ) && checkTypeF(?v_35d0f90a_8271_317c_b412_a6eec8629b8a) ) && checkTypeF(?v_c535c245_5a83_3d2b_9500_346d76834403) ) && checkTypeF(?v_37c9ca97_f24e_3c6e_b20b_97e86525a5b4) ) && checkTypeF(?v_b311cba9_6169_32fc_a57d_af9e98adab52) ) && checkTypeF(?v_9c1c939e_a918_3003_bf84_dc43bee6bb5c) )"
		// +
		// "				      }" +
		// "				      BIND(forceTypeF(?v_35d0f90a_8271_317c_b412_a6eec8629b8a) AS ?SCOPE_CATLOG)"
		// +
		// "				      BIND(forceTypeF(?v_a8c445fa_46a4_3a08_a3e3_0d165ddc0e6c) AS ?SQL_DATETIME_SUB)"
		// +
		// "				      BIND(forceTypeF(?v_65190fd1_682f_34ca_b75c_f08a7d4c5e7d) AS ?DECIMAL_DIGITS)"
		// +
		// "				      BIND(forceTypeF(?v_60396d95_05cb_3f65_ac33_76af9371d45a) AS ?TABLE_CAT)"
		// +
		// "				      BIND(forceTypeF(?v_754a86ff_b4f2_3218_b894_8855e1bd02a2) AS ?IS_NULLABLE)"
		// +
		// "				      BIND(forceTypeF(?v_4686423b_c7d8_3e2f_8419_2e8d666900bc) AS ?TYPE_NAME)"
		// +
		// "				      BIND(forceTypeF(?v_c535c245_5a83_3d2b_9500_346d76834403) AS ?SCOPE_SCHEMA)"
		// +
		// "				      BIND(forceTypeF(?v_b311cba9_6169_32fc_a57d_af9e98adab52) AS ?SOURCE_DATA_TYPE)"
		// +
		// "				      BIND(forceTypeF(?v_9c1c939e_a918_3003_bf84_dc43bee6bb5c) AS ?IS_AUTOINCREMENT)"
		// +
		// "				      BIND(forceTypeF(?v_cd150c95_4c31_3011_bd29_c0ea109ad4de) AS ?COLUMN_DEF)"
		// +
		// "				      BIND(forceTypeF(?v_055e5fdf_9ffd_3734_b005_bfe91175ad41) AS ?COLUMN_NAME)"
		// +
		// "				      BIND(forceTypeF(?v_1e2f7717_47d1_35cd_8135_a9b3b25b47c6) AS ?SQL_DATA_TYPE)"
		// +
		// "				      BIND(forceTypeF(?v_7d58e0e2_0d5e_38f1_b8e8_4d39ba9d492a) AS ?NUM_PREC_RADIX)"
		// +
		// "				      BIND(forceTypeF(?v_37c9ca97_f24e_3c6e_b20b_97e86525a5b4) AS ?SCOPE_TABLE)"
		// +
		// "				      BIND(forceTypeF(?v_a34bd851_fe93_3448_904e_058bcaa2a488) AS ?CHAR_OCTET_LENGTH)"
		// +
		// "				      BIND(forceTypeF(?v_131f86b0_fe97_3301_a6fd_4c518f5d0fa8) AS ?TABLE_SCHEM)"
		// +
		// "				      BIND(forceTypeF(?v_fdcdadc6_1aca_3423_a36b_057fb163d115) AS ?ORDINAL_POSITION)"
		// +
		// "				      BIND(forceTypeF(?v_90528c50_8dca_3922_b967_1b5f4bfbac16) AS ?COLUMN_SIZE)"
		// +
		// "				      BIND(forceTypeF(?v_caa2b141_2274_36c6_9358_ac218278a66e) AS ?BUFFER_LENGTH)"
		// +
		// "				      BIND(forceTypeF(?v_959bda57_4240_3bae_b500_a1cb3145adb3) AS ?NULLABLE)"
		// +
		// "				      BIND(forceTypeF(?v_8631fa67_8894_3282_8dc5_d6e426b90183) AS ?REMARKS)"
		// +
		// "				      BIND(forceTypeF(?v_866904db_b993_3be1_8caf_9d38d89b2d58) AS ?DATA_TYPE)"
		// +
		// "				      BIND(forceTypeF(?v_3d2e3ea9_e4d9_3606_8d7b_e9c7aed93fe4) AS ?TABLE_NAME)"
		// +
		// "				    }" +
		// "				  }" +
		// "				ORDER BY ASC(?TABLE_CAT) ASC(?TABLE_SCHEM) ASC(?TABLE_NAME) ASC(?ORDINAL_POSITION)"
		// +
		// "";

		final String queryString = "PREFIX f: <java:org.xenei.jdbc4sparql.sparql.> SELECT DISTINCT  ?TABLE_CAT ?TABLE_SCHEM ?TABLE_NAME ?COLUMN_NAME ?DATA_TYPE ?TYPE_NAME ?COLUMN_SIZE ?BUFFER_LENGTH ?DECIMAL_DIGITS ?NUM_PREC_RADIX ?NULLABLE ?REMARKS ?COLUMN_DEF ?SQL_DATA_TYPE ?SQL_DATETIME_SUB ?CHAR_OCTET_LENGTH ?ORDINAL_POSITION ?IS_NULLABLE ?SCOPE_CATLOG ?SCOPE_SCHEMA ?SCOPE_TABLE ?SOURCE_DATA_TYPE ?IS_AUTOINCREMENT"
				+ "				WHERE"
				+ "				  { { { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://org.xenei.jdbc4sparql/entity/Column> ."
				+ "				        ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Column#table> ?v_f13657e4_6674_4af2_8d4f_6de18d655197 ."
				+ "				        ?v_208b578e_16ab_4d95_afd5_27dd8ec56a12 <http://org.xenei.jdbc4sparql/entity/Schema#tables> ?v_f13657e4_6674_4af2_8d4f_6de18d655197 ."
				+ "				        ?v_0606127e_438b_49aa_bee9_4d882d7f998f <http://org.xenei.jdbc4sparql/entity/Catalog#schemas> ?v_208b578e_16ab_4d95_afd5_27dd8ec56a12 ."
				+ "				        ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Column#columnDef> ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 ."
				+ "				        ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#autoIncrement> ?v_9f33903c_1dfb_4e0c_b212_5fdcd232c99d ."
				+ "				        ?v_f13657e4_6674_4af2_8d4f_6de18d655197 <http://org.xenei.jdbc4sparql/entity/Table#column> ?v_8f09a023_7155_43bb_810e_66727126a28c . "
				+ "				        ?v_0606127e_438b_49aa_bee9_4d882d7f998f <http://www.w3.org/2000/01/rdf-schema#label> ?v_60396d95_05cb_3f65_ac33_76af9371d45a . "
				+ "				        ?v_208b578e_16ab_4d95_afd5_27dd8ec56a12 <http://www.w3.org/2000/01/rdf-schema#label> ?v_131f86b0_fe97_3301_a6fd_4c518f5d0fa8 . "
				+ "				        ?v_f13657e4_6674_4af2_8d4f_6de18d655197 <http://www.w3.org/2000/01/rdf-schema#label> ?v_3d2e3ea9_e4d9_3606_8d7b_e9c7aed93fe4 . "
				+ "				        ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://www.w3.org/2000/01/rdf-schema#label> ?v_055e5fdf_9ffd_3734_b005_bfe91175ad41 . "
				+ "				        ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#type> ?v_866904db_b993_3be1_8caf_9d38d89b2d58"
				+ "				        OPTIONAL"
				+ "				          { ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#typeName> ?v_4686423b_c7d8_3e2f_8419_2e8d666900bc}"
				+ "				        ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#scale> ?v_90528c50_8dca_3922_b967_1b5f4bfbac16"
				+ "				        OPTIONAL"
				+ "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_caa2b141_2274_36c6_9358_ac218278a66e}"
				+ "				        OPTIONAL"
				+ "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_65190fd1_682f_34ca_b75c_f08a7d4c5e7d}"
				+ "				        OPTIONAL"
				+ "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_7d58e0e2_0d5e_38f1_b8e8_4d39ba9d492a}"
				+ "				        ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#nullable> ?v_959bda57_4240_3bae_b500_a1cb3145adb3"
				+ "				        OPTIONAL"
				+ "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Column#remarks> ?v_8631fa67_8894_3282_8dc5_d6e426b90183}"
				+ "				        OPTIONAL"
				+ "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_cd150c95_4c31_3011_bd29_c0ea109ad4de}"
				+ "				        OPTIONAL"
				+ "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_1e2f7717_47d1_35cd_8135_a9b3b25b47c6}"
				+ "				        OPTIONAL"
				+ "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_a8c445fa_46a4_3a08_a3e3_0d165ddc0e6c}"
				+ "				        OPTIONAL"
				+ "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_a34bd851_fe93_3448_904e_058bcaa2a488}"
				+ "				        { ?v_f13657e4_6674_4af2_8d4f_6de18d655197 <http://org.xenei.jdbc4sparql/entity/Table#column> _:b0 ."
				+ "				          _:b0 <http://jena.hpl.hp.com/ARQ/list#index> _:b1 ."
				+ "				          _:b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> ?v_b444882e_dad7_4aa8_9f4d_fb7ba90bfc17 ."
				+ "				          _:b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:b2 ."
				+ "				          _:b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 ."
				+ "				          _:b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"
				+ "				          BIND(( ?v_b444882e_dad7_4aa8_9f4d_fb7ba90bfc17 + 1 ) AS ?v_fdcdadc6_1aca_3423_a36b_057fb163d115)"
				+ "				        }"
				+ "				        OPTIONAL"
				+ "				          { ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#nullable> ?v_38f6b9fd_7328_4b19_8d45_54c6ec7a97e3"
				+ "				            BIND(if(( ?v_38f6b9fd_7328_4b19_8d45_54c6ec7a97e3 = 1 ), \"YES\", if(( ?v_38f6b9fd_7328_4b19_8d45_54c6ec7a97e3 = 0 ), \"NO\", \"\")) AS ?v_754a86ff_b4f2_3218_b894_8855e1bd02a2)"
				+ "				          }"
				+ "				        OPTIONAL"
				+ "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_35d0f90a_8271_317c_b412_a6eec8629b8a}"
				+ "				        OPTIONAL"
				+ "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_c535c245_5a83_3d2b_9500_346d76834403}"
				+ "				        OPTIONAL"
				+ "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_37c9ca97_f24e_3c6e_b20b_97e86525a5b4}"
				+ "				        OPTIONAL"
				+ "				          { ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#type> ?v_b311cba9_6169_32fc_a57d_af9e98adab52}"
				+ "				        { ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#autoIncrement> ?v_9f33903c_1dfb_4e0c_b212_5fdcd232c99d"
				+ "				          BIND(if(?v_9f33903c_1dfb_4e0c_b212_5fdcd232c99d, \"YES\", \"NO\") AS ?v_9c1c939e_a918_3003_bf84_dc43bee6bb5c)"
				+ "				        }"
				+ "       FILTER ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( f:CheckTypeF(?v_60396d95_05cb_3f65_ac33_76af9371d45a, 12, false) && f:CheckTypeF(?v_131f86b0_fe97_3301_a6fd_4c518f5d0fa8, 12, false) ) && f:CheckTypeF(?v_3d2e3ea9_e4d9_3606_8d7b_e9c7aed93fe4, 12, false) ) && f:CheckTypeF(?v_055e5fdf_9ffd_3734_b005_bfe91175ad41, 12, false) ) && f:CheckTypeF(?v_866904db_b993_3be1_8caf_9d38d89b2d58, 4, false) ) && f:CheckTypeF(?v_4686423b_c7d8_3e2f_8419_2e8d666900bc, 12, true) ) && f:CheckTypeF(?v_90528c50_8dca_3922_b967_1b5f4bfbac16, 4, false) ) && f:CheckTypeF(?v_caa2b141_2274_36c6_9358_ac218278a66e, 4, true) ) && f:CheckTypeF(?v_65190fd1_682f_34ca_b75c_f08a7d4c5e7d, 4, true) ) && f:CheckTypeF(?v_7d58e0e2_0d5e_38f1_b8e8_4d39ba9d492a, 4, true) ) && f:CheckTypeF(?v_959bda57_4240_3bae_b500_a1cb3145adb3, 4, false) ) && f:CheckTypeF(?v_8631fa67_8894_3282_8dc5_d6e426b90183, 12, true) ) && f:CheckTypeF(?v_cd150c95_4c31_3011_bd29_c0ea109ad4de, 12, true) ) && f:CheckTypeF(?v_1e2f7717_47d1_35cd_8135_a9b3b25b47c6, 4, true) ) && f:CheckTypeF(?v_a8c445fa_46a4_3a08_a3e3_0d165ddc0e6c, 4, true) ) && f:CheckTypeF(?v_a34bd851_fe93_3448_904e_058bcaa2a488, 4, true) ) && f:CheckTypeF(?v_fdcdadc6_1aca_3423_a36b_057fb163d115, 4, false) ) && f:CheckTypeF(?v_754a86ff_b4f2_3218_b894_8855e1bd02a2, 12, true) ) && f:CheckTypeF(?v_35d0f90a_8271_317c_b412_a6eec8629b8a, 12, true) ) && f:CheckTypeF(?v_c535c245_5a83_3d2b_9500_346d76834403, 12, true) ) && f:CheckTypeF(?v_37c9ca97_f24e_3c6e_b20b_97e86525a5b4, 12, true) ) && f:CheckTypeF(?v_b311cba9_6169_32fc_a57d_af9e98adab52, 5, true) ) && f:CheckTypeF(?v_9c1c939e_a918_3003_bf84_dc43bee6bb5c, 12, false) )"
				+ "				      }"
				+ "				      BIND(?v_35d0f90a_8271_317c_b412_a6eec8629b8a AS ?SCOPE_CATLOG)"
				+ "				      BIND(?v_a8c445fa_46a4_3a08_a3e3_0d165ddc0e6c AS ?SQL_DATETIME_SUB)"
				+ "				      BIND(?v_65190fd1_682f_34ca_b75c_f08a7d4c5e7d AS ?DECIMAL_DIGITS)"
				+ "				      BIND(?v_60396d95_05cb_3f65_ac33_76af9371d45a AS ?TABLE_CAT)"
				+ "				      BIND(?v_754a86ff_b4f2_3218_b894_8855e1bd02a2 AS ?IS_NULLABLE)"
				+ "				      BIND(?v_4686423b_c7d8_3e2f_8419_2e8d666900bc AS ?TYPE_NAME)"
				+ "				      BIND(?v_c535c245_5a83_3d2b_9500_346d76834403 AS ?SCOPE_SCHEMA)"
				+ "				      BIND(?v_b311cba9_6169_32fc_a57d_af9e98adab52 AS ?SOURCE_DATA_TYPE)"
				+ "				      BIND(?v_9c1c939e_a918_3003_bf84_dc43bee6bb5c AS ?IS_AUTOINCREMENT)"
				+ "				      BIND(?v_cd150c95_4c31_3011_bd29_c0ea109ad4de AS ?COLUMN_DEF)"
				+ "				      BIND(?v_055e5fdf_9ffd_3734_b005_bfe91175ad41 AS ?COLUMN_NAME)"
				+ "				      BIND(?v_1e2f7717_47d1_35cd_8135_a9b3b25b47c6 AS ?SQL_DATA_TYPE)"
				+ "				      BIND(?v_7d58e0e2_0d5e_38f1_b8e8_4d39ba9d492a AS ?NUM_PREC_RADIX)"
				+ "				      BIND(?v_37c9ca97_f24e_3c6e_b20b_97e86525a5b4 AS ?SCOPE_TABLE)"
				+ "				      BIND(?v_a34bd851_fe93_3448_904e_058bcaa2a488 AS ?CHAR_OCTET_LENGTH)"
				+ "				      BIND(?v_131f86b0_fe97_3301_a6fd_4c518f5d0fa8 AS ?TABLE_SCHEM)"
				+ "				      BIND(?v_fdcdadc6_1aca_3423_a36b_057fb163d115 AS ?ORDINAL_POSITION)"
				+ "				      BIND(?v_90528c50_8dca_3922_b967_1b5f4bfbac16 AS ?COLUMN_SIZE)"
				+ "				      BIND(?v_caa2b141_2274_36c6_9358_ac218278a66e AS ?BUFFER_LENGTH)"
				+ "				      BIND(?v_959bda57_4240_3bae_b500_a1cb3145adb3 AS ?NULLABLE)"
				+ "				      BIND(?v_8631fa67_8894_3282_8dc5_d6e426b90183 AS ?REMARKS)"
				+ "				      BIND(?v_866904db_b993_3be1_8caf_9d38d89b2d58 AS ?DATA_TYPE)"
				+ "				      BIND(?v_3d2e3ea9_e4d9_3606_8d7b_e9c7aed93fe4 AS ?TABLE_NAME)"
				+ "				    }"
				+ "				  }"
				+ "				ORDER BY ASC(?TABLE_CAT) ASC(?TABLE_SCHEM) ASC(?TABLE_NAME) ASC(?ORDINAL_POSITION)"
				+ "";

		// String queryString =
		// "PREFIX f: <java:org.xenei.jdbc4sparql.sparql.> SELECT DISTINCT  ?TABLE_CAT ?TABLE_SCHEM ?TABLE_NAME ?COLUMN_NAME ?DATA_TYPE ?TYPE_NAME ?COLUMN_SIZE ?BUFFER_LENGTH ?DECIMAL_DIGITS ?NUM_PREC_RADIX ?NULLABLE ?REMARKS ?COLUMN_DEF ?SQL_DATA_TYPE ?SQL_DATETIME_SUB ?CHAR_OCTET_LENGTH ?ORDINAL_POSITION ?IS_NULLABLE ?SCOPE_CATLOG ?SCOPE_SCHEMA ?SCOPE_TABLE ?SOURCE_DATA_TYPE ?IS_AUTOINCREMENT"
		// +
		// "				WHERE" +
		// "				  { { { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://org.xenei.jdbc4sparql/entity/Column> ."
		// +
		// "				        ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Column#table> ?v_f13657e4_6674_4af2_8d4f_6de18d655197 ."
		// +
		// "				        ?v_208b578e_16ab_4d95_afd5_27dd8ec56a12 <http://org.xenei.jdbc4sparql/entity/Schema#tables> ?v_f13657e4_6674_4af2_8d4f_6de18d655197 ."
		// +
		// "				        ?v_0606127e_438b_49aa_bee9_4d882d7f998f <http://org.xenei.jdbc4sparql/entity/Catalog#schemas> ?v_208b578e_16ab_4d95_afd5_27dd8ec56a12 ."
		// +
		// "				        ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Column#columnDef> ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 ."
		// +
		// "				        ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#autoIncrement> ?v_9f33903c_1dfb_4e0c_b212_5fdcd232c99d ."
		// +
		// "				        ?v_f13657e4_6674_4af2_8d4f_6de18d655197 <http://org.xenei.jdbc4sparql/entity/Table#column> ?v_8f09a023_7155_43bb_810e_66727126a28c . "
		// +
		// "				        ?v_0606127e_438b_49aa_bee9_4d882d7f998f <http://www.w3.org/2000/01/rdf-schema#label> ?v_60396d95_05cb_3f65_ac33_76af9371d45a . "
		// +
		// "				        ?v_208b578e_16ab_4d95_afd5_27dd8ec56a12 <http://www.w3.org/2000/01/rdf-schema#label> ?v_131f86b0_fe97_3301_a6fd_4c518f5d0fa8 . "
		// +
		// "				        ?v_f13657e4_6674_4af2_8d4f_6de18d655197 <http://www.w3.org/2000/01/rdf-schema#label> ?v_3d2e3ea9_e4d9_3606_8d7b_e9c7aed93fe4 . "
		// +
		// "				        ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://www.w3.org/2000/01/rdf-schema#label> ?v_055e5fdf_9ffd_3734_b005_bfe91175ad41 . "
		// +
		// "				        ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#type> ?v_866904db_b993_3be1_8caf_9d38d89b2d58"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#typeName> ?v_4686423b_c7d8_3e2f_8419_2e8d666900bc}"
		// +
		// "				        ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#scale> ?v_90528c50_8dca_3922_b967_1b5f4bfbac16"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_caa2b141_2274_36c6_9358_ac218278a66e}"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_65190fd1_682f_34ca_b75c_f08a7d4c5e7d}"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_7d58e0e2_0d5e_38f1_b8e8_4d39ba9d492a}"
		// +
		// "				        ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#nullable> ?v_959bda57_4240_3bae_b500_a1cb3145adb3"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Column#remarks> ?v_8631fa67_8894_3282_8dc5_d6e426b90183}"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_cd150c95_4c31_3011_bd29_c0ea109ad4de}"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_1e2f7717_47d1_35cd_8135_a9b3b25b47c6}"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_a8c445fa_46a4_3a08_a3e3_0d165ddc0e6c}"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_a34bd851_fe93_3448_904e_058bcaa2a488}"
		// +
		// "				        { ?v_f13657e4_6674_4af2_8d4f_6de18d655197 <http://org.xenei.jdbc4sparql/entity/Table#column> _:b0 ."
		// +
		// "				          _:b0 <http://jena.hpl.hp.com/ARQ/list#index> _:b1 ." +
		// "				          _:b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> ?v_b444882e_dad7_4aa8_9f4d_fb7ba90bfc17 ."
		// +
		// "				          _:b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:b2 ."
		// +
		// "				          _:b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 ."
		// +
		// "				          _:b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"
		// +
		// "				          BIND(( ?v_b444882e_dad7_4aa8_9f4d_fb7ba90bfc17 + 1 ) AS ?v_fdcdadc6_1aca_3423_a36b_057fb163d115)"
		// +
		// "				        }" +
		// "				        OPTIONAL" +
		// "				          { ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#nullable> ?v_38f6b9fd_7328_4b19_8d45_54c6ec7a97e3"
		// +
		// "				            BIND(if(( ?v_38f6b9fd_7328_4b19_8d45_54c6ec7a97e3 = 1 ), \"YES\", if(( ?v_38f6b9fd_7328_4b19_8d45_54c6ec7a97e3 = 0 ), \"NO\", \"\")) AS ?v_754a86ff_b4f2_3218_b894_8855e1bd02a2)"
		// +
		// "				          }" +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_35d0f90a_8271_317c_b412_a6eec8629b8a}"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_c535c245_5a83_3d2b_9500_346d76834403}"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_5ac7b517_6c40_3bcb_a218_d1805f13a221 <http://org.xenei.jdbc4sparql/entity/Table#null> ?v_37c9ca97_f24e_3c6e_b20b_97e86525a5b4}"
		// +
		// "				        OPTIONAL" +
		// "				          { ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#type> ?v_b311cba9_6169_32fc_a57d_af9e98adab52}"
		// +
		// "				        { ?v_ba3dcc5e_4e4a_4e67_8c17_111b4d6b3aa2 <http://org.xenei.jdbc4sparql/entity/ColumnDef#autoIncrement> ?v_9f33903c_1dfb_4e0c_b212_5fdcd232c99d"
		// +
		// "				          BIND(if(?v_9f33903c_1dfb_4e0c_b212_5fdcd232c99d, \"YES\", \"NO\") AS ?v_9c1c939e_a918_3003_bf84_dc43bee6bb5c)"
		// +
		// "				        }" +
		// "       FILTER (  f:CheckTypeF(?v_60396d95_05cb_3f65_ac33_76af9371d45a, 12, false)  )"
		// +
		// "				      }" +
		// "				      BIND(?v_35d0f90a_8271_317c_b412_a6eec8629b8a AS ?SCOPE_CATLOG)"
		// +
		// "				      BIND(?v_a8c445fa_46a4_3a08_a3e3_0d165ddc0e6c AS ?SQL_DATETIME_SUB)"
		// +
		// "				      BIND(?v_65190fd1_682f_34ca_b75c_f08a7d4c5e7d AS ?DECIMAL_DIGITS)"
		// +
		// "				      BIND(?v_60396d95_05cb_3f65_ac33_76af9371d45a AS ?TABLE_CAT)"
		// +
		// "				      BIND(?v_754a86ff_b4f2_3218_b894_8855e1bd02a2 AS ?IS_NULLABLE)"
		// +
		// "				      BIND(?v_4686423b_c7d8_3e2f_8419_2e8d666900bc AS ?TYPE_NAME)"
		// +
		// "				      BIND(?v_c535c245_5a83_3d2b_9500_346d76834403 AS ?SCOPE_SCHEMA)"
		// +
		// "				      BIND(?v_b311cba9_6169_32fc_a57d_af9e98adab52 AS ?SOURCE_DATA_TYPE)"
		// +
		// "				      BIND(?v_9c1c939e_a918_3003_bf84_dc43bee6bb5c AS ?IS_AUTOINCREMENT)"
		// +
		// "				      BIND(?v_cd150c95_4c31_3011_bd29_c0ea109ad4de AS ?COLUMN_DEF)"
		// +
		// "				      BIND(?v_055e5fdf_9ffd_3734_b005_bfe91175ad41 AS ?COLUMN_NAME)"
		// +
		// "				      BIND(?v_1e2f7717_47d1_35cd_8135_a9b3b25b47c6 AS ?SQL_DATA_TYPE)"
		// +
		// "				      BIND(?v_7d58e0e2_0d5e_38f1_b8e8_4d39ba9d492a AS ?NUM_PREC_RADIX)"
		// +
		// "				      BIND(?v_37c9ca97_f24e_3c6e_b20b_97e86525a5b4 AS ?SCOPE_TABLE)"
		// +
		// "				      BIND(?v_a34bd851_fe93_3448_904e_058bcaa2a488 AS ?CHAR_OCTET_LENGTH)"
		// +
		// "				      BIND(?v_131f86b0_fe97_3301_a6fd_4c518f5d0fa8 AS ?TABLE_SCHEM)"
		// +
		// "				      BIND(?v_fdcdadc6_1aca_3423_a36b_057fb163d115 AS ?ORDINAL_POSITION)"
		// +
		// "				      BIND(?v_90528c50_8dca_3922_b967_1b5f4bfbac16 AS ?COLUMN_SIZE)"
		// +
		// "				      BIND(?v_caa2b141_2274_36c6_9358_ac218278a66e AS ?BUFFER_LENGTH)"
		// +
		// "				      BIND(?v_959bda57_4240_3bae_b500_a1cb3145adb3 AS ?NULLABLE)"
		// +
		// "				      BIND(?v_8631fa67_8894_3282_8dc5_d6e426b90183 AS ?REMARKS)"
		// +
		// "				      BIND(?v_866904db_b993_3be1_8caf_9d38d89b2d58 AS ?DATA_TYPE)"
		// +
		// "				      BIND(?v_3d2e3ea9_e4d9_3606_8d7b_e9c7aed93fe4 AS ?TABLE_NAME)"
		// +
		// "				    }" +
		// "				  }" +
		// "				ORDER BY ASC(?TABLE_CAT) ASC(?TABLE_SCHEM) ASC(?TABLE_NAME) ASC(?ORDINAL_POSITION)"
		// +
		// "";
		final Schema schema = catalog.getSchema(MetaCatalogBuilder.SCHEMA_NAME);
		final Table table = schema.getTable(MetaCatalogBuilder.COLUMNS_TABLE);

		final Query query = QueryFactory.create(queryString);
		final List<QuerySolution> results = table.getCatalog()
				.executeLocalQuery(query);
		System.out.println(results.size());
		for (final QuerySolution soln : results) {
			System.out.println(soln);
			// RDFNode x = soln.get("varName") ; // Get a result variable by
			// name.
			// Resource r = soln.getResource("VarR") ; // Get a result variable
			// - must be a resource
			// RDFNode t = soln.getLiteral("VarL") ; // Get a result variable -
			// must be a literal
		}

	}
}

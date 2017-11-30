package org.xenei.jdbc4sparql.iface.name;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.xenei.jdbc4sparql.iface.NameSegments;
import org.xenei.jdbc4sparql.impl.NameUtils;

@RunWith(Parameterized.class)
public class TableNameTests {
	private TableName tableName;
	private final String catalog;
	private final String schema;
	private final String table;
	private String DBName;
	private String SPARQLName;

	@Parameters(name = "schema:{0} table:{1}")
	public static Collection<String[]> data() {
		final List<String[]> lst = new ArrayList<String[]>();
		for (final String catalog : new String[] {
				"", "catalog"
		}) {
			for (final String schema : new String[] {
					"", "schema"
			}) {
				for (final String table : new String[] {
						"", "table"
				}) {
					lst.add(new String[] {
							catalog, schema, table
					});
				}
			}
		}
		return lst;
	}

	public TableNameTests(final String catalog, final String schema,
			final String table) {
		this.catalog = catalog;
		this.schema = schema;
		this.table = table;
		tableName = new TableName(catalog, schema, table);
		if ((schema != null) && (schema.length() > 0)) {
			DBName = String.format("%s%s%s", schema, NameUtils.DB_DOT, table);
			SPARQLName = String.format("%s%s%s", schema, NameUtils.SPARQL_DOT,
					table);

		}
		else {
			if ((table != null) && (table.length() > 0)) {
				DBName = table;
				SPARQLName = table;
			}
			else {
				DBName = "";
				SPARQLName = "";
			}

		}
	}

	@Test
	public void constructionTest() {
		assertEquals(schema, tableName.getSchema());
		assertEquals(table, tableName.getTable());
		assertNull(tableName.getColumn());
		assertEquals(DBName, tableName.getDBName());
		assertEquals(SPARQLName, tableName.getSPARQLName(tableName.getUsedSegments()));
		assertEquals(table, tableName.getShortName());
	}

	@Test
	public void testSchemaWithDBDot() {
		try {
			tableName = new TableName(catalog,
					"sch" + NameUtils.DB_DOT + "ema", table);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException expected) {
			assertEquals("Schema name may not contain '.'",
					expected.getMessage());
		}

	}

	@Test
	public void testCatalogWithSPARQLDot() {
		try {
			tableName = new TableName("cat" + NameUtils.SPARQL_DOT + "alog",
					schema, table);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException expected) {
			assertEquals("Catalog name may not contain '"
					+ NameUtils.SPARQL_DOT + "'", expected.getMessage());
		}
	}

	@Test
	public void testCatalogWithDBDot() {
		try {
			tableName = new TableName("cat" + NameUtils.DB_DOT + "alog",
					schema, table);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException expected) {
			assertEquals("Catalog name may not contain '.'",
					expected.getMessage());
		}

	}

	@Test
	public void testSchemaWithSPARQLDot() {
		try {
			tableName = new TableName(catalog, "sch" + NameUtils.SPARQL_DOT
					+ "ema", table);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException expected) {
			assertEquals("Schema name may not contain '" + NameUtils.SPARQL_DOT
					+ "'", expected.getMessage());
		}
	}

	@Test
	public void testTableWithDBDot() {
		try {
			tableName = new TableName(catalog, schema, "ta" + NameUtils.DB_DOT
					+ "ble");
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException expected) {
			assertEquals("Table name may not contain '.'",
					expected.getMessage());
		}

	}

	@Test
	public void testTableWithSPARQLDot() {
		try {
			tableName = new TableName(catalog, schema, "ta"
					+ NameUtils.SPARQL_DOT + "ble");
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException expected) {
			assertEquals("Table name may not contain '" + NameUtils.SPARQL_DOT
					+ "'", expected.getMessage());
		}
	}

	@Test
	public void testTableFromTable() {
		tableName = new TableName(tableName);
		assertEquals(schema, tableName.getSchema());
		assertEquals(table, tableName.getTable());
		assertNull(tableName.getColumn());
		assertEquals(DBName, tableName.getDBName());
		assertEquals(SPARQLName, tableName.getSPARQLName(tableName.getUsedSegments()));
		assertEquals(table, tableName.getShortName());

	}

	@Test
	public void testGetColumnName() {
		ColumnName columnName = tableName.getColumnName("column");
		assertEquals(schema, columnName.getSchema());
		assertEquals(table, columnName.getTable());
		assertEquals("column", columnName.getColumn());
		assertEquals("column", columnName.getShortName());

		columnName = tableName.getColumnName("");
		assertEquals(schema, columnName.getSchema());
		assertEquals(table, columnName.getTable());
		assertEquals("", columnName.getColumn());
		assertEquals("", columnName.getShortName());

		try {
			columnName = tableName.getColumnName(null);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment column may not be null", e.getMessage());
		}

	}

	@Test
	public void testTableWithSegments() {
		final boolean tf[] = {
				true, false
		};

		NameSegments segments = null;
		for (final boolean schemaFlg : tf) {

			for (final boolean tableFlg : tf) {

				for (final boolean columnFlg : tf) {
					segments = NameSegments.getInstance(false, schemaFlg,
							tableFlg, columnFlg);

					final TableName result = new TableName(tableName, segments);
					assertEquals("Bad schema: " + segments.toString(),
							schemaFlg ? schema : null, result.getSchema());
					assertEquals("Bad table: " + segments.toString(), table,
							result.getTable()); // always returns the table
					assertNull("Bad column: " + segments.toString(),
							result.getColumn());
				}
			}
		}

	}
}

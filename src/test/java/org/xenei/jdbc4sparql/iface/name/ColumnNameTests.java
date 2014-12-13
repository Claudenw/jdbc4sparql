package org.xenei.jdbc4sparql.iface.name;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.impl.NameUtils;

@RunWith(Parameterized.class)
public class ColumnNameTests {
	private ColumnName columnName;
	private final String catalog;
	private final String schema;
	private final String table;
	private final String column;
	private String DBName;
	private String SPARQLName;

	@Parameters(name = "catalog:{0} schema:{1} table:{2} col:{3}")
	public static Collection<String[]> data() {

		List<String[]> lst = new ArrayList<String[]>();
		for (String catalog : new String[] { "", "catalog" }) {
			for (String schema : new String[] { "", "schema" }) {
				for (String table : new String[] { "", "table" }) {
					for (String column : new String[] { "", "column" }) {
						lst.add(new String[] { catalog, schema, table, column });
					}
				}
			}
		}
		return lst;

	}

	public ColumnNameTests(String catalog, String schema, String table,
			String column) {
		this.catalog = catalog;
		this.schema = schema;
		this.table = table;
		this.column = column;

		columnName = new ColumnName(catalog, schema, table, column);
		if (schema != null && schema.length() > 0) {
			DBName = String.format("%s%s", schema, NameUtils.DB_DOT);
			SPARQLName = String.format("%s%s", schema, NameUtils.SPARQL_DOT);
		} else {
			DBName = "";
			SPARQLName = "";
		}
		String ts = StringUtils.defaultString(table);
		if (ts.length() > 0 || DBName.length() > 0) {
			DBName += String.format("%s%s", ts, NameUtils.DB_DOT);
			SPARQLName += String.format("%s%s", ts, NameUtils.SPARQL_DOT);

		}

		if (column != null) {
			DBName += column;
			SPARQLName += column;
		}

	}

	@Test
	public void constructionTest() {
		assertEquals(schema, columnName.getSchema());
		assertEquals(table, columnName.getTable());
		assertEquals(column, columnName.getColumn());
		assertEquals(DBName, columnName.getDBName());
		assertEquals(SPARQLName, columnName.getSPARQLName());
		assertEquals(column, columnName.getShortName());
	}

	@Test
	public void testTableFromColumn() {
		TableName tableName = columnName.getTableName();
		assertEquals(schema, tableName.getSchema());
		assertEquals(table, tableName.getTable());
		assertNull(tableName.getColumn());
		assertEquals(table, tableName.getShortName());

	}

	@Test
	public void testCatalogWithDBDot() {
		try {
			columnName = new ColumnName("cat" + NameUtils.DB_DOT + "alog",
					schema, table, column);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
			assertEquals("Catalog name may not contain '.'",
					expected.getMessage());
		}

	}

	@Test
	public void testCatalogWithSPARQLDot() {
		try {
			columnName = new ColumnName("cat" + NameUtils.SPARQL_DOT + "alog",
					schema, table, column);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
			assertEquals("Catalog name may not contain '"
					+ NameUtils.SPARQL_DOT + "'", expected.getMessage());
		}
	}

	@Test
	public void testSchemaWithDBDot() {
		try {
			columnName = new ColumnName(catalog, "sch" + NameUtils.DB_DOT
					+ "ema", table, column);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
			assertEquals("Schema name may not contain '.'",
					expected.getMessage());
		}

	}

	@Test
	public void testSchemaWithSPARQLDot() {
		try {
			columnName = new ColumnName(catalog, "sch" + NameUtils.SPARQL_DOT
					+ "ema", table, column);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
			assertEquals("Schema name may not contain '" + NameUtils.SPARQL_DOT
					+ "'", expected.getMessage());
		}
	}

	@Test
	public void testTableWithDBDot() {
		try {
			columnName = new ColumnName(catalog, schema, "ta"
					+ NameUtils.DB_DOT + "ble", column);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
			assertEquals("Table name may not contain '.'",
					expected.getMessage());
		}

	}

	@Test
	public void testTableWithSPARQLDot() {
		try {
			columnName = new ColumnName(catalog, schema, "ta"
					+ NameUtils.SPARQL_DOT + "ble", column);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
			assertEquals("Table name may not contain '" + NameUtils.SPARQL_DOT
					+ "'", expected.getMessage());
		}
	}

	@Test
	public void testColumnWithDBDot() {
		try {
			columnName = new ColumnName(catalog, schema, table, "col"
					+ NameUtils.DB_DOT + "umn");
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
			assertEquals("Column name may not contain '.'",
					expected.getMessage());
		}

	}

	@Test
	public void testColumnWithSPARQLDot() {
		try {
			columnName = new ColumnName(catalog, schema, table, "col"
					+ NameUtils.SPARQL_DOT + "umn");
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
			assertEquals("Column name may not contain '" + NameUtils.SPARQL_DOT
					+ "'", expected.getMessage());
		}
	}

	@Test
	public void testColumnFromColumn() {
		columnName = new ColumnName(columnName);
		assertEquals(schema, columnName.getSchema());
		assertEquals(table, columnName.getTable());
		assertEquals(column, columnName.getColumn());
		assertEquals(DBName, columnName.getDBName());
		assertEquals(SPARQLName, columnName.getSPARQLName());
		assertEquals(column, columnName.getShortName());

	}

	@Test
	public void testTableFromColumnFromTable() {
		columnName = new ColumnName(columnName);
		TableName tableName = columnName.getTableName();
		assertEquals(schema, tableName.getSchema());
		assertEquals(table, tableName.getTable());
		assertNull(tableName.getColumn());
		assertEquals(table, tableName.getShortName());

		try {
			columnName = new ColumnName(new TableName("catalog", "schema2",
					"table"));
			fail("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment column may not be null", e.getMessage());
		}

		try {
			columnName = new ColumnName(new SchemaName("catalog", "schema2"));
			fail("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment table may not be null", e.getMessage());
		}

		try {
			columnName = new ColumnName(new CatalogName("catalog"));
			fail("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment schema may not be null", e.getMessage());
		}
	}

	@Test
	public void testColumnWithSegments() {
		boolean tf[] = { true, false };

		NameSegments segments = null;
		for (boolean schemaFlg : tf) {
			for (boolean tableFlg : tf) {
				for (boolean columnFlg : tf) {
					segments = NameSegments.getInstance(false, schemaFlg, tableFlg,
							columnFlg);
					ColumnName result = new ColumnName(columnName, segments);
					assertEquals("Bad schema: " + segments.toString(),
							schemaFlg ? schema : null, result.getSchema());
					assertEquals("Bad table: " + segments.toString(),
							tableFlg ? table : null, result.getTable());
					assertEquals("Bad column: " + segments.toString(), column,
							result.getColumn()); // column is always returned
				}
			}
		}

	}

}

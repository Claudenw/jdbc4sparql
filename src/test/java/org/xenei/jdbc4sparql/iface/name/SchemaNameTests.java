package org.xenei.jdbc4sparql.iface.name;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.xenei.jdbc4sparql.impl.NameUtils;

@RunWith(Parameterized.class)
public class SchemaNameTests {
	@Parameters(name = "schema:{0}")
	public static Collection<String[]> data() {
		final List<String[]> lst = new ArrayList<String[]>();
		for (final String catalog : new String[] {
				"", "catalog"
		}) {
			for (final String schema : new String[] {
					"", "schema"
			}) {
				lst.add(new String[] {
						catalog, schema
				});
			}
		}
		return lst;

	}

	private SchemaName schemaName;
	private final String catalog;
	private final String schema;
	private String DBName;

	private String SPARQLName;

	public SchemaNameTests(final String catalog, final String schema) {
		this.catalog = catalog;
		this.schema = schema;

		schemaName = new SchemaName(catalog, schema);
		if ((schema != null) && (schema.length() > 0)) {
			DBName = schema;
			SPARQLName = schema;

		}
		else {
			DBName = "";
			SPARQLName = "";

		}

	}

	@Test
	public void testConstructor() {
		Assert.assertEquals(schema, schemaName.getSchema());
		Assert.assertNull(schemaName.getTable());
		Assert.assertNull(schemaName.getColumn());
		Assert.assertEquals(DBName, schemaName.getDBName());
		Assert.assertEquals(SPARQLName, schemaName.getSPARQLName());
		Assert.assertEquals(schema, schemaName.getShortName());

	}

	@Test
	public void testSchemaFromSchema() {
		schemaName = new SchemaName(schemaName);
		Assert.assertEquals(schema, schemaName.getSchema());
		Assert.assertNull(schemaName.getTable());
		Assert.assertNull(schemaName.getColumn());
		Assert.assertEquals(DBName, schemaName.getDBName());
		Assert.assertEquals(SPARQLName, schemaName.getSPARQLName());
		Assert.assertEquals(schema, schemaName.getShortName());

	}

	@Test
	public void testCatalogWithDBDot() {
		try {
			schemaName = new SchemaName("cat" + NameUtils.DB_DOT + "alog",
					schema);
			Assert.fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException expected) {
			Assert.assertEquals("Catalog name may not contain '.'",
					expected.getMessage());
		}

	}

	@Test
	public void testCatalogWithSPARQLDot() {
		try {
			schemaName = new SchemaName("cat" + NameUtils.SPARQL_DOT + "alog",
					schema);

			Assert.fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException expected) {
			Assert.assertEquals("Catalog name may not contain '"
					+ NameUtils.SPARQL_DOT + "'", expected.getMessage());
		}
	}

	@Test
	public void testSchemaWithDBDot() {
		try {
			schemaName = new SchemaName(catalog, "sch" + NameUtils.DB_DOT
					+ "ema");
			Assert.fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException expected) {
			Assert.assertEquals("Schema name may not contain '.'",
					expected.getMessage());
		}

	}

	@Test
	public void testSchemaWithSPARQLDot() {
		try {
			schemaName = new SchemaName(catalog, "sch" + NameUtils.SPARQL_DOT
					+ "ema");

			Assert.fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException expected) {
			Assert.assertEquals("Schema name may not contain '"
					+ NameUtils.SPARQL_DOT + "'", expected.getMessage());
		}
	}

	@Test
	public void testGetTableName() {
		TableName tableName = schemaName.getTableName("table");
		assertEquals(schema, tableName.getSchema());
		assertEquals("table", tableName.getTable());
		assertNull(tableName.getColumn());
		assertEquals("table", tableName.getShortName());

		tableName = schemaName.getTableName("");
		assertEquals(schema, tableName.getSchema());
		assertEquals("", tableName.getTable());
		assertNull(tableName.getColumn());
		assertEquals("", tableName.getShortName());

		try {
			tableName = schemaName.getTableName(null);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment table may not be null", e.getMessage());
		}
	}

	@Test
	public void testSchemaWithSegments() {
		final boolean tf[] = {
				true, false
		};

		NameSegments segments = null;
		for (final boolean schemaFlg : tf) {

			for (final boolean tableFlg : tf) {

				for (final boolean columnFlg : tf) {
					segments = NameSegments.getInstance(false, schemaFlg,
							tableFlg, columnFlg);

					final SchemaName result = new SchemaName(schemaName,
							segments);
					assertEquals("Bad schema: " + segments.toString(), schema,
							result.getSchema()); // always returns the schema
					assertNull("Bad table: " + segments.toString(),
							result.getTable());
					assertNull("Bad column: " + segments.toString(),
							result.getColumn());
				}
			}
		}

	}
}

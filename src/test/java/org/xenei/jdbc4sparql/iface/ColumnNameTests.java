package org.xenei.jdbc4sparql.iface;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.xenei.jdbc4sparql.iface.ItemName.UsedSegments;
import org.xenei.jdbc4sparql.impl.NameUtils;

@RunWith( Parameterized.class )
public class ColumnNameTests
{
	private ColumnName columnName;
	private String schema;
	private String table;
	private String column;
	private String DBName;
	private String SPARQLName;

	@Parameters( name = "schema:{0} table:{1} col:{2}" )
	public static Collection<String[]> data()
	{
		return Arrays.asList(new String[][] { { null, null, null },
				{ null, null, "" }, { null, null, "column" },
				{ null, "", null }, { null, "", "" }, { null, "", "column" },
				{ null, "table", null }, { null, "table", "" },
				{ null, "table", "column" }, { "", null, null },
				{ "", null, "" }, { "", null, "column" }, { "", "", null },
				{ "", "", "" }, { "", "", "column" }, { "", "table", null },
				{ "", "table", "" }, { "", "table", "column" },
				{ "schema", null, null }, { "schema", null, "" },
				{ "schema", null, "column" }, { "schema", "", null },
				{ "schema", "", "" }, { "schema", "", "column" },
				{ "schema", "table", null }, { "schema", "table", "" },
				{ "schema", "table", "column" }, });
	}

	public ColumnNameTests( String schema, String table, String column )
	{
		this.schema = schema;
		this.table = table;
		this.column = column;
		columnName = new ColumnName(schema, table, column);
		if (schema != null && schema.length()>0)
		{
			DBName = String.format("%s%s", schema, NameUtils.DB_DOT);
			SPARQLName = String.format("%s%s", schema, NameUtils.SPARQL_DOT);
		}
		else
		{
			DBName = "";
			SPARQLName = "";
		}
		String ts = StringUtils.defaultString(table);
		if (ts.length() > 0 || DBName.length()>0)
		{
			DBName += String.format("%s%s", ts, NameUtils.DB_DOT);
			SPARQLName += String.format("%s%s", ts, NameUtils.SPARQL_DOT);

		}

		if (column != null)
		{
			DBName += column;
			SPARQLName += column;
		}

	}

	@Test
	public void constructionTest()
	{
		assertEquals(schema, columnName.getSchema());
		assertEquals(table, columnName.getTable());
		assertEquals(column, columnName.getCol());
		assertEquals(DBName, columnName.getDBName());
		assertEquals(SPARQLName, columnName.getSPARQLName());
		assertEquals(column, columnName.getShortName());
	}

	@Test
	public void testTableFromColumn()
	{
		TableName tableName = columnName.getTableName();
		assertEquals(schema, tableName.getSchema());
		assertEquals(table, tableName.getTable());
		assertNull(tableName.getCol());
		assertEquals( table, tableName.getShortName());

	}

	@Test
	public void testSchemaWithDBDot()
	{
		try
		{
			columnName = new ColumnName("sch" + NameUtils.DB_DOT + "ema",
					table, column);
			fail("Should have thrown IllegalArgumentException");
		}
		catch (IllegalArgumentException expected)
		{
			assertEquals("Schema name may not contain '.'",
					expected.getMessage());
		}

	}

	@Test
	public void testSchemaWithSPARQLDot()
	{
		try
		{
			columnName = new ColumnName("sch" + NameUtils.SPARQL_DOT + "ema",
					table, column);
			fail("Should have thrown IllegalArgumentException");
		}
		catch (IllegalArgumentException expected)
		{
			assertEquals("Schema name may not contain '" + NameUtils.SPARQL_DOT
					+ "'", expected.getMessage());
		}
	}

	@Test
	public void testTableWithDBDot()
	{
		try
		{
			columnName = new ColumnName(schema,
					"ta" + NameUtils.DB_DOT + "ble", column);
			fail("Should have thrown IllegalArgumentException");
		}
		catch (IllegalArgumentException expected)
		{
			assertEquals("Table name may not contain '.'",
					expected.getMessage());
		}

	}

	@Test
	public void testTableWithSPARQLDot()
	{
		try
		{
			columnName = new ColumnName(schema, "ta" + NameUtils.SPARQL_DOT
					+ "ble", column);
			fail("Should have thrown IllegalArgumentException");
		}
		catch (IllegalArgumentException expected)
		{
			assertEquals("Table name may not contain '" + NameUtils.SPARQL_DOT
					+ "'", expected.getMessage());
		}
	}

	@Test
	public void testColumnWithDBDot()
	{
		try
		{
			columnName = new ColumnName(schema, table, "col" + NameUtils.DB_DOT
					+ "umn");
			fail("Should have thrown IllegalArgumentException");
		}
		catch (IllegalArgumentException expected)
		{
			assertEquals("Column name may not contain '.'",
					expected.getMessage());
		}

	}

	@Test
	public void testColumnWithSPARQLDot()
	{
		try
		{
			columnName = new ColumnName(schema, table, "col"
					+ NameUtils.SPARQL_DOT + "umn");
			fail("Should have thrown IllegalArgumentException");
		}
		catch (IllegalArgumentException expected)
		{
			assertEquals("Column name may not contain '" + NameUtils.SPARQL_DOT
					+ "'", expected.getMessage());
		}
	}

	@Test
	public void testColumnFromColumn()
	{
		columnName = new ColumnName(columnName);
		assertEquals(schema, columnName.getSchema());
		assertEquals(table, columnName.getTable());
		assertEquals(column, columnName.getCol());
		assertEquals(DBName, columnName.getDBName());
		assertEquals(SPARQLName, columnName.getSPARQLName());
		assertEquals( column, columnName.getShortName());

	}

	@Test
	public void testTableFromColumnFromTable()
	{
		columnName = new ColumnName(columnName);
		TableName tableName = columnName.getTableName();
		assertEquals(schema, tableName.getSchema());
		assertEquals(table, tableName.getTable());
		assertNull(tableName.getCol());
		assertEquals( table, tableName.getShortName());

	}

	@Test
	public void testColumnWithSegments()
	{
		boolean tf[] = { true, false };

		UsedSegments segments = new UsedSegments();
		for (boolean schemaFlg : tf)
		{
			segments.setSchema(schemaFlg);
			for (boolean tableFlg : tf)
			{
				segments.setTable(tableFlg);
				for (boolean columnFlg : tf)
				{
					segments.setColumn(columnFlg);
					ColumnName result = columnName.withSegments(segments);
					assertEquals("Bad schema: " + segments.toString(),
							schemaFlg ? schema : null, result.getSchema());
					assertEquals("Bad table: " + segments.toString(),
							tableFlg ? table : null, result.getTable());
					assertEquals("Bad column: " + segments.toString(),
							columnFlg ? column : null, result.getCol());
				}
			}
		}

	}

	@Test
	public void testMerge()
	{
		for (String[] args : data())
		{
			String lbl = String.format("S:%s T:%s C:%s", args[0], args[1],
					args[2]);
			ColumnName other = new ColumnName(args[0], args[1], args[2]);
			ColumnName merged = columnName.merge(other);
			assertEquals("Bad schema: " + lbl,
					StringUtils.isEmpty(schema) ? args[0] : schema,
					merged.getSchema());
			assertEquals("Bad table: " + lbl,
					StringUtils.isEmpty(table) ? args[1] : table,
					merged.getTable());
			assertEquals("Bad column: " + lbl,
					StringUtils.isEmpty(column) ? args[2] : column,
					merged.getCol());
		}
	}

}

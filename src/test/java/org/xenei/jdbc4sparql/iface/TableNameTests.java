package org.xenei.jdbc4sparql.iface;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.xenei.jdbc4sparql.iface.ItemName.UsedSegments;
import org.xenei.jdbc4sparql.impl.NameUtils;

@RunWith(Parameterized.class)
public class TableNameTests
{
	private TableName tableName;
	private String schema;
	private String table;
	private String DBName;
	private String SPARQLName;
	
	@Parameters(name="schema:{0} table:{1}")
    public static Collection<String[]> data() {
            return Arrays.asList(new String[][] {
            		{null,null},
            		{null,""},
            		{null,"table"},
            		{"",null},
            		{"",""},
            		{"","table"},
            		{"schema",null},
            		{"schema",""},
            		{"schema","table"} });
    }

	public TableNameTests(String schema, String table)
	{
		this.schema = schema;
		this.table = table;
		tableName = new TableName(schema, table);
		if (schema != null && schema.length()>0)
		{
			DBName = String.format( "%s%s%s", schema, NameUtils.DB_DOT, table);
			SPARQLName = String.format( "%s%s%s", schema, NameUtils.SPARQL_DOT, table);
			
		} else {
			if (table != null && table.length()>0)
			{
				DBName = table;
				SPARQLName = table;
			}
			else
			{
				DBName = "";
				SPARQLName = "";		
			}
			
		}
	}

	@Test
	public void constructionTest()
	{
		assertEquals(schema, tableName.getSchema());
		assertEquals(table, tableName.getTable());
		assertNull(tableName.getCol());
		assertEquals(DBName, tableName.getDBName());
		assertEquals(SPARQLName, tableName.getSPARQLName());
		assertEquals( table, tableName.getShortName());
	}

	
	@Test
	public void testSchemaWithDBDot()
	{
		try
		{
			tableName = new TableName("sch" + NameUtils.DB_DOT + "ema", table);
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
			tableName = new TableName("sch" + NameUtils.SPARQL_DOT + "ema",
					table);
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
			tableName = new TableName(schema, "ta" + NameUtils.DB_DOT + "ble");
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
			tableName = new TableName(schema, "ta" + NameUtils.SPARQL_DOT + "ble");
			fail("Should have thrown IllegalArgumentException");
		}
		catch (IllegalArgumentException expected)
		{
			assertEquals("Table name may not contain '" + NameUtils.SPARQL_DOT
					+ "'", expected.getMessage());
		}
	}

	@Test
	public void testTableFromTable()
	{
		tableName = new TableName( tableName );
		assertEquals(schema, tableName.getSchema());
		assertEquals(table, tableName.getTable());
		assertNull(tableName.getCol());
		assertEquals(DBName, tableName.getDBName());
		assertEquals(SPARQLName, tableName.getSPARQLName());
		assertEquals( table, tableName.getShortName());

	}
	
	@Test
	public void testGetColumnName()
	{
		ColumnName columnName = tableName.getColumnName( "column");
		assertEquals(schema, columnName.getSchema());
		assertEquals(table, columnName.getTable());
		assertEquals("column", columnName.getCol());
		assertEquals( "column", columnName.getShortName());

		
		columnName = tableName.getColumnName( "");
		assertEquals(schema, columnName.getSchema());
		assertEquals(table, columnName.getTable());
		assertEquals("", columnName.getCol());
		assertEquals( "", columnName.getShortName());
		
		columnName = tableName.getColumnName( null );
		assertEquals(schema, columnName.getSchema());
		assertEquals(table, columnName.getTable());
		assertNull( columnName.getCol());
		assertNull( columnName.getShortName());

	}

	@Test
	public void testTableWithSegments()
	{
		boolean tf[] = {true,false};
	
		UsedSegments segments = new UsedSegments();
		for (boolean schemaFlg : tf )
		{
			segments.setSchema(schemaFlg);
			for (boolean tableFlg : tf )
			{
				segments.setTable(tableFlg);
				for (boolean columnFlg : tf )
				{
					segments.setColumn(columnFlg);
					TableName result = tableName.withSegments(segments);
					assertEquals( "Bad schema: "+segments.toString(), schemaFlg?schema:null, result.getSchema());
					assertEquals( "Bad table: "+segments.toString(), tableFlg?table:null, result.getTable());
					assertNull( "Bad column: "+segments.toString(), result.getCol());
				}
			}
		}
		
	}
}

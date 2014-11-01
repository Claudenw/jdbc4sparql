package org.xenei.jdbc4sparql.iface;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.xenei.jdbc4sparql.iface.ItemName.UsedSegments;
import org.xenei.jdbc4sparql.impl.NameUtils;

@RunWith( Parameterized.class )
public class SchemaNameTests
{
	@Parameters( name = "schema:{0}" )
	public static Collection<String[]> data()
	{
		return Arrays
				.asList(new String[][] { { null }, { "", }, { "schema" } });
	}

	private SchemaName schemaName;
	private final String schema;
	private String DBName;

	private String SPARQLName;

	public SchemaNameTests( final String schema )
	{
		this.schema = schema;

		schemaName = new SchemaName(schema);
		if ((schema != null) && (schema.length() > 0))
		{
			DBName = schema;
			SPARQLName = schema;

		}
		else
		{

			DBName = "";
			SPARQLName = "";

		}

	}

	@Test
	public void testConstructor()
	{
		Assert.assertEquals(schema, schemaName.getSchema());
		Assert.assertNull(schemaName.getTable());
		Assert.assertNull(schemaName.getCol());
		Assert.assertEquals(DBName, schemaName.getDBName());
		Assert.assertEquals(SPARQLName, schemaName.getSPARQLName());
		Assert.assertEquals( schema, schemaName.getShortName());

	}

	@Test
	public void testSchemaFromSchema()
	{
		schemaName = new SchemaName(schemaName);
		Assert.assertEquals(schema, schemaName.getSchema());
		Assert.assertNull(schemaName.getTable());
		Assert.assertNull(schemaName.getCol());
		Assert.assertEquals(DBName, schemaName.getDBName());
		Assert.assertEquals(SPARQLName, schemaName.getSPARQLName());
		Assert.assertEquals( schema, schemaName.getShortName());

	}

	@Test
	public void testSchemaWithDBDot()
	{
		try
		{
			schemaName = new SchemaName("sch" + NameUtils.DB_DOT + "ema");
			Assert.fail("Should have thrown IllegalArgumentException");
		}
		catch (final IllegalArgumentException expected)
		{
			Assert.assertEquals("Schema name may not contain '.'",
					expected.getMessage());
		}

	}

	@Test
	public void testSchemaWithSPARQLDot()
	{
		try
		{
			schemaName = new SchemaName("sch" + NameUtils.SPARQL_DOT + "ema");

			Assert.fail("Should have thrown IllegalArgumentException");
		}
		catch (final IllegalArgumentException expected)
		{
			Assert.assertEquals("Schema name may not contain '"
					+ NameUtils.SPARQL_DOT + "'", expected.getMessage());
		}
	}

	@Test
	public void testGetTableName()
	{
		TableName tableName = schemaName.getTableName( "table");
		assertEquals(schema, tableName.getSchema());
		assertEquals("table", tableName.getTable());
		assertNull( tableName.getCol());
		assertEquals( "table", tableName.getShortName());
		
		tableName = schemaName.getTableName( "");
		assertEquals(schema, tableName.getSchema());
		assertEquals("", tableName.getTable());
		assertNull( tableName.getCol());
		assertEquals( "", tableName.getShortName());
		
		tableName = schemaName.getTableName( null );
		assertEquals(schema, tableName.getSchema());
		assertNull( tableName.getTable());
		assertNull( tableName.getCol());
		assertNull( tableName.getShortName());

	}

	@Test
	public void testSchemaWithSegments()
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
					SchemaName result = schemaName.withSegments(segments);
					assertEquals( "Bad schema: "+segments.toString(), schemaFlg?schema:null, result.getSchema());
					assertNull( "Bad table: "+segments.toString(), result.getTable());
					assertNull( "Bad column: "+segments.toString(), result.getCol());
				}
			}
		}
		
	}
}

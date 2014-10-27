package org.xenei.jdbc4sparql.sparql;

import org.junit.Assert;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.ColumnName;
import org.xenei.jdbc4sparql.iface.ItemName;
import org.xenei.jdbc4sparql.iface.TableName;

public class QueryItemNameTest
{
	private final TableName tableNames[] = {
			new TableName(null, null),
			new TableName(null, "table"),
			new TableName("schema", null),
			new TableName("schema", "table") };

	private final ColumnName columnNames[] = {
			new ColumnName(null, null, null),
			new ColumnName(null, null, "column"),
			new ColumnName(null, "table", null),
			new ColumnName(null, "table", "column"),
			new ColumnName("schema", null, null),
			new ColumnName("schema", null, "column"),
			new ColumnName("schema", "table", null),
			new ColumnName("schema", "table", "column") };

	private void testEquality( final ItemName name1,
			final ItemName name2 )
	{
		Assert.assertEquals(name1, name2);
		Assert.assertEquals(name2, name1);
		Assert.assertEquals(name1.hashCode(), name2.hashCode());
	}

	@Test
	public void testEquals()
	{

		// test table name

		for (final TableName tableName : tableNames)
		{
			testEquality(tableName, tableName);
		}

		TableName tableName1 = new TableName(null,
				null);
		TableName tableName2 = new TableName(null,
				null);
		testEquality(tableName1, tableName2);

		tableName1 = new TableName(null, "table");
		tableName2 = new TableName(null, "table");
		testEquality(tableName1, tableName2);

		tableName1 = new TableName("schema", null);
		tableName2 = new TableName("schema", null);
		testEquality(tableName1, tableName2);

		tableName1 = new TableName("schema", "table");
		tableName2 = new TableName("schema", "table");
		testEquality(tableName1, tableName2);

		// -- test column Name

		for (final org.xenei.jdbc4sparql.iface.ColumnName columnName : columnNames)
		{
			testEquality(columnName, columnName);
		}

		ColumnName columnName1 = new ColumnName(
				null, null, null);
		ColumnName columnName2 = new ColumnName(
				null, null, null);
		testEquality(tableName1, tableName2);

		columnName1 = new ColumnName(null, null, "column");
		columnName2 = new ColumnName(null, null, "column");
		testEquality(tableName1, tableName2);

		columnName1 = new ColumnName(null, "table", null);
		columnName2 = new ColumnName(null, "table", null);
		testEquality(columnName1, columnName2);

		columnName1 = new ColumnName(null, "table", "column");
		columnName2 = new ColumnName(null, "table", "column");
		testEquality(columnName1, columnName2);

		columnName1 = new ColumnName("schema", null, null);
		columnName2 = new ColumnName("schema", null, null);
		testEquality(columnName1, columnName2);

		columnName1 = new ColumnName("schema", null, "column");
		columnName2 = new ColumnName("schema", null, "column");
		testEquality(columnName1, columnName2);

		columnName1 = new ColumnName("schema", "table", null);
		columnName2 = new ColumnName("schema", "table", null);
		testEquality(columnName1, columnName2);

		columnName1 = new ColumnName("schema", "table",
				"column");
		columnName2 = new ColumnName("schema", "table",
				"column");
		testEquality(columnName1, columnName2);
	}

	private void testInequality( final ItemName[] names )
	{
		for (int i = 0; i < names.length; i++)
		{
			for (int j = 0; j < names.length; j++)
			{
				if (j != i)
				{
					Assert.assertNotEquals(names[i], names[j]);
				}
			}
		}
	}

	@Test
	public void testMatch()
	{
		for (int i = 0; i < tableNames.length; i++)
		{
			testMatch(tableNames[i], tableNames[i]);
			switch (i)
			{
				case 0:
					for (int j = 1; j < tableNames.length; j++)
					{
						testMatch(tableNames[i], tableNames[j]);
					}
					break;
				case 1:
					testNotMatch(tableNames[i], tableNames[0]);
					testNotMatch(tableNames[i], tableNames[2]);
					testMatch(tableNames[i], tableNames[3]);
					break;

				case 2:
					testNotMatch(tableNames[i], tableNames[0]);
					testNotMatch(tableNames[i], tableNames[1]);
					testMatch(tableNames[i], tableNames[3]);
					break;
				case 3:
					testNotMatch(tableNames[i], tableNames[0]);
					testNotMatch(tableNames[i], tableNames[1]);
					testNotMatch(tableNames[i], tableNames[2]);
					break;
				default:
					Assert.fail("Table name index " + i + " not expected");
			}
		}

		for (int i = 0; i < columnNames.length; i++)
		{
			testMatch(columnNames[i], columnNames[i]);
			switch (i)
			{
				case 0:
					for (int j = 1; j < columnNames.length; j++)
					{
						testMatch(columnNames[i], columnNames[j]);
					}
					break;
				case 1:
					testNotMatch(columnNames[i], columnNames[0]);
					testNotMatch(columnNames[i], columnNames[2]);
					testMatch(columnNames[i], columnNames[3]);
					testNotMatch(columnNames[i], columnNames[4]);
					testMatch(columnNames[i], columnNames[5]);
					testNotMatch(columnNames[i], columnNames[6]);
					testMatch(columnNames[i], columnNames[7]);
					break;

				case 2:
					testNotMatch(columnNames[i], columnNames[0]);
					testNotMatch(columnNames[i], columnNames[1]);
					testMatch(columnNames[i], columnNames[3]);
					testNotMatch(columnNames[i], columnNames[4]);
					testNotMatch(columnNames[i], columnNames[5]);
					testMatch(columnNames[i], columnNames[6]);
					testMatch(columnNames[i], columnNames[7]);
					break;
				case 3:
					testNotMatch(columnNames[i], columnNames[0]);
					testNotMatch(columnNames[i], columnNames[1]);
					testNotMatch(columnNames[i], columnNames[2]);
					testNotMatch(columnNames[i], columnNames[4]);
					testNotMatch(columnNames[i], columnNames[5]);
					testNotMatch(columnNames[i], columnNames[6]);
					testMatch(columnNames[i], columnNames[7]);
					break;
				case 4:
					testNotMatch(columnNames[i], columnNames[0]);
					testNotMatch(columnNames[i], columnNames[1]);
					testNotMatch(columnNames[i], columnNames[2]);
					testNotMatch(columnNames[i], columnNames[3]);
					testMatch(columnNames[i], columnNames[5]);
					testMatch(columnNames[i], columnNames[6]);
					testMatch(columnNames[i], columnNames[7]);
					break;
				case 5:
					testNotMatch(columnNames[i], columnNames[0]);
					testNotMatch(columnNames[i], columnNames[1]);
					testNotMatch(columnNames[i], columnNames[2]);
					testNotMatch(columnNames[i], columnNames[3]);
					testNotMatch(columnNames[i], columnNames[4]);
					testNotMatch(columnNames[i], columnNames[6]);
					testMatch(columnNames[i], columnNames[7]);
					break;
				case 6:
					testNotMatch(columnNames[i], columnNames[0]);
					testNotMatch(columnNames[i], columnNames[1]);
					testNotMatch(columnNames[i], columnNames[2]);
					testNotMatch(columnNames[i], columnNames[3]);
					testNotMatch(columnNames[i], columnNames[4]);
					testNotMatch(columnNames[i], columnNames[5]);
					testMatch(columnNames[i], columnNames[7]);
					break;
				case 7:
					testNotMatch(columnNames[i], columnNames[0]);
					testNotMatch(columnNames[i], columnNames[1]);
					testNotMatch(columnNames[i], columnNames[2]);
					testNotMatch(columnNames[i], columnNames[3]);
					testNotMatch(columnNames[i], columnNames[4]);
					testNotMatch(columnNames[i], columnNames[5]);
					testNotMatch(columnNames[i], columnNames[6]);
					break;
				default:
					Assert.fail("Column name index " + i + " not expected");

			}

		}
	}

	public void testMatch( final ItemName name1, final ItemName name2 )
	{
		final String s = String.format("%s does not match %s", name1, name2);
		Assert.assertTrue(s, name1.matches(name2));
	}

	@Test
	public void testNotEquals()
	{

		// test table name

		testInequality(tableNames);

		testInequality(columnNames);

	}

	public void testNotMatch( final ItemName name1,
			final ItemName name2 )
	{
		final String s = String.format("%s does match %s", name1, name2);
		Assert.assertFalse(s, name1.matches(name2));
	}
}

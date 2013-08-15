package org.xenei.jdbc4sparql.sparql;

import org.junit.Assert;
import org.junit.Test;
import org.xenei.jdbc4sparql.sparql.QueryTableInfo.Name;

public class QueryItemNameTest
{
	private final QueryTableInfo.Name tableNames[] = {
			QueryTableInfo.getNameInstance(null, null),
			QueryTableInfo.getNameInstance(null, "table"),
			QueryTableInfo.getNameInstance("schema", null),
			QueryTableInfo.getNameInstance("schema", "table") };

	private final QueryColumnInfo.Name columnNames[] = {
			QueryColumnInfo.getNameInstance(null, null, null),
			QueryColumnInfo.getNameInstance(null, null, "column"),
			QueryColumnInfo.getNameInstance(null, "table", null),
			QueryColumnInfo.getNameInstance(null, "table", "column"),
			QueryColumnInfo.getNameInstance("schema", null, null),
			QueryColumnInfo.getNameInstance("schema", null, "column"),
			QueryColumnInfo.getNameInstance("schema", "table", null),
			QueryColumnInfo.getNameInstance("schema", "table", "column") };

	private void testEquality( final QueryItemName name1,
			final QueryItemName name2 )
	{
		Assert.assertEquals(name1, name2);
		Assert.assertEquals(name2, name1);
		Assert.assertEquals(name1.hashCode(), name2.hashCode());
	}

	@Test
	public void testEquals()
	{

		// test table name

		for (final Name tableName : tableNames)
		{
			testEquality(tableName, tableName);
		}

		QueryTableInfo.Name tableName1 = QueryTableInfo.getNameInstance(null,
				null);
		QueryTableInfo.Name tableName2 = QueryTableInfo.getNameInstance(null,
				null);
		testEquality(tableName1, tableName2);

		tableName1 = QueryTableInfo.getNameInstance(null, "table");
		tableName2 = QueryTableInfo.getNameInstance(null, "table");
		testEquality(tableName1, tableName2);

		tableName1 = QueryTableInfo.getNameInstance("schema", null);
		tableName2 = QueryTableInfo.getNameInstance("schema", null);
		testEquality(tableName1, tableName2);

		tableName1 = QueryTableInfo.getNameInstance("schema", "table");
		tableName2 = QueryTableInfo.getNameInstance("schema", "table");
		testEquality(tableName1, tableName2);

		// -- test column Name

		for (final org.xenei.jdbc4sparql.sparql.QueryColumnInfo.Name columnName : columnNames)
		{
			testEquality(columnName, columnName);
		}

		QueryColumnInfo.Name columnName1 = QueryColumnInfo.getNameInstance(
				null, null, null);
		QueryColumnInfo.Name columnName2 = QueryColumnInfo.getNameInstance(
				null, null, null);
		testEquality(tableName1, tableName2);

		columnName1 = QueryColumnInfo.getNameInstance(null, null, "column");
		columnName2 = QueryColumnInfo.getNameInstance(null, null, "column");
		testEquality(tableName1, tableName2);

		columnName1 = QueryColumnInfo.getNameInstance(null, "table", null);
		columnName2 = QueryColumnInfo.getNameInstance(null, "table", null);
		testEquality(columnName1, columnName2);

		columnName1 = QueryColumnInfo.getNameInstance(null, "table", "column");
		columnName2 = QueryColumnInfo.getNameInstance(null, "table", "column");
		testEquality(columnName1, columnName2);

		columnName1 = QueryColumnInfo.getNameInstance("schema", null, null);
		columnName2 = QueryColumnInfo.getNameInstance("schema", null, null);
		testEquality(columnName1, columnName2);

		columnName1 = QueryColumnInfo.getNameInstance("schema", null, "column");
		columnName2 = QueryColumnInfo.getNameInstance("schema", null, "column");
		testEquality(columnName1, columnName2);

		columnName1 = QueryColumnInfo.getNameInstance("schema", "table", null);
		columnName2 = QueryColumnInfo.getNameInstance("schema", "table", null);
		testEquality(columnName1, columnName2);

		columnName1 = QueryColumnInfo.getNameInstance("schema", "table",
				"column");
		columnName2 = QueryColumnInfo.getNameInstance("schema", "table",
				"column");
		testEquality(columnName1, columnName2);
	}

	private void testInequality( final QueryItemName[] names )
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

	public void testMatch( final QueryItemName name1, final QueryItemName name2 )
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

	public void testNotMatch( final QueryItemName name1,
			final QueryItemName name2 )
	{
		final String s = String.format("%s does match %s", name1, name2);
		Assert.assertFalse(s, name1.matches(name2));
	}
}

package org.xenei.jdbc4sparql;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.mock.MockCatalog;
import org.xenei.jdbc4sparql.mock.MockConnection;
import org.xenei.jdbc4sparql.mock.MockDriver;

public class J4SDatabaseMetaDataTest
{

	private J4SDatabaseMetaData metadata;
	
	@Before
	public void setup()
	{
		MockDriver driver = new MockDriver();
		metadata = new J4SDatabaseMetaData(new MockConnection(driver, null, null), driver, new MockCatalog());
	}

	@Test
	public void testGetAllTables() throws SQLException
	{
		String[] names = {"Attributes",
		"BestRow",
		"Catalogs",
		"ClientInfo",
		"ColumnPriviliges",
		"Columns",
		"ExportedKeys",
		"FunctionColumns",
		"Functions",
		"ImportedKeys",
		"IndexInfo",
		"PrimaryKeys",
		"ProcedureColumns",
		"Procedures",
		"Schemas",
		"SuperTables",
		"SuperTypes",
		"TablePriv",
		"TableTypes",
		"Tables",
		"TypeInfo",
		"UDTs",
		"Version",
		"XrefKeys" };
		
		ResultSet rs = metadata.getTables(null, null, null, null);
		rs.first();
		
		for (int i=0;i<names.length;i++)
		{
			Assert.assertEquals( "Jdbc4Sparql_METADATA", rs.getString(1)); // TABLE_CAT
			Assert.assertEquals( "Jdbc4Sparql_SCHEMA", rs.getString(2)); // TABLE_SCHEM
			Assert.assertEquals( names[i], rs.getString(3)); // TABLE_NAME
			Assert.assertEquals( "TABLE", rs.getString(4)); // TABLE_TYPE
			Assert.assertEquals( "", rs.getString(5)); // REMARKS
			rs.next();
		}
		Assert.assertTrue( rs.isAfterLast());
			
	}
}

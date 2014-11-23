package org.xenei.jdbc4sparql.iface.name;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class RenamedBaseNameTest {

	public static final String CATALOG = "catalog";
	public static final String SCHEMA = "schema";
	public static final String TABLE = "table";
	public static final String COLUMN = "column";

	private RenamedBaseName baseName;
	private BaseName base;
	private String catalog;
	private String schema;
	private String table;
	private String column;

	@Parameters(name = "catalog:{0} schema:{1} table:{2} col:{3}")
	public static Collection<String[]> data() {

		List<String[]> lst = new ArrayList<String[]>();
		for (String catalog : new String[] { null, "", CATALOG + "1" }) {
			for (String schema : new String[] { null, "", SCHEMA + "1" }) {
				for (String table : new String[] { null, "", TABLE + "1" }) {
					for (String column : new String[] { null, "", COLUMN + "1" }) {
						lst.add(new String[] { catalog, schema, table, column });
					}
				}
			}
		}
		return lst;

	}

	public RenamedBaseNameTest(String catalog, String schema, String table,
			String column) {
		this.catalog = catalog;
		this.schema = schema;
		this.table = table;
		this.column = column;
		this.base = new BaseNameImpl(CATALOG, SCHEMA, TABLE, COLUMN);
		this.baseName = new RenamedBaseName(base, catalog, schema, table,
				column);
	}

	@Test
	public void testGetCatalog() {
		assertEquals(StringUtils.defaultString(catalog, CATALOG),
				baseName.getCatalog());
	}

	@Test
	public void testGetSchema() {
		assertEquals(StringUtils.defaultString(schema, SCHEMA),
				baseName.getSchema());
	}

	@Test
	public void testGetTable() {
		assertEquals(StringUtils.defaultString(table, TABLE),
				baseName.getTable());
	}

	@Test
	public void testGetColumn() {
		assertEquals(StringUtils.defaultString(column, COLUMN),
				baseName.getCol());
	}

	@Test
	public void testEquality() {
		BaseName bn2 = new BaseNameImpl(StringUtils.defaultString(catalog,
				CATALOG), StringUtils.defaultString(schema, SCHEMA),
				StringUtils.defaultString(table, TABLE),
				StringUtils.defaultString(column, COLUMN));
		assertEquals(baseName, bn2);
		assertEquals(bn2, baseName);
		assertEquals(baseName.hashCode(), bn2.hashCode());
	}

	@Test
	public void testGetGUID() {
		assertEquals(base.getGUID(), baseName.getGUID());
	}

	// @Test
	// public void testInEquality( )
	// {
	// BaseName bn2 = new BaseNameImpl( CATALOG, SCHEMA, TABLE, COLUMN+"1" );
	// assertNotEquals( baseName, bn2 );
	// assertNotEquals( bn2, baseName );
	//
	// bn2 = new BaseNameImpl( CATALOG, SCHEMA, TABLE+"1", COLUMN );
	// assertNotEquals( baseName, bn2 );
	// assertNotEquals( bn2, baseName );
	//
	// bn2 = new BaseNameImpl( CATALOG, SCHEMA+"1", TABLE, COLUMN );
	// assertNotEquals( baseName, bn2 );
	// assertNotEquals( bn2, baseName );
	//
	// bn2 = new BaseNameImpl( CATALOG+"1", SCHEMA, TABLE, COLUMN );
	// assertNotEquals( baseName, bn2 );
	// assertNotEquals( bn2, baseName );
	// }

}

package org.xenei.jdbc4sparql.iface.name;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class BaseNameImplTest {

	public static final String CATALOG = "catalog";
	public static final String SCHEMA = "schema";
	public static final String TABLE = "table";
	public static final String COLUMN = "column";

	protected FQNameNameImpl baseName;

	@Before
	public void setup() {
		baseName = new FQNameNameImpl(CATALOG, SCHEMA, TABLE, COLUMN);
	}

	@Test
	public void testGetCatalog() {
		assertEquals(CATALOG, baseName.getCatalog());
	}

	@Test
	public void testGetSchema() {
		assertEquals(SCHEMA, baseName.getSchema());
	}

	@Test
	public void testGetTable() {
		assertEquals(TABLE, baseName.getTable());
	}

	@Test
	public void testGetColumn() {
		assertEquals(COLUMN, baseName.getColumn());
	}

	@Test
	public void testEquality() {
		FQName bn2 = new FQNameNameImpl(CATALOG, SCHEMA, TABLE, COLUMN);
		assertEquals(baseName, bn2);
		assertEquals(bn2, baseName);
		assertEquals(baseName.hashCode(), bn2.hashCode());
	}

	@Test
	public void testInEquality() {
		FQName bn2 = new FQNameNameImpl(CATALOG, SCHEMA, TABLE, COLUMN + "1");
		assertNotEquals(baseName, bn2);
		assertNotEquals(bn2, baseName);

		bn2 = new FQNameNameImpl(CATALOG, SCHEMA, TABLE + "1", COLUMN);
		assertNotEquals(baseName, bn2);
		assertNotEquals(bn2, baseName);

		bn2 = new FQNameNameImpl(CATALOG, SCHEMA + "1", TABLE, COLUMN);
		assertNotEquals(baseName, bn2);
		assertNotEquals(bn2, baseName);

		bn2 = new FQNameNameImpl(CATALOG + "1", SCHEMA, TABLE, COLUMN);
		assertNotEquals(baseName, bn2);
		assertNotEquals(bn2, baseName);

		bn2 = new FQNameNameImpl(CATALOG, SCHEMA, TABLE, null);
		assertNotEquals(baseName, bn2);
		assertNotEquals(bn2, baseName);

		bn2 = new FQNameNameImpl(CATALOG, SCHEMA, null, COLUMN);
		assertNotEquals(baseName, bn2);
		assertNotEquals(bn2, baseName);

		bn2 = new FQNameNameImpl(CATALOG, null, TABLE, COLUMN);
		assertNotEquals(baseName, bn2);
		assertNotEquals(bn2, baseName);

		bn2 = new FQNameNameImpl(null, SCHEMA, TABLE, COLUMN);
		assertNotEquals(baseName, bn2);
		assertNotEquals(bn2, baseName);
	}

}

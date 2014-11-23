package org.xenei.jdbc4sparql.iface.name;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class NameSegmentTest {
	private BaseName baseName;
	private NameSegments segments;
	private boolean catalog;
	private boolean schema;
	private boolean table;
	private boolean column;

	public NameSegmentTest(Boolean catalog, Boolean schema, Boolean table,
			Boolean column) {
		segments = new NameSegments(catalog, schema, table, column);
		baseName = new BaseNameImpl("catalog", "schema", "table", "column");
		this.catalog = catalog;
		this.schema = schema;
		this.table = table;
		this.column = column;
	}

	@Parameters(name = "catalog:{0} schema:{1} table:{2} col:{3}")
	public static Collection<Boolean[]> data() {
		Boolean vals[] = new Boolean[] { Boolean.TRUE, Boolean.FALSE };
		List<Boolean[]> lst = new ArrayList<Boolean[]>();
		for (Boolean catalog : vals) {
			for (Boolean schema : vals) {
				for (Boolean table : vals) {
					for (Boolean column : vals) {
						lst.add(new Boolean[] { catalog, schema, table, column });
					}
				}
			}
		}
		return lst;
	}

	@Test
	public void testGetCatalog() {
		assertEquals(catalog ? "catalog" : null, segments.getCatalog(baseName));
	}

	@Test
	public void testGetSchema() {
		assertEquals(schema ? "schema" : null, segments.getSchema(baseName));
	}

	@Test
	public void testGetTable() {
		assertEquals(table ? "table" : null, segments.getTable(baseName));
	}

	@Test
	public void testGetColumn() {
		assertEquals(column ? "column" : null, segments.getColumn(baseName));
	}

}

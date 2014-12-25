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
	private final FQName baseName;
	private final NameSegments segments;
	private final boolean catalog;
	private final boolean schema;
	private final boolean table;
	private final boolean column;

	public NameSegmentTest(final Boolean catalog, final Boolean schema,
			final Boolean table, final Boolean column) {
		segments = NameSegments.getInstance(catalog, schema, table, column);
		baseName = new FQNameImpl("catalog", "schema", "table", "column");
		this.catalog = catalog;
		this.schema = schema;
		this.table = table;
		this.column = column;
	}

	@Parameters(name = "catalog:{0} schema:{1} table:{2} col:{3}")
	public static Collection<Boolean[]> data() {
		final Boolean vals[] = new Boolean[] {
				Boolean.TRUE, Boolean.FALSE
		};
		final List<Boolean[]> lst = new ArrayList<Boolean[]>();
		for (final Boolean catalog : vals) {
			for (final Boolean schema : vals) {
				for (final Boolean table : vals) {
					for (final Boolean column : vals) {
						lst.add(new Boolean[] {
								catalog, schema, table, column
						});
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

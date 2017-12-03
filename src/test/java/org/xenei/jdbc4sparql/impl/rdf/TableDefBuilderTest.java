package org.xenei.jdbc4sparql.impl.rdf;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.impl.EntityManagerImpl;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

public class TableDefBuilderTest {
	private Model model;
	private EntityManager mgr;

	private RdfKey getPrimaryKey() {
		new RdfTableDef.Builder();
		final RdfKey.Builder builder = new RdfKey.Builder()
				.addSegment(new RdfKeySegment.Builder().build( mgr ))
				.setUnique(true).setKeyName("PK");
		return builder.build( mgr );
	}

	private RdfKey getSortKey() {
		new RdfTableDef.Builder();
		final RdfKey.Builder builder = new RdfKey.Builder()
				.addSegment(new RdfKeySegment.Builder().setIdx(1).build( mgr ))
				.setUnique(false).setKeyName("SK");
		return builder.build( mgr );
	}

	@Before
	public void setUp() throws Exception {
		model = ModelFactory.createDefaultModel();
		mgr = EntityManagerFactory.create( model );
	}

	@After
	public void tearDown() throws Exception {
		model.close();
	}

	@Test
	public void testBadPrimaryKey() {
		try {
			new RdfTableDef.Builder()
					.addColumnDef(
							RdfColumnDef.Builder.getStringBuilder()
									.build( mgr ))
					.addColumnDef(
							RdfColumnDef.Builder.getIntegerBuilder().build(
									mgr )).setPrimaryKey(getSortKey());
			Assert.fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			// expected
		}
	}

	@Test
	public void testDefault() {
		final RdfTableDef.Builder builder = new RdfTableDef.Builder()
				.addColumnDef(
						RdfColumnDef.Builder.getStringBuilder().build( mgr ))
				.addColumnDef(
						RdfColumnDef.Builder.getIntegerBuilder().build( mgr ));
		final TableDef tableDef = builder.build( mgr );

		Assert.assertEquals(2, tableDef.getColumnCount());
		Assert.assertEquals(RdfColumnDef.Builder.getStringBuilder()
				.build( mgr ), tableDef.getColumnDef(0));
		Assert.assertEquals(
				RdfColumnDef.Builder.getIntegerBuilder().build( mgr ),
				tableDef.getColumnDef(1));
		Assert.assertEquals(1, tableDef.getColumnIndex(RdfColumnDef.Builder
				.getIntegerBuilder().build( mgr )));
		Assert.assertNotNull(tableDef.getColumnDefs());
		Assert.assertNull(tableDef.getPrimaryKey());
		Assert.assertNull(tableDef.getSortKey());
		Assert.assertNull(tableDef.getSuperTableDef());
	}

	@Test
	public void testPrimaryKey() {
		final RdfTableDef.Builder builder = new RdfTableDef.Builder()
				.addColumnDef(
						RdfColumnDef.Builder.getStringBuilder().build( mgr ))
				.addColumnDef(
						RdfColumnDef.Builder.getIntegerBuilder().build( mgr ))
				.setPrimaryKey(getPrimaryKey());
		final TableDef tableDef = builder.build( mgr );

		Assert.assertEquals(2, tableDef.getColumnCount());
		Assert.assertEquals(RdfColumnDef.Builder.getStringBuilder()
				.build( mgr ), tableDef.getColumnDef(0));
		Assert.assertEquals(
				RdfColumnDef.Builder.getIntegerBuilder().build( mgr ),
				tableDef.getColumnDef(1));
		Assert.assertEquals(1, tableDef.getColumnIndex(RdfColumnDef.Builder
				.getIntegerBuilder().build( mgr )));
		Assert.assertNotNull(tableDef.getColumnDefs());
		Assert.assertEquals(getPrimaryKey(), tableDef.getPrimaryKey());
		Assert.assertNull(tableDef.getSortKey());
		Assert.assertNull(tableDef.getSuperTableDef());
	}

	@Test
	public void testSortKey() {
		final RdfTableDef.Builder builder = new RdfTableDef.Builder()
				.addColumnDef(
						RdfColumnDef.Builder.getStringBuilder().build( mgr ))
				.addColumnDef(
						RdfColumnDef.Builder.getIntegerBuilder().build( mgr ))
				.setSortKey(getSortKey());
		final TableDef tableDef = builder.build( mgr );

		Assert.assertEquals(2, tableDef.getColumnCount());
		Assert.assertEquals(RdfColumnDef.Builder.getStringBuilder()
				.build( mgr ), tableDef.getColumnDef(0));
		Assert.assertEquals(
				RdfColumnDef.Builder.getIntegerBuilder().build( mgr ),
				tableDef.getColumnDef(1));
		Assert.assertEquals(1, tableDef.getColumnIndex(RdfColumnDef.Builder
				.getIntegerBuilder().build( mgr )));
		Assert.assertNotNull(tableDef.getColumnDefs());
		Assert.assertNull(tableDef.getPrimaryKey());
		Assert.assertEquals(getSortKey(), tableDef.getSortKey());
		Assert.assertNull(tableDef.getSuperTableDef());
	}

	@Test
	public void testSuperTable() {
		RdfTableDef.Builder builder = new RdfTableDef.Builder().addColumnDef(
				RdfColumnDef.Builder.getStringBuilder().build( mgr ))
				.addColumnDef(
						RdfColumnDef.Builder.getIntegerBuilder().build( mgr ));
		final RdfTableDef tableDef2 = builder.build( mgr );

		builder = new RdfTableDef.Builder()
				.addColumnDef(
						RdfColumnDef.Builder.getStringBuilder().build( mgr ))
				.addColumnDef(
						RdfColumnDef.Builder.getIntegerBuilder().build( mgr ))
				.setSuperTableDef(tableDef2);
		final TableDef tableDef = builder.build( mgr );

		Assert.assertEquals(2, tableDef.getColumnCount());
		Assert.assertEquals(RdfColumnDef.Builder.getStringBuilder()
				.build( mgr ), tableDef.getColumnDef(0));
		Assert.assertEquals(
				RdfColumnDef.Builder.getIntegerBuilder().build( mgr ),
				tableDef.getColumnDef(1));
		Assert.assertEquals(1, tableDef.getColumnIndex(RdfColumnDef.Builder
				.getIntegerBuilder().build( mgr )));
		Assert.assertNotNull(tableDef.getColumnDefs());
		Assert.assertNull(tableDef.getPrimaryKey());
		Assert.assertNull(tableDef.getSortKey());
		Assert.assertEquals(tableDef2, tableDef.getSuperTableDef());
	}
}

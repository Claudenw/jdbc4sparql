/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.jdbc4sparql.sparql.builders;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlParserImpl;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.impl.EntityManagerImpl;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

public class SimpleBuilderTest {
	private static final String NS = "http://example.com/jdbc4sparql#";
	private RdfCatalog catalog;
	private RdfSchema schema;
	// data model
	private Model model;
	private RDFConnection mgr;
	// schema model
	private Model schemaModel;
	private EntityManager sMgr;
	private SchemaBuilder builder;
	private Map<String, Catalog> catalogs;
	private SparqlParser parser;

	private void addModelData(final Model model) {
		model.removeAll();
		final Resource fooType = model.createResource(SimpleBuilderTest.NS
				+ "fooTable");
		final Resource foo1 = model.createResource(fooType);
		final Resource foo2 = model.createResource(fooType);
		final Property stringCol = model.createProperty(SimpleBuilderTest.NS,
				"StringCol");
		final Property nullableStringCol = model.createProperty(
				SimpleBuilderTest.NS, "NullableStringCol");
		final Property intCol = model.createProperty(SimpleBuilderTest.NS,
				"IntCol");
		final Property nullableIntCol = model.createProperty(
				SimpleBuilderTest.NS, "NullableIntCol");
		model.add(fooType, RDF.type, RDFS.Class);
		model.add(foo1, stringCol, "FooString");
		model.add(foo1, nullableStringCol, "FooNullableFooString");
		model.add(foo1, intCol, "5");
		model.add(foo1, nullableIntCol, "6");

		model.add(foo2, stringCol, "Foo2String");
		model.add(foo2, intCol, "5");

	}

	@Test
	public void buildRdfTableTest() throws SQLException {
		final Set<RdfTable> tables = builder.getTables(schema);
		final Map<String, Integer> counter = new HashMap<String, Integer>();
		final String[] columnNames = {
				"StringCol", "NullableStringCol", "IntCol", "NullableIntCol"
		};
		for (final RdfTable tbl : tables) {
			// schema.addTables(tbl);
			final ResultSet rs = tbl.getResultSet(catalogs, parser);
			int count = 0;
			while (rs.next()) {
				count++;
				for (int i = 1; i <= tbl.getColumnCount(); i++) {
					// just verify that no exception is thrown when reading
					// numeric columns
					rs.getString(i);
				}
				for (final String s : columnNames) {
					rs.getString(s);
					if (!rs.wasNull()) {
						Integer i = counter.get(s);
						i = i == null ? 1 : i + 1;
						counter.put(s, i);
					}
				}
			}
			Assert.assertEquals(2, count);
			for (final String s : columnNames) {
				final Integer i = counter.get(s);
				Assert.assertNotNull(i);
				final int b = s.startsWith("Nullable") ? 1 : 2;
				Assert.assertEquals(b, i.intValue());
			}
		}
	}

	@Test
	public void checkNullReturnValues() throws SQLException {

		final Set<RdfTable> tables = builder.getTables(schema);
		for (final RdfTable tbl : tables) {
			final ResultSet rs = tbl.getResultSet(catalogs, parser);
			boolean foundNull = false;
			while (rs.next() && !foundNull) {
				rs.getString("NullableIntCol");
				if (rs.wasNull()) {
					foundNull = true;
					Assert.assertNull(rs.getString("NullableIntCol"));
					Assert.assertFalse(rs.getBoolean("NullableIntCol"));
					Assert.assertEquals(0, rs.getByte("NullableIntCol"));
					Assert.assertEquals(0, rs.getShort("NullableIntCol"));
					Assert.assertEquals(0, rs.getInt("NullableIntCol"));
					Assert.assertEquals(0, rs.getLong("NullableIntCol"));
					Assert.assertEquals(0.0F, rs.getFloat("NullableIntCol"), 0);
					Assert.assertEquals(0.0D, rs.getDouble("NullableIntCol"), 0);
					Assert.assertNull(rs.getBigDecimal("NullableIntCol"));
					Assert.assertNull(rs.getBytes("NullableIntCol"));
					// not supported Assert.assertNull(
					// rs.getDate("NullableIntCol"));
					// not supported Assert.assertNull(
					// rs.getTime("NullableIntCol"));
					// not supported Assert.assertNull(
					// rs.getTimestamp("NullableIntCol"));
					// not supported Assert.assertNull(
					// rs.getAsciiStream("NullableIntCol"));
					// not supported Assert.assertNull(
					// rs.getUnicodeStream("NullableIntCol"));
					// not supported Assert.assertNull(
					// rs.getCharacterStream("NullableIntCol"));
					// not supported Assert.assertNull(
					// rs.getBinaryStream("NullableIntCol"));
				}
			}
			Assert.assertTrue( "Missing null for table "+tbl.getName().getFQName(),foundNull);

		}
	}

	@Before
	public void setup() {
		model = ModelFactory.createDefaultModel();
		mgr = RDFConnectionFactory.connect( DatasetFactory.create( model ));
		schemaModel = ModelFactory.createDefaultModel();
		sMgr = EntityManagerFactory.create( schemaModel );
		addModelData(model);
		catalog = new RdfCatalog.Builder().setLocalConnection(mgr)
				.setName("SimpleSparql").build(sMgr);
		catalogs = new HashMap<String, Catalog>();
		catalogs.put(catalog.getName().getShortName(), catalog);
		schema = new RdfSchema.Builder().setCatalog(catalog)
				.setName("builderTest").build();

		builder = new SimpleBuilder();
		parser = new SparqlParserImpl();
	}

}

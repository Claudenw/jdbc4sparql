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

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlSchema;
import org.xenei.jdbc4sparql.sparql.SparqlTable;

public class SimpleBuilderTest
{
	private static final String CAT_NS = "http://example.com/jdbc4sparql/meta/catalog#";
	private static final String NS = "http://example.com/jdbc4sparql#";
	private SparqlCatalog catalog;
	private Model model;
	private SchemaBuilder builder;

	private void addModelData()
	{
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
		model.write(System.out, "TURTLE");
	}

	@Test
	public void buildSparqlTableTest() throws SQLException
	{
		final SparqlSchema schema = new SparqlSchema(catalog,
				SimpleBuilderTest.NS, "builderTest");
		catalog.addSchema(schema);
		final Set<TableDef> tableDefs = builder.getTableDefs(catalog);
		final Map<String, Integer> counter = new HashMap<String, Integer>();
		final String[] columnNames = { "StringCol", "NullableStringCol",
				"IntCol", "NullableIntCol" };
		for (final TableDef td : tableDefs)
		{
			schema.addTableDef(td);
			final SparqlTable t = (SparqlTable) schema.getTable(td
					.getLocalName());
			final ResultSet rs = t.getResultSet();
			int count = 0;
			while (rs.next())
			{
				count++;
				for (int i = 1; i <= td.getColumnCount(); i++)
				{
					// just verify that no exception is thrown when reading
					// numeric columns
					rs.getString(i);
				}
				for (final String s : columnNames)
				{
					rs.getString(s);
					if (!rs.wasNull())
					{
						Integer i = counter.get(s);
						i = i == null ? 1 : i + 1;
						counter.put(s, i);
					}
				}
			}
			Assert.assertEquals(2, count);
			for (final String s : columnNames)
			{
				final Integer i = counter.get(s);
				Assert.assertNotNull(i);
				final int b = s.startsWith("Nullable") ? 1 : 2;
				Assert.assertEquals(b, i.intValue());
			}
		}
	}

	@Test
	public void checkNullReturnValues() throws SQLException
	{
		final SparqlSchema schema = new SparqlSchema(catalog,
				SimpleBuilderTest.NS, "builderTest");
		catalog.addSchema(schema);
		final Set<TableDef> tableDefs = builder.getTableDefs(catalog);
		for (final TableDef td : tableDefs)
		{
			schema.addTableDef(td);
			final SparqlTable t = (SparqlTable) schema.getTable(td
					.getLocalName());
			final ResultSet rs = t.getResultSet();
			boolean foundNull = false;
			while (rs.next() && !foundNull)
			{
				rs.getString("NullableIntCol");
				if (rs.wasNull())
				{
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
			Assert.assertTrue(foundNull);

		}
	}

	@Before
	public void setup()
	{
		model = ModelFactory.createDefaultModel();
		addModelData();
		catalog = new SparqlCatalog(SimpleBuilderTest.CAT_NS, model,
				"SimpleSparql");
		builder = new SimpleBuilder();
	}

}

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
package org.xenei.jdbc4sparql;

import org.apache.jena.riot.RDFLanguages;
import org.junit.Assert;
import org.junit.Test;

public class J4SUrlTest
{
	@Test
	public void testBuilderNoValueURL()
	{
		J4SUrl url;
		url = new J4SUrl("jdbc:j4s?builder:http://example.com/test.file");

		Assert.assertEquals("", url.getCatalog());
		Assert.assertEquals(
				"org.xenei.jdbc4sparql.sparql.builders.SimpleBuilder", url
						.getBuilder().getClass().getName());
		Assert.assertEquals("http://example.com/test.file", url.getEndpoint()
				.toString());

		url = new J4SUrl(
				"jdbc:j4s?builder=org.xenei.jdbc4sparql.sparql.builders.SimpleBuilder:http://example.com/test.file");
		Assert.assertEquals("", url.getCatalog());
		Assert.assertEquals(
				"org.xenei.jdbc4sparql.sparql.builders.SimpleBuilder", url
						.getBuilder().getClass().getName());
		Assert.assertEquals("http://example.com/test.file", url.getEndpoint()
				.toString());

	}

	@Test
	public void testBuilderURL()
	{
		J4SUrl url;
		url = new J4SUrl(
				"jdbc:j4s?builder=org.xenei.jdbc4sparql.sparql.builders.SimpleBuilder:http://example.com/test.file");

		Assert.assertEquals("", url.getCatalog());
		Assert.assertEquals(
				"org.xenei.jdbc4sparql.sparql.builders.SimpleBuilder", url
						.getBuilder().getClass().getName());
		Assert.assertEquals("http://example.com/test.file", url.getEndpoint()
				.toString());

		url = new J4SUrl(
				"jdbc:j4s?builder=org.xenei.jdbc4sparql.sparql.builders.SimpleBuilder:http://example.com/test.file");
		Assert.assertEquals("", url.getCatalog());
		Assert.assertEquals(
				"org.xenei.jdbc4sparql.sparql.builders.SimpleBuilder", url
						.getBuilder().getClass().getName());
		Assert.assertEquals("http://example.com/test.file", url.getEndpoint()
				.toString());

	}

	@Test
	public void testCatalogURL()
	{
		J4SUrl url;
		url = new J4SUrl("jdbc:j4s?catalog=foo:http://example.com/test.file");
		Assert.assertEquals("foo", url.getCatalog());
		Assert.assertNull(url.getBuilder());
		Assert.assertEquals("http://example.com/test.file", url.getEndpoint()
				.toString());

		url = new J4SUrl("jdbc:j4s?catalog=foo:file:///test.file");
		Assert.assertEquals("foo", url.getCatalog());
		Assert.assertNull(url.getBuilder());
		Assert.assertEquals("file:///test.file", url.getEndpoint().toString());

		url = new J4SUrl("jdbc:j4s?catalog=foo:ftp://example.com/test.file");
		Assert.assertEquals("foo", url.getCatalog());
		Assert.assertNull(url.getBuilder());
		Assert.assertEquals("ftp://example.com/test.file", url.getEndpoint()
				.toString());
	}

	@Test
	public void testSimpleURL()
	{
		J4SUrl url;
		url = new J4SUrl("jdbc:j4s:http://example.com/test.file");
		Assert.assertEquals("", url.getCatalog());
		Assert.assertNull(url.getBuilder());
		Assert.assertEquals("http://example.com/test.file", url.getEndpoint()
				.toString());

		url = new J4SUrl("jdbc:j4s:file:///test.file");
		Assert.assertEquals("", url.getCatalog());
		Assert.assertNull(url.getBuilder());
		Assert.assertEquals("file:///test.file", url.getEndpoint().toString());

		url = new J4SUrl("jdbc:j4s:ftp://example.com/test.file");
		Assert.assertEquals("", url.getCatalog());
		Assert.assertNull(url.getBuilder());
		Assert.assertEquals("ftp://example.com/test.file", url.getEndpoint()
				.toString());
	}

	@Test
	public void testTypeURL()
	{
		J4SUrl url;
		url = new J4SUrl("jdbc:j4s?type=sparql:http://example.com/test.file");
		Assert.assertEquals(J4SUrl.TYPE_SPARQL, url.getType());
		Assert.assertEquals("http://example.com/test.file", url.getEndpoint()
				.toString());

		url = new J4SUrl("jdbc:j4s?type=turtle:file:///test.file");
		Assert.assertEquals(RDFLanguages.TURTLE.getName(), url.getType());
		Assert.assertNull(url.getBuilder());
		Assert.assertEquals("file:///test.file", url.getEndpoint().toString());

		url = new J4SUrl("jdbc:j4s?type=rdf/xml:ftp://example.com/test.file");
		Assert.assertEquals(RDFLanguages.RDFXML.getName(), url.getType());
		Assert.assertNull(url.getBuilder());
		Assert.assertEquals("ftp://example.com/test.file", url.getEndpoint()
				.toString());

		url = new J4SUrl("jdbc:j4s:ftp://example.com/test.file");
		Assert.assertEquals(J4SUrl.TYPE_CONFIG, url.getType());
		Assert.assertNull(url.getBuilder());
		Assert.assertEquals("ftp://example.com/test.file", url.getEndpoint()
				.toString());

	}

}

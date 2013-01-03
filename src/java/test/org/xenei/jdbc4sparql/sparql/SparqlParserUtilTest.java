package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.graph.Node;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

public class SparqlParserUtilTest
{

	@Test
	public void parseNodeTest()
	{
		Node n;

		n = SparqlParser.Util.parseNode("<who>");
		Assert.assertTrue(n.isURI());
		Assert.assertEquals("who", n.getURI());

		n = SparqlParser.Util.parseNode("[]");
		Assert.assertTrue(n.isBlank());

		n = SparqlParser.Util.parseNode("$that");
		Assert.assertTrue(n.isVariable());
		Assert.assertEquals("that", n.getName());

		n = SparqlParser.Util.parseNode("?is");
		Assert.assertTrue(n.isVariable());
		Assert.assertEquals("is", n.getName());

		n = SparqlParser.Util.parseNode("'this'");
		Assert.assertTrue(n.isLiteral());
		Assert.assertEquals("this", n.getLiteralValue());

		n = SparqlParser.Util.parseNode("\"Another great one\"");
		Assert.assertTrue(n.isLiteral());
		Assert.assertEquals("Another great one", n.getLiteralValue());

		n = SparqlParser.Util.parseNode("\"Paul said: 'I am the egg man'\"");
		Assert.assertTrue(n.isLiteral());
		Assert.assertEquals("Paul said: 'I am the egg man'",
				n.getLiteralValue());

		n = SparqlParser.Util
				.parseNode("\"Paul said: 'I am the \"egg\" man'\"");
		Assert.assertTrue(n.isLiteral());
		Assert.assertEquals("Paul said: 'I am the \"egg\" man'",
				n.getLiteralValue());

		n = SparqlParser.Util.parseNode("5");
		Assert.assertTrue(n.isLiteral());
		Assert.assertEquals("5", n.getLiteralValue());

		n = SparqlParser.Util.parseNode("5^^xsd:Integer");
		Assert.assertTrue(n.isLiteral());
		Assert.assertEquals("5^^xsd:Integer", n.getLiteralValue());

	}

	@Test
	public void parseQuerySegmentTest()
	{
		List<String> parsed = SparqlParser.Util
				.parseQuerySegment("<who> ?is 'this'");
		Assert.assertEquals("<who>", parsed.get(0));
		Assert.assertEquals("?is", parsed.get(1));
		Assert.assertEquals("'this'", parsed.get(2));

		parsed = SparqlParser.Util
				.parseQuerySegment("\"Another great one\" [] $that");
		Assert.assertEquals("\"Another great one\"", parsed.get(0));
		Assert.assertEquals("[]", parsed.get(1));
		Assert.assertEquals("$that", parsed.get(2));

		parsed = SparqlParser.Util
				.parseQuerySegment("\"Paul said: 'I am the egg man'\" <lyrics> 5^^xsd:Integer");
		Assert.assertEquals("\"Paul said: 'I am the egg man'\"", parsed.get(0));
		Assert.assertEquals("<lyrics>", parsed.get(1));
		Assert.assertEquals("5^^xsd:Integer", parsed.get(2));

		parsed = SparqlParser.Util
				.parseQuerySegment("\"Paul said: 'I am the \"egg\" man'\" <lyrics> ?beatles");
		Assert.assertEquals("\"Paul said: 'I am the \"egg\" man'\"",
				parsed.get(0));
		Assert.assertEquals("<lyrics>", parsed.get(1));
		Assert.assertEquals("?beatles", parsed.get(2));

	}

}

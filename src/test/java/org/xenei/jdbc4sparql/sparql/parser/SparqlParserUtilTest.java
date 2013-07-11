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
package org.xenei.jdbc4sparql.sparql.parser;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.path.P_Link;
import com.hp.hpl.jena.sparql.path.Path;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class SparqlParserUtilTest
{
	Var vIs = Var.alloc("is");
	Var vWas = Var.alloc("was");
	Node nWho = NodeFactory.createURI("who");
	Node nWhat = NodeFactory.createURI("what");
	Path pWho = new P_Link(nWho);
	Path pWhat =new P_Link(nWhat);
	Node nThis = NodeFactory.createLiteral("this");
	Node nThat = NodeFactory.createLiteral("that");
	
	@Test
	public void parseSingleQuerySegmentTest() throws Exception
	{
		Element parsed = SparqlParser.Util.parse("{ ?is <who> 'that' }");

		Assert.assertTrue(parsed instanceof ElementGroup);
		List<Element> lst = ((ElementGroup) parsed).getElements();
		Assert.assertEquals(1, lst.size());
		Element e = lst.get(0);
		Assert.assertTrue( e instanceof ElementPathBlock);
		ElementPathBlock epb = (ElementPathBlock) e;
		List<TriplePath> l = epb.getPattern().getList();
		Assert.assertEquals(1, l.size());
		Assert.assertEquals( new TriplePath( vIs, pWho, nThat ), l.get(0));
	}
		@Test
		public void parseLinkedQuerySegmentTest() throws Exception
		{
			Element parsed = SparqlParser.Util.parse("{ ?is <who> 'this' ;"
				+ "	<what> 'that' }");
		Assert.assertTrue(parsed instanceof ElementGroup);
		List<Element> lst = ((ElementGroup) parsed).getElements();
		Element e = lst.get(0);
		Assert.assertTrue( e instanceof ElementPathBlock);
		ElementPathBlock epb = (ElementPathBlock) e;
		List<TriplePath> l = epb.getPattern().getList();
		Assert.assertEquals(2, l.size());
		Assert.assertEquals( new TriplePath( vIs, pWho, nThis ), l.get(0));
		Assert.assertEquals( new TriplePath( vIs, pWhat, nThat ), l.get(1));
	}
	
	@Test
	public void parseDisjointQuerySegmentTest() throws Exception
	{
		Element parsed = SparqlParser.Util.parse("{ ?is <who> 'this' ."
				+ "?was <what> 'that' }");
		Assert.assertTrue(parsed instanceof ElementGroup);
		List<Element> lst = ((ElementGroup) parsed).getElements();
		Assert.assertEquals(1, lst.size());
		Assert.assertTrue(parsed instanceof ElementGroup);
		lst = ((ElementGroup) parsed).getElements();
		Element e = lst.get(0);
		Assert.assertTrue( e instanceof ElementPathBlock);
		ElementPathBlock epb = (ElementPathBlock) e;
		List<TriplePath> l = epb.getPattern().getList();
		Assert.assertEquals(2, l.size());
		Assert.assertEquals( new TriplePath( vIs, pWho, nThis ), l.get(0));
		Assert.assertEquals( new TriplePath( vWas, pWhat, nThat ), l.get(1));
	}

}

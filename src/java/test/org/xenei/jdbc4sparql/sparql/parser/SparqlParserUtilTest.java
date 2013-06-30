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
import com.hp.hpl.jena.sparql.lang.sparql_11.ParseException;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;

import java.util.List;


import org.junit.Assert;
import org.junit.Test;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

public class SparqlParserUtilTest
{

	@Test
	public void parseQuerySegmentTest() throws Exception
	{
		Element parsed = SparqlParser.Util
				.parse( "{ ?is <who> 'this' }");
		
		Assert.assertTrue( parsed instanceof ElementGroup);
		List<Element> lst = ((ElementGroup)parsed).getElements();
		Assert.assertEquals( 1,  lst.size() );
		
		parsed = SparqlParser.Util
				.parse( "{ ?is <who> 'this' ;" +
						"	<what> 'that' }");
		Assert.assertTrue( parsed instanceof ElementGroup);
		lst = ((ElementGroup)parsed).getElements();
		Assert.assertEquals( 2,  lst.size() );

		parsed = SparqlParser.Util
				.parse( "{ ?is <who> 'this' ." +
						"?was <what> 'that' }");
		Assert.assertTrue( parsed instanceof ElementGroup);
		lst = ((ElementGroup)parsed).getElements();
		Assert.assertEquals( 2,  lst.size() );
		
	}

}

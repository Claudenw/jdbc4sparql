package org.xenei.jdbc4sparql.mock;

import org.xenei.jdbc4sparql.sparql.SparqlTableDef;

public class MockTableDef extends SparqlTableDef
{

	public MockTableDef(  String name )
	{
		super( MockCatalog.NS, name, "# table "+name+" query segment");
	}

}

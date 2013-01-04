package org.xenei.jdbc4sparql.mock;

import org.xenei.jdbc4sparql.meta.MetaSchema;
import org.xenei.jdbc4sparql.sparql.SparqlColumnDef;
import org.xenei.jdbc4sparql.sparql.SparqlTable;

public class MockColumn extends SparqlColumnDef
{

	public MockColumn( String name, int type)
	{
		super(MockCatalog.NS, name, type, "# column "+name+" query segment");
	}

	

}

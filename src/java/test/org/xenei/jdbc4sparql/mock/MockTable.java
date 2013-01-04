package org.xenei.jdbc4sparql.mock;

import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.impl.DataTable;
import org.xenei.jdbc4sparql.impl.TableDefImpl;
import org.xenei.jdbc4sparql.sparql.SparqlTable;
import org.xenei.jdbc4sparql.sparql.SparqlTableDef;

public class MockTable extends SparqlTable
{
	public MockTable( final MockSchema schema, MockTableDef tableDef )
	{
		super(schema, tableDef);
	}

	@Override
	public String getType()
	{
		return "MOCK TABLE";
	}
}

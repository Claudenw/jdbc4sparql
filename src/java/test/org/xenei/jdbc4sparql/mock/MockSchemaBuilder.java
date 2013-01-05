package org.xenei.jdbc4sparql.mock;

import java.util.Collections;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;
import org.xenei.jdbc4sparql.sparql.builders.SchemaBuilder;

public class MockSchemaBuilder implements SchemaBuilder
{

	public MockSchemaBuilder()
	{
		
	}

	@Override
	public Set<TableDef> getTableDefs( SparqlCatalog catalog )
	{
		return Collections.emptySet();
	}

}

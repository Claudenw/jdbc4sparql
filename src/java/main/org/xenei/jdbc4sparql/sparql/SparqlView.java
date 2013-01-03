package org.xenei.jdbc4sparql.sparql;

import java.util.UUID;

public class SparqlView extends SparqlTable
{
	public static final String NAME_SPACE = "http://org.xenei.jdbc4sparql/vocab#View";

	public SparqlView( final SparqlQueryBuilder builder )
	{
		super(builder.getCatalog().getViewSchema(), builder.getTableDef(
				SparqlView.NAME_SPACE, UUID.randomUUID().toString()), builder
				.build());
	}

}

package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import java.util.ArrayList;
import java.util.List;

import org.xenei.jdbc4sparql.impl.ColumnImpl;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

public class SparqlColumn extends ColumnImpl
{

	public SparqlColumn( final SparqlTable table,
			final SparqlColumnDef columnDef )
	{
		super(columnDef.getNamespace(), table, columnDef);
	}

	public List<Triple> getQuerySegments( final Node tableVar,
			final Node columnVar )
	{
		final List<Triple> retval = new ArrayList<Triple>();
		final String fqName = "<" + getFQName() + ">";
		for (final String segment : ((SparqlColumnDef) getColumnDef())
				.getQuerySegments())
		{
			if (! segment.trim().startsWith("#"))
			{
				final List<String> parts = SparqlParser.Util
						.parseQuerySegment(String.format(segment, tableVar,
								columnVar, fqName));
				if (parts.size() != 3)
				{
					throw new IllegalStateException(getFQName() + " query segment "
							+ segment + " does not parse into 3 components");
				}
				retval.add(new Triple(SparqlParser.Util.parseNode(parts.get(0)),
						SparqlParser.Util.parseNode(parts.get(1)),
						SparqlParser.Util.parseNode(parts.get(2))));
			}
		}
		return retval;
	}
}

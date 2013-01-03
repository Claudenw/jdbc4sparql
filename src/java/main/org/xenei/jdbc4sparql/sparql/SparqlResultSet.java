package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.RDFNode;

import java.sql.SQLException;

import org.xenei.jdbc4sparql.iface.TypeConverter;
import org.xenei.jdbc4sparql.impl.ListResultSet;

public class SparqlResultSet extends ListResultSet
{

	public SparqlResultSet( final SparqlTable table ) throws SQLException
	{
		super(table.getCatalog().executeQuery(table.getQuery()), table);
	}

	@Override
	protected Object readObject( final int columnOrdinal ) throws SQLException
	{
		checkPosition();
		checkColumn(columnOrdinal);
		final QuerySolution soln = (QuerySolution) getRowObject();
		final RDFNode node = soln.get(getTable().getColumn(columnOrdinal - 1)
				.getLabel());
		if (node == null)
		{
			return null;
		}
		if (node.isLiteral())
		{
			return TypeConverter.getJavaValue(node.asLiteral());
		}
		return node.toString();
	}
}

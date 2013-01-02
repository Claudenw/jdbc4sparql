package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.RDFNode;

import java.sql.SQLException;
import java.util.List;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.AbstractCollectionResultSet;
import org.xenei.jdbc4sparql.impl.AbstractResultSet;
import org.xenei.jdbc4sparql.impl.ListResultSet;

public class SparqlResultSet extends ListResultSet
{
	SparqlTable table;
	
	public SparqlResultSet(SparqlTable table) throws SQLException
	{
		super( table.getCatalog().executeQuery( table.getTableDef().getQuery()), table );
		this.table = table;
	}

	@Override
	protected Object readObject( int idx ) throws SQLException
	{
		checkPosition();
		checkColumn( idx );
		QuerySolution soln = (QuerySolution) getRowObject();
		RDFNode node = soln.get( getTable().getColumn(idx).getLabel());
		if (node.isLiteral())
		{
			// convert type here
			node.asLiteral().
		}
		return node.toString(); 
			
	}
}

	

}

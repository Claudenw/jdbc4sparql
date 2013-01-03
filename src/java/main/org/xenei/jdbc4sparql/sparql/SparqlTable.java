package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.impl.AbstractTable;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

public class SparqlTable extends AbstractTable
{
	public class ColumnIterator implements Iterator<SparqlColumn>
	{

		private final Iterator<? extends ColumnDef> iter;

		public ColumnIterator()
		{
			iter = getColumnDefs().iterator();
		}

		@Override
		public boolean hasNext()
		{
			return iter.hasNext();
		}

		@Override
		public SparqlColumn next()
		{
			return new SparqlColumn(SparqlTable.this,
					(SparqlColumnDef) iter.next());
		}

		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}

	}

	private Query query;

	protected SparqlTable( final SparqlSchema schema,
			final SparqlTableDef tableDef )
	{
		super(tableDef.getNamespace(), schema, tableDef);
	}

	protected SparqlTable( final SparqlSchema schema,
			final SparqlTableDef tableDef, final Query query )
	{
		this(schema, tableDef);
		this.query = query;
	}

	@Override
	public SparqlCatalog getCatalog()
	{
		return (SparqlCatalog) super.getCatalog();
	}

	@Override
	public Iterator<SparqlColumn> getColumns()
	{
		return new ColumnIterator();
	}

	public Query getQuery() throws SQLException
	{
		if (query == null)
		{
			final SparqlQueryBuilder builder = new SparqlQueryBuilder(
					getCatalog());
			builder.addTable(getSchema().getLocalName(), getLocalName());
			final Iterator<SparqlColumn> iter = getColumns();
			while (iter.hasNext())
			{
				final SparqlColumn col = iter.next();
				builder.addColumn(col);
				builder.addVar(col, col.getLocalName());
			}
			query = builder.build();
		}
		return query;
	}

	public List<Triple> getQuerySegments( final Node tableVar )
	{
		final List<Triple> retval = new ArrayList<Triple>();
		final String fqName = "<" + getFQName() + ">";
		for (final String segment : getTableDef().getQuerySegments())
		{
			final List<String> parts = SparqlParser.Util
					.parseQuerySegment(String.format(segment, tableVar, fqName));
			if (parts.size() != 3)
			{
				throw new IllegalStateException(getFQName() + " query segment "
						+ segment + " does not parse into 3 components");
			}
			retval.add(new Triple(SparqlParser.Util.parseNode(parts.get(0)),
					SparqlParser.Util.parseNode(parts.get(1)),
					SparqlParser.Util.parseNode(parts.get(2))));
		}
		return retval;
	}

	@Override
	public ResultSet getResultSet() throws SQLException
	{
		return new SparqlResultSet(this);
	}

	@Override
	public SparqlSchema getSchema()
	{
		return (SparqlSchema) super.getSchema();
	}

	@Override
	public SparqlTableDef getTableDef()
	{
		return (SparqlTableDef) super.getTableDef();
	}

	@Override
	public String getType()
	{
		return "SPARQL TABLE";
	}
}

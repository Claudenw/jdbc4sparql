package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.lang.sparql_11.ParseException;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumn;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

public class QueryTableInfo extends QueryItemInfo
{
	public static class Name extends QueryItemName
	{
		private Name( QueryItemName name )
		{
			super( name.getSchema(), name.getTable(), null);
		}
		
		private Name( final String schema, final String table )
		{
			super(schema, table, null);
		}
	}

	public static Name getNameInstance( final String alias )
	{
		if (alias == null)
		{
			throw new IllegalArgumentException("Alias must be provided");
		}
		final String[] parts = alias.split("\\" + NameUtils.DB_DOT);
		switch (parts.length)
		{
			case 2:
				return new Name(parts[0], parts[1]);
			case 1:
				return new Name(null, parts[0]);

			default:
				throw new IllegalArgumentException(String.format(
						"Column name must be 1 or 2 segments not %s as in %s",
						parts.length, alias));

		}
	}

	public static Name getNameInstance( QueryItemName name )
	{
		return new Name( name );
	}
	public static Name getNameInstance( final String schema,
			final String table )
	{
		return new Name(schema, table);
	}

	/**
	 * 
	 */
	private final QueryTableSet tableSet;
	private final RdfTable table;
	private final ElementGroup eg;

	private final boolean optional;

	// list of type filters to add at the end of the query
	private final Set<CheckTypeF> typeFilterList;

	private static Logger LOG = LoggerFactory.getLogger(QueryTableInfo.class);

	public QueryTableInfo( final QueryTableSet tableSet,
			final ElementGroup queryElementGroup, final RdfTable table,
			final String alias, final boolean optional )
	{
		super(QueryTableInfo.getNameInstance(alias));
		this.tableSet = tableSet;
		this.table = table;
		this.eg = new ElementGroup();
		this.optional = optional;
		this.typeFilterList = new HashSet<CheckTypeF>();

		// add the table var to the nodes.
		tableSet.addTable(this);
		LOG.debug( "adding required columns");
		final String eol = System.getProperty("line.separator");
		final StringBuilder queryFmt = new StringBuilder("{ ")
				.append(StringUtils.defaultString(table.getQuerySegmentFmt(),
						""));

		for (final Iterator<RdfColumn> colIter = table.getColumns(); colIter
				.hasNext();)
		{
			final RdfColumn column = colIter.next();
			if (!column.isOptional())
			{
				final Node colVar = addColumn(new QueryColumnInfo(tableSet,
						column, column.getSQLName()));

				final String fmt = column.getQuerySegmentFmt();
				if (StringUtils.isNotBlank(fmt))
				{
					queryFmt.append(String.format(fmt, getVar(), colVar))
							.append(eol);
				}
			}
		}
		queryFmt.append("}");
		final String queryStr = String.format(queryFmt.toString(), getVar());
		try
		{
			eg.addElement(SparqlParser.Util.parse(queryStr));
		}
		catch (final ParseException e)
		{
			throw new IllegalStateException(table.getFQName()
					+ " query segment " + queryStr, e);
		}
		catch (final QueryException e)
		{
			throw new IllegalStateException(table.getFQName()
					+ " query segment " + queryStr, e);
		}
		LOG.debug( "finished adding required columns");
		if (optional)
		{
			LOG.debug( "marking {} as optional.", this );
			queryElementGroup.addElement(new ElementOptional(eg));
		}
		else
		{
			LOG.debug( "marking {} as required.", this );
			queryElementGroup.addElement(eg);
		}
	}

	/**
	 * Add the column to the columns being returned from this table.
	 * 
	 * @param columnInfo
	 *            the column info for the column to return
	 * @return The columnVar node.
	 */
	public Node addColumn( final QueryColumnInfo columnInfo )
	{
		LOG.debug("Adding column: {} as {}", columnInfo, columnInfo.isOptional()?"optional":"required" );	
		
		if (columnInfo.isOptional())
		{
			
			eg.addElement(new ElementOptional(columnInfo.getColumn()
					.getQuerySegments(getVar(), columnInfo.getVar())));
		}

		typeFilterList.add(new CheckTypeF(columnInfo.getColumn(), Var
				.alloc(columnInfo.getVar())));

		return columnInfo.getVar();
	}

	public void addFilter( final Expr expr )
	{
		LOG.debug( "Adding filter: {}", expr );
		eg.addElementFilter(new ElementFilter(expr));
	}

	public void addOptional( final ElementTriplesBlock etb )
	{
		LOG.debug( "Adding optional: {}", etb );
		eg.addElement(new ElementOptional(etb));
	}

	public void addTypeFilters()
	{
		for (final CheckTypeF f : typeFilterList)
		{
			addFilter(f);
		}
	}

	/**
	 * Returns the column or null if not found
	 * 
	 * @param name
	 *            The name of the column to look for.
	 * @return
	 */
	public QueryColumnInfo getColumn( final String name )
	{
		QueryItemName qiName = QueryColumnInfo.getNameInstance(name);
		final RdfColumn col = (RdfColumn) table.getColumn(qiName.getCol());
		if (col != null)
		{
			qiName = QueryColumnInfo.getNameInstance(getName().getSchema(),
					getName().getTable(), col.getName());
			QueryColumnInfo qci = new QueryColumnInfo(tableSet, col, qiName.getDBName());
			addColumn( qci );
			return qci;
		}
		return null;
	}

	public Iterator<RdfColumn> getColumns()
	{
		return table.getColumns();
	}

	public String getSQLName()
	{
		return table.getSQLName();
	}

	public Set<CheckTypeF> getTypeFilterList()
	{
		return typeFilterList;
	}

	public boolean isOptional()
	{
		return optional;
	}

	@Override
	public String toString()
	{
		return String.format("QueryTableInfo[%s(%s)]", table.getSQLName(),
				getName());
	}
}
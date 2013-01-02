package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementBind;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;
import com.hp.hpl.jena.vocabulary.RDF;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.NamespacedObject;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.TableDefImpl;

public class SparqlQueryBuilder
{
	private static final String NOT_FOUND_IN_QUERY = "%s not found in SPARQL query";
	private static final String FOUND_IN_MULTIPLE_ = "%s was found in multiple %s";
	private static final String NOT_FOUND_IN_ANY_ = "%s was not found in any %s";

	private static final String COLUMN_NAME_FMT = "%s.%s.%s";
	private static final String TABLE_NAME_FMT = "%s.%s";

	private final Query query;
	private final SparqlCatalog catalog;
	private final Map<String, Table> tablesInQuery;
	private final Map<String, Node> nodesInQuery;
	private final Map<String, Column> columnsInQuery;

	public SparqlQueryBuilder( final SparqlCatalog catalog )
	{
		this.catalog = catalog;
		this.query = new Query();
		this.tablesInQuery = new HashMap<String, Table>();
		this.nodesInQuery = new HashMap<String, Node>();
		this.columnsInQuery = new HashMap<String, Column>();
		query.setQuerySelectType();
	}

	public void addBGP( final Node s, final Node p, final Node o )
	{
		final ElementGroup eg = getElementGroup();
		final Triple t = new Triple(s, p, o);
		for (final Element el : eg.getElements())
		{
			if (el instanceof ElementTriplesBlock)
			{
				if (((ElementTriplesBlock) el).getPattern().getList()
						.contains(t))
				{
					return;
				}
			}
		}
		eg.addTriplePattern(t);
	}

	public Node addColumn( final Column column ) throws SQLException
	{
		/*
		 * Add bgp triples
		 */

		Node tableVar;
		Node columnURI;

		columnURI = Node.createURI(column.getFQName());
		if (!columnsInQuery.containsKey(column.getDBName()))
		{
			columnsInQuery.put(column.getDBName(), column);
			tableVar = addTable(column.getSchema().getLocalName(), column
					.getTable().getLocalName());
			final Node columnVar = Node.createVariable(getDBName(column));
			nodesInQuery.put( column.getDBName(), columnVar);
			// ?tableVar columnURI ?columnVar
			if (column.getNullable() == DatabaseMetaData.columnNoNulls)
			{
				addBGP(tableVar, columnURI, columnVar);
			}
			else
			{
				final ElementGroup eg = getElementGroup();
				ElementTriplesBlock etb = new ElementTriplesBlock();
				etb.addTriple( new Triple( tableVar, columnURI, columnVar ));
				eg.addElement(new ElementOptional( etb ));
			}
			return columnVar;
		}
		else
		{
			return getDBNode(column.getDBName());
		}

	}

	public Node addColumn( String schemaName, String tableName, String columnName)
			throws SQLException
	{
		final Collection<Column> columns = findColumns(schemaName,
				tableName,columnName);

		if (columns.size() > 1)
		{
			throw new SQLException(String.format(
					SparqlQueryBuilder.FOUND_IN_MULTIPLE_,
					columnName, "tables"));
		}
		if (columns.isEmpty())
		{
			throw new SQLException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_ANY_,
					columnName, "table"));
		}
		return addColumn(columns.iterator().next());
	}

	public void addEquals( final Node left, final Node right )
	{
		final Var vLeft = Var.alloc(left);
		final Var vRight = Var.alloc(right);

		final Column cLeft = columnsInQuery.get(vLeft.getName());
		if (cLeft == null)
		{
			throw new IllegalStateException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_QUERY, vLeft));
		}
		final Column cRight = columnsInQuery.get(vRight.getName());
		if (cRight == null)
		{
			throw new IllegalStateException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_QUERY, vRight));
		}
		final Node tnLeft = getDBNode(cLeft.getTable().getDBName());
		final Node tnRight = getDBNode(cRight.getTable().getDBName());
		final Node anon = Node.createAnon(new AnonId());
		addBGP(tnLeft, Node.createURI(cLeft.getFQName()), anon);
		addBGP(tnRight, Node.createURI(cRight.getFQName()), anon);
	}

	public void addFilter( final Expr filter )
	{
		final ElementFilter el = new ElementFilter(filter);
		getElementGroup().addElementFilter(el);
	}

	public void addOrderBy( final Expr expr, final boolean ascending )
	{
		query.addOrderBy(expr, ascending ? Query.ORDER_ASCENDING
				: Query.ORDER_DESCENDING);
	}

	/**
	 * Returns the variable for the table.
	 * 
	 * @param schemaName
	 *            The schema name to find (null = any)
	 * @param tableName
	 *            The table name to find (null = any)
	 * @return
	 * @throws SQLException
	 */
	public Node addTable( final String schemaName, final String tableName )
			throws SQLException
	{
		final Collection<Table> tables = findTables(schemaName, tableName);
		if (tables.size() > 1)
		{
			throw new SQLException(
					String.format(SparqlQueryBuilder.FOUND_IN_MULTIPLE_,
							tableName, "schemas"));
		}
		if (tables.isEmpty())
		{
			throw new SQLException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_ANY_, tableName, "schema"));
		}
		final Table table = tables.iterator().next();
		if (!tablesInQuery.containsKey(table.getDBName()))
		{
			final Node tableVar = Node.createVariable(getDBName(table));
			tablesInQuery.put(table.getDBName(), table);
			nodesInQuery.put( table.getDBName(), tableVar );
			addBGP(tableVar, RDF.type.asNode(),
					Node.createURI(table.getFQName()));
			return tableVar;
		}
		else
		{
			return getDBNode(table.getDBName());
		}
	}

	public void addVar( final Column col, final String name )
			throws SQLException
	{
		final String realName = name == null ? getDBName(col) : name;
		final Var v = Var.alloc(realName);
		if (!query.getResultVars().contains(v.toString()))
		{
			if (realName.equalsIgnoreCase(name))
			{

				final Node n = addColumn(col);
				final ElementBind bind = new ElementBind(v, new ExprVar(n));
				getElementGroup().addElement(bind);
			}
			query.addResultVar(v);
		}

	}

	public void addVar( final Expr expr, final String name )
	{
		final NodeValue nv = expr.getConstant();
		if ((name != null) || (nv == null) || !nv.getNode().isVariable())
		{
			if (name == null)
			{
				query.addResultVar(expr);
			}
			else
			{
				query.addResultVar(name, expr);
			}
		}
		else
		{
			query.addResultVar(nv.getNode());
		}
	}

	public void addVars( final Iterator<? extends Column> cols )
			throws SQLException
	{
		while (cols.hasNext())
		{
			final Column col = cols.next();
			addVar(col, (getColCount(col) > 1 ? null : col.getLocalName()));
		}
	}

	public Query build()
	{
		return query;
	}

	private Collection<Column> findColumns( final String schemaName,
			final String tableName, final String columnName )
	{
		final List<Column> columns = new ArrayList<Column>();
		for (final Schema schema : catalog.findSchemas(schemaName))
		{
			for (final Table table : schema.findTables(tableName))
			{
				for (final Column column : table.findColumns(columnName))
				{
					columns.add(column);
				}
			}
		}
		return columns;
	}

	private Collection<Table> findTables( final String schemaName,
			final String tableName )
	{
		final List<Table> tables = new ArrayList<Table>();
		for (final Schema schema : catalog.findSchemas(schemaName))
		{
			for (final Table table : schema.findTables(tableName))
			{
				tables.add(table);
			}
		}
		return tables;
	}

	public SparqlCatalog getCatalog()
	{
		return catalog;
	}

	// get the number of columns in the selected tables with the same name
	private int getColCount( final Column col )
	{
		int retval = 0;
		for (final Column c : columnsInQuery.values())
		{
			if (c.getLocalName().equalsIgnoreCase(col.getLocalName()))
			{
				retval++;
			}
		}
		return retval;
	}

	private String getDBName( final Column col )
	{
		return String.format(SparqlQueryBuilder.COLUMN_NAME_FMT, col
				.getSchema().getLocalName(), col.getTable().getLocalName(), col
				.getLocalName());
	}

	private String getDBName( final Table table )
	{
		return String.format(SparqlQueryBuilder.TABLE_NAME_FMT, table
				.getSchema().getLocalName(), table.getLocalName());
	}

	private ElementGroup getElementGroup()
	{
		ElementGroup retval;
		final Element e = query.getQueryPattern();
		if (e == null)
		{
			retval = new ElementGroup();
			query.setQueryPattern(retval);
		}
		else if (e instanceof ElementGroup)
		{
			retval = (ElementGroup) e;
		}
		else
		{
			retval = new ElementGroup();
			retval.addElement(e);
		}
		return retval;
	}

	/*
	 * private String getTableVar( Table table )
	 * {
	 * return String.format( "TABLE_%s_%s_%s",
	 * table.getCatalog().getLocalName(),
	 * table.getSchema().getLocalName(), table.getLocalName() );
	 * }
	 */
	private Node getDBNode( String dbName )
	{
		final Node n = nodesInQuery.get(dbName);
		if (n == null)
		{
			throw new IllegalStateException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_QUERY, dbName ));
		}
		return n;
	}

	public SparqlTableDef getTableDef( final String name )
	{
		final SparqlTableDef tableDef = new SparqlTableDef(name, query);
		for (final Var var : query.getProjectVars())
		{
			final Column c = columnsInQuery.get(var);
			if (c == null)
			{
				throw new IllegalStateException(String.format(
						SparqlQueryBuilder.NOT_FOUND_IN_QUERY, var));
			}
			tableDef.add(columnsInQuery.get(var));
		}
		return tableDef;
	}

	public boolean isAllColumns()
	{
		return query.isQueryResultStar();
	}

	public void setAllColumns() throws SQLException
	{
		// query.setQueryResultStar(true);
		for (final Table t : tablesInQuery.values())
		{
			final Iterator<Column> iter = t.getColumns();
			while (iter.hasNext())
			{
				addColumn(iter.next());
			}
			addVars(t.getColumns());
		}
	}

	public void setDistinct()
	{
		query.setDistinct(true);
	}

	public void setLimit( final Long limit )
	{
		query.setLimit(limit);
	}

	public void setOffset( final Long offset )
	{
		query.setOffset(offset);
	}

	@Override
	public String toString()
	{
		return "QueryBuilder[" + query.toString() + "]";
	}
}

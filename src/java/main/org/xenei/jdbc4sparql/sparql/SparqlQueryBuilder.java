/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.expr.E_Exists;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.function.FunctionEnv;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementBind;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TypeConverter;
import org.xenei.jdbc4sparql.impl.AbstractResultSet;
import org.xenei.jdbc4sparql.impl.NameUtils;

/**
 * Creates a SparqlQuery while tracking naming changes between nomenclatures.
 */
public class SparqlQueryBuilder
{
	/**
	 * A local filter that removes any values that are null and not allowed to
	 * be null or that can not be converted
	 * to the expected column value type.
	 */
	private class CheckTypeF extends ExprFunction1
	{
		private final SparqlColumn column;

		public CheckTypeF( final SparqlColumn column, final Node columnVar )
		{
			super(new ExprVar(columnVar), "checkTypeF");
			this.column = column;
		}

		@Override
		public Expr copy( final Expr expr )
		{
			return new CheckTypeF(column, expr.asVar());
		}

		@Override
		public NodeValue eval( final NodeValue v )
		{
			return NodeValue.FALSE;
		}

		@Override
		protected NodeValue evalSpecial( final Binding binding,
				final FunctionEnv env )
		{
			final Var v = Var.alloc(column.getSPARQLName());
			final Node n = binding.get(v);
			if (n == null)
			{
				return column.getNullable() == ResultSetMetaData.columnNullable ? NodeValue.TRUE
						: NodeValue.FALSE;
			}
			final Class<?> resultingClass = TypeConverter.getJavaType(column
					.getType());
			Object columnObject;
			if (n.isLiteral())
			{
				columnObject = n.getLiteralValue();
			}
			else if (n.isURI())
			{
				columnObject = n.getURI();
			}
			else if (n.isBlank())
			{
				columnObject = n.getBlankNodeId();
			}
			else if (n.isVariable())
			{
				columnObject = n.getName();
			}
			else
			{
				columnObject = n.toString();
			}

			try
			{
				AbstractResultSet.extractData(columnObject, resultingClass);
				return NodeValue.TRUE;
			}
			catch (final SQLException e)
			{
				return NodeValue.FALSE;
			}

		}
	}

	// a set of error messages.
	private static final String NOT_FOUND_IN_QUERY = "%s not found in SPARQL query";
	private static final String FOUND_IN_MULTIPLE_ = "%s was found in multiple %s";

	private static final String NOT_FOUND_IN_ANY_ = "%s was not found in any %s";
	// the query we are building.
	private final Query query;
	// sparql catalog we are running against.
	private final SparqlCatalog catalog;
	// the list of tables in the query indexed by SQL name.
	private final Map<String, SparqlTable> tablesInQuery;
	// the list of nodes in the query indexed by SQL name.
	private final Map<String, Node> nodesInQuery;

	// the list of columns in the query indexed by SQL name.
	private final Map<String, SparqlColumn> columnsInQuery;

	/**
	 * Create a query builder for a catalog
	 * 
	 * @param catalog
	 *            The catalog to build the query for.
	 */
	public SparqlQueryBuilder( final SparqlCatalog catalog )
	{
		this.catalog = catalog;
		this.query = new Query();
		this.tablesInQuery = new HashMap<String, SparqlTable>();
		this.nodesInQuery = new HashMap<String, Node>();
		this.columnsInQuery = new HashMap<String, SparqlColumn>();
		query.setQuerySelectType();
	}

	/**
	 * Add the triple to the BGP for the query.
	 * This method handles adding the triple to the proper section of the
	 * query.
	 * 
	 * @param t
	 *            The Triple to add.
	 */
	public void addBGP( final Triple t )
	{
		final ElementGroup eg = getElementGroup();
		// final Triple t = new Triple(s, p, o);
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

	/**
	 * Add a column to the query.
	 * 
	 * Adds the column to the query tracking the name changes as necessary.
	 * If the column has not already been added to the query this method does
	 * the following
	 * <ul>
	 * <li>
	 * Adds the column table to the table list if not already added.</li>
	 * <li>
	 * Adds the column to the bgp using the column query segments.</li>
	 * <li>
	 * Makes the column values SPARQL optional if they are SQL nullable.</li>
	 * </ul>
	 * 
	 * @param column
	 *            The column to add
	 * @return The SPARQL based node name for the column.
	 * @throws SQLException
	 */
	public Node addColumn( final SparqlColumn column ) throws SQLException
	{
		// if the column is not in the query add it.
		if (!columnsInQuery.containsKey(column.getSQLName()))
		{
			// add the column to the query.
			columnsInQuery.put(column.getSQLName(), column);
			// make sure the table is in the query.
			final Node tableVar = addTable(column.getSchema().getLocalName(),
					column.getTable().getLocalName());
			// add the node to the query
			final Node columnVar = Node.createVariable(column.getSPARQLName());
			nodesInQuery.put(column.getSQLName(), columnVar);
			// if the column does not allow null add triple
			// and a filter
			if (column.getNullable() == DatabaseMetaData.columnNoNulls)
			{
				for (final Triple t : column.getQuerySegments(tableVar,
						columnVar))
				{
					addBGP(t);
					// we have to check not null
					final ElementTriplesBlock etb = new ElementTriplesBlock();
					etb.addTriple(t);
					addFilter(new E_Exists(etb));
				}

			}
			else
			{
				// column supports nulls so make it optional.
				final ElementGroup eg = getElementGroup();
				final ElementTriplesBlock etb = new ElementTriplesBlock();
				for (final Triple t : column.getQuerySegments(tableVar,
						columnVar))
				{
					etb.addTriple(t);
				}
				eg.addElement(new ElementOptional(etb));
			}
			// only return values of the proper type
			addFilter(new CheckTypeF(column, columnVar));
			return columnVar;
		}
		else
		{
			// already there, return node from the query.
			return getNodeBySQLName(column.getSQLName());
		}
	}

	/**
	 * Add teh column to the query.
	 * 
	 * @param schemaName
	 *            The schema name.
	 * @param tableName
	 *            The table name.
	 * @param columnName
	 *            The column name.
	 * @return The node for the column.
	 * @throws SQLException
	 */
	public Node addColumn( final String schemaName, final String tableName,
			final String columnName ) throws SQLException
	{
		// get all the columns that match the name patterns
		final Collection<SparqlColumn> columns = findColumns(schemaName,
				tableName, columnName);

		// if there are more than one see if only one is specified in the list
		// of tables.
		if (columns.size() > 1)
		{
			if (tableName == null)
			{
				// wild table so look for table names currently in the query.
				// get the set of table names.
				final Set<String> tblNames = WrappedIterator
						.create(tablesInQuery.values().iterator())
						.mapWith(new Map1<SparqlTable, String>() {

							@Override
							public String map1( final SparqlTable o )
							{
								return o.getSQLName();
							}
						}).toSet();
				SparqlColumn col = null;
				SparqlColumn thisCol = null;
				final Iterator<SparqlColumn> iter = columns.iterator();
				while (iter.hasNext())
				{
					thisCol = iter.next();
					if (tblNames.contains(thisCol.getTable().getSQLName()))
					{
						if (col != null)
						{
							throw new SQLException(String.format(
									SparqlQueryBuilder.FOUND_IN_MULTIPLE_,
									columnName, "tables"));
						}
						else
						{
							col = thisCol;
						}
					}
				}
				if (col != null)
				{
					return addColumn(col);
				}

			}
			throw new SQLException(
					String.format(SparqlQueryBuilder.FOUND_IN_MULTIPLE_,
							columnName, "tables"));
		}
		if (columns.isEmpty())
		{
			throw new SQLException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_ANY_, columnName, "table"));
		}
		return addColumn(columns.iterator().next());
	}

	/**
	 * Add a filter to the query.
	 * 
	 * @param filter
	 *            The filter to add.
	 */
	public void addFilter( final Expr filter )
	{
		final ElementFilter el = new ElementFilter(filter);
		getElementGroup().addElementFilter(el);
	}

	/**
	 * Add an order by to the query.
	 * 
	 * @param expr
	 *            The expression to order by.
	 * @param ascending
	 *            true of order should be ascending, false = descending.
	 */
	public void addOrderBy( final Expr expr, final boolean ascending )
	{
		query.addOrderBy(expr, ascending ? Query.ORDER_ASCENDING
				: Query.ORDER_DESCENDING);
	}

	/**
	 * Add a table to the query.
	 * 
	 * @param schemaName
	 *            The schema name
	 * @param tableName
	 *            The table name
	 * @return The node that represents the table.
	 * @throws SQLException
	 *             if the table is in multiple schemas or not found.
	 */
	public Node addTable( final String schemaName, final String tableName )
			throws SQLException
	{
		final Collection<SparqlTable> tables = findTables(schemaName, tableName);

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
		final SparqlTable table = tables.iterator().next();
		// make sure the table is in the query.
		if (!tablesInQuery.containsKey(table.getSQLName()))
		{
			tablesInQuery.put(table.getSQLName(), table);
			// add the table var to the nodes.
			final Node tableVar = Node.createVariable(table.getSPARQLName());
			nodesInQuery.put(table.getSQLName(), tableVar);
			// add the bgp to the query
			for (final Triple t : table.getQuerySegments(tableVar))
			{
				addBGP(t);
			}
			return tableVar;
		}
		else
		{
			// look up the table node by sql name.
			return getNodeBySQLName(table.getSQLName());
		}
	}

	/**
	 * Adds the the expression as a variable to the query.
	 * As a variable the result will be returned from the query.
	 * 
	 * @param expr
	 *            The expression that defines the variable
	 * @param name
	 *            the alias for the expression, if null no alias is used.
	 */
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

	/**
	 * Adds the the column as a variable to the query.
	 * As a variable the result will be returned from the query.
	 * 
	 * @param col
	 *            Adds a column as a variable.
	 * @param alias
	 *            the alias for the expression, if null no alias is used.
	 * @throws SQLException
	 */
	public void addVar( final SparqlColumn col, final String alias )
			throws SQLException
	{
		// figure out what name we are going to use.
		final String sparqlName = StringUtils.defaultString(alias,
				col.getSPARQLName());

		// allocate the var for the real name.
		final Var v = Var.alloc(sparqlName);
		// if we have not seen this var before register all the info.
		if (!query.getResultVars().contains(v.toString()))
		{
			// if an alias is being used then do special registration
			if (sparqlName.equalsIgnoreCase(alias))
			{
				final Node n = addColumn(col);
				final ElementBind bind = new ElementBind(v, new ExprVar(n));
				getElementGroup().addElement(bind);
				// add the alias for the column to the columns list.
				columnsInQuery.put(v.getName(), col);
			}
			query.addResultVar(v);
		}
	}

	/**
	 * Adds the the columns as a variables to the query.
	 * As variables the results will be returned from the query.
	 * If there is already a column with the same name in the vars then
	 * the column's full SPARQL DB name will be used as the
	 * alias. If there are multiple columns then no alias is assumed.
	 * 
	 * @param cols
	 *            Columns to add to the query as variables.
	 * @throws SQLException
	 */
	public void addVars( final Iterator<SparqlColumn> cols )
			throws SQLException
	{
		while (cols.hasNext())
		{
			final SparqlColumn col = cols.next();
			addVar(col, getColCount(col) > 1 ? null : col.getLocalName());
		}
	}

	/**
	 * Get the SPARQL query.
	 * 
	 * @return The constructed SPARQL query.
	 */
	public Query build()
	{
		return query;
	}

	/**
	 * Find all the columns for the given schema, table, and column patterns
	 * 
	 * @param schemaNamePattern
	 *            The schema name pattern. null = no restriction.
	 * @param tableNamePattern
	 *            The table name pattern. null = no restriction.
	 * @param columnNamePattern
	 *            The column name pattern. null = no restriction.
	 * @return
	 */
	private Collection<SparqlColumn> findColumns(
			final String schemaNamePattern, final String tableNamePattern,
			final String columnNamePattern )
	{
		final List<SparqlColumn> columns = new ArrayList<SparqlColumn>();
		for (final Schema schema : catalog.findSchemas(schemaNamePattern))
		{
			for (final Table table : schema.findTables(tableNamePattern))
			{
				for (final Column column : table.findColumns(columnNamePattern))
				{
					if (column instanceof SparqlColumn)
					{
						columns.add((SparqlColumn) column);
					}
				}
			}
		}
		return columns;
	}

	private Collection<SparqlTable> findTables( final String schemaName,
			final String tableName )
	{
		final List<SparqlTable> tables = new ArrayList<SparqlTable>();
		for (final Schema schema : catalog.findSchemas(schemaName))
		{
			for (final Table table : schema.findTables(tableName))
			{
				if (table instanceof SparqlTable)
				{
					tables.add((SparqlTable) table);
				}
			}
		}
		return tables;
	}

	/**
	 * Get the catalog this builder is working with.
	 * 
	 * @return a Catalog.
	 */
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

	/**
	 * Get a column from the query.
	 * 
	 * None of the parameters may be null.
	 * 
	 * @param schemaName
	 *            The schema name
	 * @param tableName
	 *            The table name
	 * @param columnName
	 *            The column name
	 * @return The SparqlColumn or null if no column is defined in the query.
	 */
	public SparqlColumn getColumn( final String schemaName,
			final String tableName, final String columnName )
	{
		if (schemaName == null)
		{
			throw new IllegalArgumentException("Schema name may not be null");
		}
		if (tableName == null)
		{
			throw new IllegalArgumentException("Table name may not be null");
		}
		if (columnName == null)
		{
			throw new IllegalArgumentException("Column name may not be null");
		}
		final Iterator<SparqlColumn> iter = findColumns(schemaName, tableName,
				columnName).iterator();
		return (iter.hasNext()) ? iter.next() : null;
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
	private Node getNodeBySQLName( final String dbName )
	{
		final Node n = nodesInQuery.get(dbName);
		if (n == null)
		{
			throw new IllegalStateException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_QUERY, dbName));
		}
		return n;
	}

	/**
	 * For a given result column get the column name
	 * 
	 * @param idx
	 * @return
	 */
	public String getSolutionName( final int idx )
	{
		return query.getProjectVars().get(idx).getName();
	}

	/**
	 * Get the table definition for the specified namespace and name.
	 * The Tabledef will not have a query segment.
	 * 
	 * @param namespace
	 *            The namespace for the table definition.
	 * @param localName
	 *            The name for the table definition.
	 * @return The table definition.
	 */
	public SparqlTableDef getTableDef( final String namespace,
			final String localName )
	{
		final SparqlTableDef tableDef = new SparqlTableDef(namespace,
				localName, "");
		for (final Var var : query.getProjectVars())
		{
			final String varColName = var.getName().replace(
					NameUtils.SPARQL_DOT, ".");
			final Column c = columnsInQuery.get(varColName);
			if (c == null)
			{
				throw new IllegalStateException(String.format(
						SparqlQueryBuilder.NOT_FOUND_IN_QUERY, var));
			}
			tableDef.add(columnsInQuery.get(varColName));
		}
		return tableDef;
	}

	/**
	 * @return returns true if the query is going to return all the columns from
	 *         all the tables.
	 */
	public boolean isAllColumns()
	{
		return query.isQueryResultStar();
	}

	/**
	 * Sets all the columns for all the tables currently defined.
	 * This method should be called after all tables have been added to the
	 * querybuilder.
	 * 
	 * @throws SQLException
	 */
	public void setAllColumns() throws SQLException
	{
		// query.setQueryResultStar(true);
		for (final SparqlTable t : tablesInQuery.values())
		{
			final Iterator<SparqlColumn> iter = t.getColumns();
			while (iter.hasNext())
			{
				addColumn(iter.next());
			}
			addVars(t.getColumns());
		}
	}

	/**
	 * Sets the distinct flag for the SPARQL query.
	 */
	public void setDistinct()
	{
		query.setDistinct(true);
	}

	/**
	 * Sets the limit for the SPARQL query.
	 * 
	 * @param limit
	 *            The number of records to return
	 */
	public void setLimit( final Long limit )
	{
		query.setLimit(limit);
	}

	/**
	 * Set the offset for the SPARQL query.
	 * 
	 * @param offset
	 *            The number of rows to skip over.
	 */
	public void setOffset( final Long offset )
	{
		query.setOffset(offset);
	}

	/**
	 * Return the SPARQL query as the string value for the builder.
	 */
	@Override
	public String toString()
	{
		return "QueryBuilder[" + query.toString() + "]";
	}
}

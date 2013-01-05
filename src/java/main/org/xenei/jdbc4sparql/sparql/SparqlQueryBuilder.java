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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

/**
 * Creates a SparqlQuery while tracking naming changes between nomenclatures.
 */
public class SparqlQueryBuilder
{
	private static final String NOT_FOUND_IN_QUERY = "%s not found in SPARQL query";
	private static final String FOUND_IN_MULTIPLE_ = "%s was found in multiple %s";
	private static final String NOT_FOUND_IN_ANY_ = "%s was not found in any %s";

	private static final String COLUMN_NAME_FMT = "%s\u00B7%s\u00B7%s";
	private static final String TABLE_NAME_FMT = "%s\u00B7%s";

	private final Query query;
	private final SparqlCatalog catalog;
	private final Map<String, SparqlTable> tablesInQuery;
	private final Map<String, Node> nodesInQuery;
	private final Map<String, SparqlColumn> columnsInQuery;

	/**
	 * Create a query builder for a catalog
	 * @param catalog The catalog to build the query for.
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

	// public void addBGP( final Node s, final Node p, final Node o )
	/**
	 * Add the triple to the BGP for the query.
	 * This method handles adding the triple to the proper section of the 
	 * query.
	 * @param t The Triple to add.
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
	 * Adds the column table to the table list if not already added.
	 * </li><li>
	 * Adds the column to the bgp using the column query segments.
	 * </li><li>
	 * Makes the column values SPARQL optional if they are SQL nullable.
	 * </li>
	 * </ul>
	 * 
	 * @param column The column to add
	 * @return The SPARQL based node name for the column.
	 * @throws SQLException
	 */
	public Node addColumn( final SparqlColumn column ) throws SQLException
	{
		Node tableVar;

		if (!columnsInQuery.containsKey(column.getDBName()))
		{
			columnsInQuery.put(column.getDBName(), column);
			tableVar = addTable(column.getSchema().getLocalName(), column
					.getTable().getLocalName());
			final Node columnVar = Node.createVariable(getDBName(column));
			nodesInQuery.put(column.getDBName(), columnVar);
			// ?tableVar columnURI ?columnVar
			if (column.getNullable() == DatabaseMetaData.columnNoNulls)
			{
				for (final Triple t : column.getQuerySegments(tableVar,
						columnVar))
				{
					addBGP(t);
				}
			}
			else
			{
				final ElementGroup eg = getElementGroup();
				final ElementTriplesBlock etb = new ElementTriplesBlock();
				for (final Triple t : column.getQuerySegments(tableVar,
						columnVar))
				{
					etb.addTriple(t);
				}
				eg.addElement(new ElementOptional(etb));
			}
			return columnVar;
		}
		else
		{
			return getDBNode(column.getDBName());
		}

	}

	public Node addColumn( final String schemaName, final String tableName,
			final String columnName ) throws SQLException
	{
		final Collection<SparqlColumn> columns = findColumns(schemaName,
				tableName, columnName);

		if (columns.size() > 1)
		{
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
		addBGP(new Triple(tnLeft, Node.createURI(cLeft.getFQName()), anon));
		addBGP(new Triple(tnRight, Node.createURI(cRight.getFQName()), anon));
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
		if (!tablesInQuery.containsKey(table.getDBName()))
		{
			final Node tableVar = Node.createVariable(getDBName(table));
			tablesInQuery.put(table.getDBName(), table);
			nodesInQuery.put(table.getDBName(), tableVar);
			for (final Triple t : table.getQuerySegments(tableVar))
			{
				addBGP(t);
			}
			return tableVar;
		}
		else
		{
			return getDBNode(table.getDBName());
		}
	}

	/**
	 * Adds the the expression as a variable to the query.
	 * As a variable the result will be returned from the query.  
	 * @param expr The expression that defines the variable
	 * @param name the alias for the expression, if null no alias is used.
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
	 * As a variable the result will be returned from the query.  	 * 
	 * @param col Adds a column as a variable.
	 * @param alias the alias for the expression, if null no alias is used.
	 * @throws SQLException
	 */
	public void addVar( final SparqlColumn col, final String alias )
			throws SQLException
	{
		// figure out what name we are going to use.
		String realName = alias;
		if (realName != null)
		{
			// var name must not have a dot (.) character
			realName.replace( ".", SparqlParser.SPARQL_DOT );
		}
		else
		{
			realName = getDBName(col);
		}

		// allocate the var for the real name.
		final Var v = Var.alloc(realName);
		// if we have not seen this var before register all the info.
		if (!query.getResultVars().contains(v.toString()))
		{
			// if an alias is being used then do special registration
			if (realName.equalsIgnoreCase(alias))
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
	 * alias.  If there are multiple columns then no alias is assumed.  
	 * @param cols Columns to add to the query as variables.
	 * @throws SQLException
	 */
	public void addVars( final Iterator<SparqlColumn> cols )
			throws SQLException
	{
		while (cols.hasNext())
		{
			final SparqlColumn col = cols.next();
			addVar(col, getColCount(col)>1?null: col.getLocalName());
		}
	}

	/**
	 * Get the SPARQL query.
	 * @return The constructed SPARQL query.
	 */
	public Query build()
	{
		return query;
	}

	/**
	 * Find all the columns for the given schema, table, and column patterns
	 * @param schemaNamePattern The schema name pattern. null = no restriction.
	 * @param tableNamePattern The table name pattern. null = no restriction.
	 * @param columnNamePattern The column name pattern. null = no restriction.
	 * @return
	 */
	private Collection<SparqlColumn> findColumns( final String schemaNamePattern,
			final String tableNamePattern, final String columnNamePattern )
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

	/*
	 * private String getTableVar( Table table )
	 * {
	 * return String.format( "TABLE_%s_%s_%s",
	 * table.getCatalog().getLocalName(),
	 * table.getSchema().getLocalName(), table.getLocalName() );
	 * }
	 */
	private Node getDBNode( final String dbName )
	{
		final Node n = nodesInQuery.get(dbName);
		if (n == null)
		{
			throw new IllegalStateException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_QUERY, dbName));
		}
		return n;
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

	/**
	 * Get the table definition for the specified namespace and name.
	 * The Tabledef will not have a query segment.
	 * @param namespace The namespace for the table definition.
	 * @param localName The name for the table definition.
	 * @return The table definition.
	 */
	public SparqlTableDef getTableDef( final String namespace, final String localName )
	{
		final SparqlTableDef tableDef = new SparqlTableDef(namespace, localName, "");
		for (final Var var : query.getProjectVars())
		{
			String varColName = var.getName().replace( SparqlParser.SPARQL_DOT, ".");
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
	 * For a given result column get the column name 
	 * @param idx
	 * @return 
	 */
	public String getSolutionName( int idx )
	{
		return query.getProjectVars().get(idx).getName();
	}

	/**
	 * @return returns true if the query is going to return all the columns from all the tables.
	 */
	public boolean isAllColumns()
	{
		return query.isQueryResultStar();
	}

	/**
	 * Sets all the columns for all the tables currently defined.
	 * This method should be called after all tables have been added to the querybuilder.
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
	 * @param limit The number of records to return
	 */
	public void setLimit( final Long limit )
	{
		query.setLimit(limit);
	}

	/**
	 * Set the offset for the SPARQL query.
	 * @param offset The number of rows to skip over.
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

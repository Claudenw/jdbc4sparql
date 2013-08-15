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
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.core.VarExprList;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementBind;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementService;
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumn;
import org.xenei.jdbc4sparql.impl.rdf.RdfKey;
import org.xenei.jdbc4sparql.impl.rdf.RdfKeySegment;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef;

/**
 * Creates a SparqlQuery while tracking naming changes between nomenclatures.
 */
public class SparqlQueryBuilder
{
	// a set of error messages.
	static final String NOT_FOUND_IN_QUERY = "%s not found in SPARQL query";

	static final String FOUND_IN_MULTIPLE_ = "%s was found in multiple %s";

	static final String NOT_FOUND_IN_ANY_ = "%s was not found in any %s";

	private static void addFilter( final Query query, final Expr filter )
	{
		final ElementFilter el = new ElementFilter(filter);
		SparqlQueryBuilder.getElementGroup(query).addElementFilter(el);
	}

	private static ElementGroup getElementGroup( final Query query )
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

	private final QueryInfoSet infoSet;

	// the query we are building.
	private Query query;

	// query was built flag;
	private boolean isBuilt;

	// sparql catalog we are running against.
	private final RdfCatalog catalog;

	// the list of columns not to be included in the "all columns" result.
	// perhaps this should store column so that tables may be checked in case
	// tables are added to the query later. But I don't think so.
	private final List<String> columnsInUsing;

	// columns indexed by var.
	private final List<Column> columnsInResult;

	private static Logger LOG = LoggerFactory
			.getLogger(SparqlQueryBuilder.class);

	/**
	 * Create a query builder for a catalog
	 * 
	 * @param catalog
	 *            The catalog to build the query for.
	 * @throws IllegalArgumentException
	 *             if catalog is null.
	 */
	public SparqlQueryBuilder( final RdfCatalog catalog )
	{
		if (catalog == null)
		{
			throw new IllegalArgumentException("Catalog may not be null");
		}
		this.catalog = catalog;
		this.query = new Query();
		this.isBuilt = false;
		this.infoSet = new QueryInfoSet();
		this.columnsInUsing = new ArrayList<String>();
		this.columnsInResult = new ArrayList<Column>();
		query.setQuerySelectType();
	}

	public SparqlQueryBuilder( final RdfTable table )
	{
		this(table.getCatalog());
		addTable(table);
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
		LOG.debug( "addBGP: {}", t );
		checkBuilt();
		final ElementGroup eg = getElementGroup();
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
	public Node addColumn( final RdfColumn column ) throws SQLException
	{
		if (LOG.isDebugEnabled()) {
			LOG.debug("(addColumn-1-arg) looking for Column {}", column.getSQLName());
		}
		checkBuilt();
		if (!infoSet.containsColumn(column.getSQLName()))
		{
			if (LOG.isDebugEnabled()) {
				LOG.debug("adding Column {}", column.getSQLName());
			}
			final String alias = column.getTable().getSQLName();
			QueryTableInfo sti = infoSet.getTable(alias);
			if (sti == null)
			{
				sti = new QueryTableInfo(infoSet, getElementGroup(),
						column.getTable(), alias, false);
			}
			return sti.addColumn(new QueryColumnInfo(infoSet, column, column
					.getSQLName()));
		}
		else
		{
			// already there, return node from the query.
			return infoSet.getColumnByName(column.getSQLName()).getVar();
		}
	}

	/**
	 * Add the column to the query.
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
		if (LOG.isDebugEnabled()) {
			LOG.debug("(addColumn-3-args) adding Column {}", NameUtils.getDBName(schemaName, tableName, columnName));
		}
		checkBuilt();
		final QueryColumnInfo columnInfo = infoSet.findColumn(schemaName,
				tableName, columnName);
		return columnInfo.getVar();
		// // get all the columns that match the name patterns
		// final Collection<RdfColumn> columns = findColumns(schemaName,
		// tableName, columnName);
		//
		// // if there are more than one see if only one is specified in the
		// list
		// // of tables.
		// if (columns.size() > 1)
		// {
		// if (tableName == null)
		// {
		// // wild table so look for table names currently in the query.
		// // get the set of table names.
		// final Set<String> tblNames = infoSet.getTableNames();
		// RdfColumn col = null;
		// RdfColumn thisCol = null;
		// final Iterator<RdfColumn> iter = columns.iterator();
		// while (iter.hasNext())
		// {
		// thisCol = iter.next();
		// if (tblNames.contains(thisCol.getTable().getSQLName()))
		// {
		// if (col != null)
		// {
		// throw new SQLException(String.format(
		// SparqlQueryBuilder.FOUND_IN_MULTIPLE_,
		// columnName, "tables"));
		// }
		// else
		// {
		// col = thisCol;
		// }
		// }
		// }
		// if (col != null)
		// {
		// return addColumn(col);
		// }
		//
		// }
		// throw new SQLException(
		// String.format(SparqlQueryBuilder.FOUND_IN_MULTIPLE_,
		// columnName, "tables"));
		// }
		// if (columns.isEmpty())
		// {
		// throw new SQLException(String.format(
		// SparqlQueryBuilder.NOT_FOUND_IN_ANY_, columnName, "table"));
		// }
		// return addColumn(columns.iterator().next());
	}

	/**
	 * Add a filter to the query.
	 * 
	 * @param filter
	 *            The filter to add.
	 */
	public void addFilter( final Expr filter )
	{
		SparqlQueryBuilder.LOG.debug("adding Filter: {}", filter);
		checkBuilt();
		SparqlQueryBuilder.addFilter(query, filter);
	}

	/**
	 * Add an optional table to the query.
	 * 
	 * @param schemaName
	 *            The schema name
	 * @param tableName
	 *            The table name
	 * @return The node that represents the table.
	 * @throws SQLException
	 *             if the table is in multiple schemas or not found.
	 */
	public Node addOptionalTable( final String schemaName,
			final String tableName ) throws SQLException
	{
		if (LOG.isDebugEnabled()) {
			LOG.debug( "Adding optional table {}", NameUtils.getDBName(schemaName, tableName, null));
		}
		checkBuilt();
		return addTable(schemaName, tableName, true);
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
		LOG.debug( "Adding order by {} {}", expr, ascending?"Ascending":"Descending");
		checkBuilt();
		query.addOrderBy(expr, ascending ? Query.ORDER_ASCENDING
				: Query.ORDER_DESCENDING);
	}

	public Node addTable( final RdfTable table )
	{
		return addTable(table, null, false);
	}

	public Node addTable( final RdfTable table, final boolean optional )
	{
		return addTable(table, null, optional);
	}

	public Node addTable( final RdfTable table, final String tableAlias,
			final boolean optional )
	{
		final String tblName = StringUtils.defaultString(tableAlias,
				table.getSQLName());
		if (LOG.isDebugEnabled())
		{
			LOG.debug( "Adding Rdftable {} as {}", table.getSQLName(), tblName );
		}
		
		// make sure the table is in the query.
		QueryTableInfo sti = infoSet.getTable(tblName);
		if (sti == null)
		{
			sti = new QueryTableInfo(infoSet, getElementGroup(), table,
					tblName, optional);
		}
		return sti.getVar();
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
		return addTable(schemaName, tableName, false);
	}

	public Node addTable( final String schemaName, final String tableName,
			final boolean optional ) throws SQLException
	{
		return addTable(schemaName, tableName, null, optional);
	}

	public Node addTable( final String schemaName, final String tableName,
			final String tableAlias, final boolean optional )
			throws SQLException
	{
		if (LOG.isDebugEnabled())
		{
			LOG.debug( "Adding table {} alias {}", NameUtils.getDBName(schemaName, tableName, null), tableAlias );
		}
		checkBuilt();
		final Collection<RdfTable> tables = findTables(schemaName, tableName);

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
		return addTable(tables.iterator().next(), tableAlias, optional);
	}

	public void addUsing( final String columnName )
	{
		LOG.debug( "adding Using {}", columnName );
		final Set<String> tableAliases = infoSet.getTableAliases();
		if (tableAliases.size() < 2)
		{
			throw new IllegalArgumentException(
					"There must be at least 2 tables in the query");
		}
		final Iterator<String> iter = tableAliases.iterator();
		final QueryTableInfo rti = infoSet.getTable(iter.next());
		final QueryColumnInfo baseColumn = rti.getColumn(columnName);
		if (baseColumn == null)
		{
			throw new IllegalArgumentException(String.format(
					"column %s not found in %s", columnName, rti.getSQLName()));
		}
		columnsInUsing.add(columnName);
		try
		{
			final ExprVar left = new ExprVar(addColumn(baseColumn.getColumn()));
			while (iter.hasNext())
			{
				final QueryTableInfo rti2 = infoSet.getTable(iter.next());
				final QueryColumnInfo col = rti2.getColumn(columnName);
				if (col == null)
				{
					throw new IllegalArgumentException(
							String.format("column %s not found in %s", col,
									rti2.getSQLName()));
				}

				addFilter(new E_Equals(left, new ExprVar(
						addColumn(col.getColumn()))));
			}
		}
		catch (final SQLException e)
		{
			throw new IllegalArgumentException(e.getMessage(), e);
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
		LOG.debug( "Adding Var {} as {}", expr, name );
		Column column;
		checkBuilt();
		final NodeValue nv = expr.getConstant();
		if ((name != null) || (nv == null) || !nv.getNode().isVariable())
		{
			final String s = StringUtils.defaultString(expr.getVarName());
			if (s.equals(StringUtils.defaultIfBlank(name, s)))
			{
				column = new ExprColumn(s, expr);
				query.addResultVar(s);
			}
			else
			{
				column = new ExprColumn(name, expr);
				query.addResultVar(name, expr);
			}
		}
		else
		{
			column = new ExprColumn(nv.getVarName(), expr);
			query.addResultVar(nv.getNode());
		}
		query.getResultVars();
		columnsInResult.add(column);
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
	public void addVar( final RdfColumn col, final String alias )
			throws SQLException
	{
		checkBuilt();

		// figure out what name we are going to use.
		final String sparqlName = StringUtils.defaultString(alias,
				col.getSPARQLName());
		if (LOG.isDebugEnabled())
		{
			LOG.debug( String.format("addVar %s as %s", col.getSPARQLName(), sparqlName ));
		}

		// allocate the var for the real name.
		final Var v = Var.alloc(col.getSPARQLName());
		// if we have not seen this var before register all the info.
		if (!query.getResultVars().contains(v.toString()))
		{
			// if an alias is being used then do special registration
			if ((alias != null) && !sparqlName.equalsIgnoreCase(col.getSPARQLName()))
			{
				QueryColumnInfo qci = new QueryColumnInfo(infoSet, col, alias);
				final ElementBind bind = new ElementBind(Var.alloc(qci.getVar()), new ExprVar(v));
				getElementGroup().addElement(bind);
				// add the alias for the column to the columns list.
				//new QueryColumnInfo(infoSet, col, v.getName());
			}
			query.addResultVar(v);
			// make sure SELECT * is processed
			query.getResultVars();
			columnsInResult.add(col);
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
	public void addVars( final Iterator<RdfColumn> cols ) throws SQLException
	{
		checkBuilt();
		while (cols.hasNext())
		{
			final RdfColumn col = cols.next();
			addVar(col, getColCount(col) > 1 ? null : col.getName());
		}
	}

	/**
	 * Get the SPARQL query.
	 * 
	 * @return The constructed SPARQL query.
	 */
	public Query build()
	{

		if (!isBuilt)
		{
			if (!catalog.isService())
			{
				// apply the type filters to each subpart.
				for (final QueryTableInfo sti : infoSet.listTables())
				{
					sti.addTypeFilters();
				}

			}

			// renumber the Bnodes.
			final Element e = new BnodeRenumber().renumber(query
					.getQueryPattern());
			query.setQueryPattern(e);

			if (catalog.isService())
			{
				// create a copy of the query so that we can verify that it is
				// good.
				Query result = query.cloneQuery();

				// create the service call
				// make sure we project all vars for the filters.
				final Set<Var> vars = new HashSet<Var>();
				for (final CheckTypeF f : getTypeFilterList())
				{
					vars.add(f.getArg().asVar());
				}
				result.addProjectVars(vars);
				final ElementService service = new ElementService(
						catalog.getServiceNode(), new ElementSubQuery(result),
						false);
				// create real result
				result = new Query();
				result.setQuerySelectType();
				result.addProjectVars(query.getProjectVars());
				SparqlQueryBuilder.getElementGroup(result).addElement(service);
				for (final CheckTypeF f : getTypeFilterList())
				{
					SparqlQueryBuilder.addFilter(result, f);
				}
				query = result;
			}
			isBuilt = true;
			SparqlQueryBuilder.LOG.debug("Query parsed as {}", query);
		}
		return query;
	}

	private void checkBuilt()
	{
		if (isBuilt)
		{
			throw new IllegalStateException("Query was already built");
		}
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
	private Collection<RdfColumn> findColumns( final String schemaNamePattern,
			final String tableNamePattern, final String columnNamePattern )
	{
		if (SparqlQueryBuilder.LOG.isDebugEnabled())
		{
			SparqlQueryBuilder.LOG.debug(String.format(
					"Looking for column %s.%s.%s", schemaNamePattern,
					tableNamePattern, columnNamePattern));
		}
		final List<RdfColumn> columns = new ArrayList<RdfColumn>();
		for (final Schema schema : catalog.findSchemas(schemaNamePattern))
		{
			for (final Table table : schema.findTables(tableNamePattern))
			{
				for (final Column column : table.findColumns(columnNamePattern))
				{
					if (column instanceof RdfColumn)
					{
						columns.add((RdfColumn) column);
					}
				}
			}
		}
		return columns;
	}

	private Collection<RdfTable> findTables( final String schemaName,
			final String tableName )
	{
		if (SparqlQueryBuilder.LOG.isDebugEnabled())
		{
			SparqlQueryBuilder.LOG.debug(String.format(
					"Looking for Table %s.%s", schemaName, tableName));
		}
		final List<RdfTable> tables = new ArrayList<RdfTable>();
		for (final Schema schema : catalog.findSchemas(schemaName))
		{
			for (final Table table : schema.findTables(tableName))
			{
				if (table instanceof RdfTable)
				{
					tables.add((RdfTable) table);
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
	public RdfCatalog getCatalog()
	{
		return catalog;
	}

	// get the number of columns in the selected tables with the same name
	private int getColCount( final Column col )
	{
		int retval = 0;
		for (final Column c : infoSet.getColumns())
		{
			if (c.getName().equalsIgnoreCase(col.getName()))
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
	 * @return The RdfColumn or null if no column is defined in the query.
	 */
//	public RdfColumn getColumn( final String schemaName,
//			final String tableName, final String columnName )
//	{
//		if (schemaName == null)
//		{
//			throw new IllegalArgumentException("Schema name may not be null");
//		}
//		if (tableName == null)
//		{
//			throw new IllegalArgumentException("Table name may not be null");
//		}
//		if (columnName == null)
//		{
//			throw new IllegalArgumentException("Column name may not be null");
//		}
//		final Iterator<RdfColumn> iter = findColumns(schemaName, tableName,
//				columnName).iterator();
//		return (iter.hasNext()) ? iter.next() : null;
//	}
	
	public QueryTableInfo getTable( QueryTableInfo.Name name )
	{
		return infoSet.getTable(name);
	}

	private ElementGroup getElementGroup()
	{
		return SparqlQueryBuilder.getElementGroup(query);
	}

	public QueryColumnInfo getNodeColumn( final Node n )
	{
		checkBuilt();
		return infoSet.getColumnByNode(n);
	}

	public QueryTableInfo getNodeTable( final Node n )
	{
		checkBuilt();
		return infoSet.getTableByNode(n);
	}

	public List<Column> getResultColumns()
	{
		return columnsInResult;
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
	public RdfTableDef getTableDef( final String namespace,
			final String localName )
	{
		final RdfTableDef.Builder builder = new RdfTableDef.Builder();
		final Model model = ModelFactory.createDefaultModel();

		final VarExprList expLst = query.getProject();
		new ArrayList<Column>();
		for (final Var var : expLst.getVars())
		{

			final Expr expr = expLst.getExpr(var);
			final String varColName = NameUtils
					.convertSPARQL2DB(expr == null ? var.getName() : expr
							.getExprVar().getVarName());
			builder.addColumnDef(infoSet.getColumnByName(varColName)
					.getColumn().getColumnDef());

		}
		return builder.build(model);
	}

	private Set<CheckTypeF> getTypeFilterList()
	{
		final Set<CheckTypeF> retval = new HashSet<CheckTypeF>();
		for (final QueryTableInfo sti : infoSet.getTables())
		{
			retval.addAll(sti.getTypeFilterList());
		}
		return retval;
	}

	/**
	 * @return returns true if the query is going to return all the columns from
	 *         all the tables.
	 */
	public boolean isAllColumns()
	{
		checkBuilt();
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
		LOG.debug( "Setting All Columns");
		checkBuilt();
		int i = 0;
		for (final QueryTableInfo t : infoSet.getTables())
		{

			final Iterator<RdfColumn> iter = t.getColumns();
			while (iter.hasNext())
			{
				final RdfColumn col = iter.next();
				if ((i == 0) || !columnsInUsing.contains(col.getName()))
				{
					addColumn(col);
					addVar(col, i == 0 ? col.getName()
							: getColCount(col) > 1 ? null : col.getName());
				}
			}
			i++;
		}
	}

	/**
	 * Sets the distinct flag for the SPARQL query.
	 */
	public void setDistinct()
	{
		LOG.debug("Setting Distinct");
		checkBuilt();
		query.setDistinct(true);
	}

	public SparqlQueryBuilder setKey( final RdfKey key )
	{
		LOG.debug( "Setting key {}", key );
		if (key != null)
		{
			setOrderBy(key);
			if (key.isUnique())
			{
				setDistinct();
			}
		}
		return this;
	}

	/**
	 * Sets the limit for the SPARQL query.
	 * 
	 * @param limit
	 *            The number of records to return
	 */
	public void setLimit( final Long limit )
	{
		LOG.debug( "Setting limit {}", limit );
		checkBuilt();
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
		LOG.debug( "Setting Offset {}", offset );
		checkBuilt();
		query.setOffset(offset);
	}

	public void setOrderBy( final RdfKey key )
	{
		LOG.debug( "Setting orderBy {}", key );
		final List<Var> vars = query.getProjectVars();
		for (final RdfKeySegment seg : key.getSegments())
		{
			query.addOrderBy(vars.get(seg.getIdx()),
					seg.isAscending() ? Query.ORDER_ASCENDING
							: Query.ORDER_DESCENDING);
		}
	}

	/**
	 * Return the SPARQL query as the string value for the builder.
	 */
	@Override
	public String toString()
	{
		return String.format("QueryBuilder%s[%s]", (isBuilt ? "(builtargs)"
				: ""), query.toString());
	}

}

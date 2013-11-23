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
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementService;
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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

	static final String NOT_FOUND_IN_ = "%s was not found in %s";

	public static final boolean OPTIONAL = true;
	public static final boolean REQUIRED = false;

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
	//private final List<Column> columnsInResult;

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
		//this.columnsInResult = new ArrayList<Column>();
		query.setQuerySelectType();
	}
	
	public int getColumnIndex(final String columnName )
	{
		QueryColumnInfo.Name cName = QueryColumnInfo.getNameInstance(columnName);	
		return infoSet.getColumnIndex(cName);
	}
	
	public int getColumnCount()
	{
		if (!isBuilt)
		{
			//return infoSet.getColumns().size();
			throw new IllegalStateException( "Column count may not be retrieved from a builder until after query is built");
		}
		
		return query.getProjectVars().size();
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
		SparqlQueryBuilder.LOG.debug("addBGP: {}", t);
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
	public Node addColumn( final RdfColumn column, final String alias )
			throws SQLException
	{
		final QueryColumnInfo.Name cName = QueryColumnInfo
				.getNameInstance(alias);
		if (SparqlQueryBuilder.LOG.isDebugEnabled())
		{
			SparqlQueryBuilder.LOG.debug(
					"(addColumn-2-arg) looking for Column {} as {}",
					column.getSQLName(), cName);
		}
		checkBuilt();
		if (!infoSet.containsColumn(cName))
		{
			if (SparqlQueryBuilder.LOG.isDebugEnabled())
			{
				SparqlQueryBuilder.LOG.debug("adding Column {}",
						column.getSQLName());
			}
			final QueryTableInfo.Name tName = QueryTableInfo
					.getNameInstance(cName);
			QueryTableInfo tableInfo = infoSet.getTable(tName);
			if (tableInfo == null)
			{
				tableInfo = new QueryTableInfo(infoSet, getElementGroup(),
						column.getTable(), tName, false);
				tableInfo.addRequiredColumns(false,
						Collections.<String> emptyList());
			}
			return tableInfo.addColumnToQuery(column, cName);
		}
		else
		{
			// already there, return node from the query.
			return infoSet.getColumnByName(cName).getVar();
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
			final String columnName, boolean optional ) throws SQLException
	{
		if (SparqlQueryBuilder.LOG.isDebugEnabled())
		{
			SparqlQueryBuilder.LOG.debug("(addColumn-3-args) adding Column {}",
					NameUtils.getDBName(schemaName, tableName, columnName));
		}
		checkBuilt();
		final QueryColumnInfo.Name cName = QueryColumnInfo.getNameInstance(
				schemaName, tableName, columnName);
		QueryColumnInfo columnInfo = infoSet.scanTablesForColumn(cName);
		if (columnInfo == null)
		{
			QueryTableInfo.Name tName = QueryTableInfo.getNameInstance(cName);
			if (tName.isWild())
			{
				throw new SQLException(String.format(
						SparqlQueryBuilder.NOT_FOUND_IN_ANY_, cName, "table"));
			} else {
				throw new SQLException(String.format(
						SparqlQueryBuilder.NOT_FOUND_IN_, cName, tName));
			}
		}
		return columnInfo.getVar();
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
	 * Add an order by to the query.
	 * 
	 * @param expr
	 *            The expression to order by.
	 * @param ascending
	 *            true of order should be ascending, false = descending.
	 */
	public void addOrderBy( final Expr expr, final boolean ascending )
	{
		SparqlQueryBuilder.LOG.debug("Adding order by {} {}", expr,
				ascending ? "Ascending" : "Descending");
		checkBuilt();
		query.addOrderBy(expr, ascending ? Query.ORDER_ASCENDING
				: Query.ORDER_DESCENDING);
	}

	public void addRequiredColumns()
	{
		infoSet.addRequiredColumns(Collections.<String> emptyList());
	}

	/**
	 * Add a table to the query.
	 * 
	 * @param name
	 *            The table name
	 * @param optional
	 *            if true table is optional.
	 * @return The node that represents the table.
	 * @throws SQLException
	 *             if the table is in multiple schemas or not found.
	 */
	public Node addTable( final QueryTableInfo.Name name, QueryTableInfo.Name asName, final boolean optional )
			throws SQLException
	{

		if (SparqlQueryBuilder.LOG.isDebugEnabled())
		{
			SparqlQueryBuilder.LOG.debug(String.format("Adding %s table %s as %s",
					optional ? "optional" : "required", name, asName));
		}
		checkBuilt();
		final Collection<RdfTable> tables = findTables(name);

		if (tables.size() > 1)
		{
			throw new SQLException(String.format(
					SparqlQueryBuilder.FOUND_IN_MULTIPLE_+" of catalog '%s'", name, "schemas", catalog.getName()));
		}
		if (tables.isEmpty())
		{
			throw new SQLException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_ANY_+" of catalog '%s'", name, "schema", catalog.getName()));
		}
		return addTable(tables.iterator().next(), asName, optional);
	}

	public Node addTable( final RdfTable table, final QueryTableInfo.Name name,
			final boolean optional )
	{
		if (SparqlQueryBuilder.LOG.isDebugEnabled())
		{
			SparqlQueryBuilder.LOG.debug("Adding Rdftable {} as {}",
					table.getSQLName(), name);
		}

		// make sure the table is in the query.
		QueryTableInfo tableInfo = infoSet.getTable(name);
		if (tableInfo == null)
		{
			tableInfo = new QueryTableInfo(infoSet, getElementGroup(), table,
					name, optional);
		}
		return tableInfo.getVar();
	}

	public void addTableColumns( final QueryTableInfo tableInfo )
	{
		tableInfo.addTableColumns(query);
	}

	public void addUsing( final String columnName )
	{
		SparqlQueryBuilder.LOG.debug("adding Using {}", columnName);
		final Set<String> tableAliases = infoSet.getTableAliases();
		if (tableAliases.size() < 2)
		{
			throw new IllegalArgumentException(
					"There must be at least 2 tables in the query");
		}
		final Iterator<String> iter = tableAliases.iterator();
		final QueryTableInfo tableInfo = infoSet.getTable(iter.next());
		final QueryColumnInfo.Name cName = QueryColumnInfo
				.getNameInstance(columnName);
		final QueryColumnInfo columnInfo = tableInfo.getColumn(cName, false);
		if (columnInfo == null)
		{
			throw new IllegalArgumentException(String.format(
					"column %s not found in %s", columnName,
					tableInfo.getSQLName()));
		}
		columnsInUsing.add(columnName);
		// final ExprVar left = new ExprVar(columnInfo.getVar());
		while (iter.hasNext())
		{
			final QueryTableInfo tableInfo2 = infoSet.getTable(iter.next());
			final QueryColumnInfo columnInfo2 = tableInfo2.getColumn(cName,
					false);
			if (columnInfo2 == null)
			{
				throw new IllegalArgumentException(String.format(
						"column %s not found in %s", columnInfo2,
						tableInfo2.getSQLName()));
			}
			// addFilter(new E_Equals(left, new ExprVar(columnInfo2.getVar())));
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
		SparqlQueryBuilder.LOG.debug("Adding Var {} as {}", expr, name);
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
		//columnsInResult.add(column);
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
	{
		checkBuilt();

		// figure out what name we are going to use.
		final QueryColumnInfo.Name cName = QueryColumnInfo
				.getNameInstance(StringUtils.defaultIfBlank(alias,
						col.getSQLName()));

		if (SparqlQueryBuilder.LOG.isDebugEnabled())
		{
			SparqlQueryBuilder.LOG.debug(String.format("addVar %s as %s",
					col.getSPARQLName(), cName));
		}

		// allocate the var for the real name.
		final Var v = Var.alloc(cName.getSPARQLName());
		// if we have not seen this var before register all the info.
		if (!query.getResultVars().contains(v.toString()))
		{
			// see if we have registered the name
			QueryColumnInfo columnInfo = infoSet.getColumnByName(cName);
			if (columnInfo == null)
			{
				QueryTableInfo tableInfo = null;
				// see if we have registered the column before
				columnInfo = infoSet.getColumnByName(col.getSQLName());
				if (columnInfo == null)
				{
					// add native column to query
					tableInfo = infoSet.getTable(col.getTable().getSQLName());
					if (tableInfo == null)
					{
						throw new IllegalStateException(String.format(
								SparqlQueryBuilder.NOT_FOUND_IN_QUERY, col
										.getTable().getSQLName()));
					}
					columnInfo = tableInfo.getColumn(QueryColumnInfo
							.getNameInstance(col.getSQLName()));
					if (columnInfo == null)
					{
						throw new IllegalStateException(String.format(
								SparqlQueryBuilder.NOT_FOUND_IN_,
								col.getSQLName(), tableInfo.getName()));
					}
				}
				if (!columnInfo.getName().equals(cName))
				{
					if (tableInfo == null)
					{
						tableInfo = infoSet.getTable(col.getTable()
								.getSQLName());
						if (tableInfo == null)
						{
							throw new IllegalStateException(String.format(
									SparqlQueryBuilder.NOT_FOUND_IN_QUERY, col
											.getTable().getSQLName()));
						}
					}
					tableInfo.addColumnToQuery(columnInfo, cName);
				}

			}
			query.addResultVar(v);
			// make sure SELECT * is processed
			query.getResultVars();
			//columnsInResult.add(col);
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

	private Collection<RdfTable> findTables( QueryItemName name )
	{
		if (SparqlQueryBuilder.LOG.isDebugEnabled())
		{
			SparqlQueryBuilder.LOG.debug(String.format(
					"Looking for Table %s.%s in '%s' catalog", name.getSchema(), name.getTable(), catalog.getName()));
		}
		final List<RdfTable> tables = new ArrayList<RdfTable>();
		
		for (final Schema schema : catalog.findSchemas(name.getSchema()))
		{
			for (final Table table : schema.findTables(name.getTable()))
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
		QueryColumnInfo.Name cName = QueryColumnInfo.getNameInstance(col.getName());
		
		int retval = 0;
		for (final QueryColumnInfo columnInfo : infoSet.getColumns())
		{
			if (cName.matches( columnInfo.getName()))
			{
				retval++;
			}
		}
		return retval;
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

	public QueryColumnInfo getColumn( QueryColumnInfo.Name cName )
	{
		return infoSet.getColumnByName(cName);
	}
	
	public QueryColumnInfo getColumn( int i )
	{
		if (!isBuilt)
		{
			//return (QueryColumnInfo) infoSet.getColumns().toArray()[i];
			throw new IllegalStateException( "Columns may not be retrieved from a builder until after query is built");
		}
		
		Var v = query.getProjectVars().get(i);
		return infoSet.getColumnByName( v );
		//return (QueryColumnInfo) infoSet.getColumns().toArray()[i];
		
	}
	
	public List<QueryColumnInfo> getResultColumns()
	{
		return infoSet.getColumns();
	}

	/**
	 * Get a table from the query.
	 * 
	 * @param name
	 *            The table name.
	 * @return The query table info for the name or null if none found
	 * @throws IllegalArgumentException
	 *             if more than one object matches.
	 */
	public QueryTableInfo getTable( final QueryTableInfo.Name name )
	{
		return infoSet.getTable(name);
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
		SparqlQueryBuilder.LOG.debug("Setting All Columns");
		checkBuilt();

		final Collection<QueryTableInfo> tableInfos = infoSet.getTables();
		if (tableInfos.size() == 0)
		{
			throw new IllegalArgumentException(
					"There must be a least one table");
		}
		//final boolean shortName = (tableInfos.size() == 1) || forceShortName;
		QueryColumnInfo.Name name = null;
		for (final QueryTableInfo tableInfo : tableInfos)
		{
			final Iterator<RdfColumn> iter = tableInfo.getRdfTable()
					.getColumns();
			while (iter.hasNext())
			{
				final RdfColumn col = iter.next();
				if (infoSet.getShortNames())
				{
					name = QueryColumnInfo.getNameInstance(col.getName());
				}
				else
				{
					name = tableInfo.getName().getColumnName(col.getName());
				}

				final QueryColumnInfo columnInfo = tableInfo.getColumn(name,
						col.isOptional());
				final Var v = Var.alloc(columnInfo.getVar());
				if (!query.getResultVars().contains(v.toString()))
				{
					query.addResultVar(v);
				}
			}
		}
	}

	/**
	 * Sets the distinct flag for the SPARQL query.
	 */
	public void setDistinct()
	{
		SparqlQueryBuilder.LOG.debug("Setting Distinct");
		checkBuilt();
		query.setDistinct(true);
	}

	public void setForceShortName( final boolean state )
	{
		infoSet.setShortNames( state );
	}

	public SparqlQueryBuilder setKey( final RdfKey key )
	{
		SparqlQueryBuilder.LOG.debug("Setting key {}", key);
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
		SparqlQueryBuilder.LOG.debug("Setting limit {}", limit);
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
		SparqlQueryBuilder.LOG.debug("Setting Offset {}", offset);
		checkBuilt();
		query.setOffset(offset);
	}

	public void setOrderBy( final RdfKey key )
	{
		SparqlQueryBuilder.LOG.debug("Setting orderBy {}", key);
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

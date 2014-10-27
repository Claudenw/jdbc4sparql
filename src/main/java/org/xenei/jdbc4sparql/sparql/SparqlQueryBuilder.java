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
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.expr.aggregate.Aggregator;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementService;
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;
import com.hp.hpl.jena.sparql.syntax.ElementUnion;

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
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnName;
import org.xenei.jdbc4sparql.iface.ItemName;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableName;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfKey;
import org.xenei.jdbc4sparql.impl.rdf.RdfKeySegment;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef;
import org.xenei.jdbc4sparql.impl.virtual.VirtualSchema;
import org.xenei.jdbc4sparql.impl.virtual.VirtualTable;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.FunctionColumn;

/**
 * Creates a SparqlQuery while tracking naming changes between nomenclatures.
 */
public class SparqlQueryBuilder
{
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

	// a set of error messages.
	static final String NOT_FOUND_IN_QUERY = "%s not found in SPARQL query";

	static final String FOUND_IN_MULTIPLE_ = "%s was found in multiple %s";

	static final String NOT_FOUND_IN_ANY_ = "%s was not found in any %s";
	static final String NOT_FOUND_IN_ = "%s was not found in %s";
	public static final boolean OPTIONAL = true;
	public static final boolean REQUIRED = false;

	private final SparqlParser parser;

	private final Map<String, Catalog> catalogs;

	private final QueryInfoSet infoSet;

	// the query we are building.
	private Query query;

	// query was built flag;
	private boolean isBuilt;

	// sparql catalog we are running against.
	private final RdfCatalog catalog;

	// sparql schema for default tables
	private final RdfSchema schema;

	// the list of columns not to be included in the "all columns" result.
	// perhaps this should store column so that tables may be checked in case
	// tables are added to the query later. But I don't think so.
	private final List<String> columnsInUsing;

	// columns indexed by var.
	// private final List<Column> columnsInResult;

	private static Logger LOG = LoggerFactory
			.getLogger(SparqlQueryBuilder.class);

	/**
	 * Create a query builder for a catalog and schema
	 *
	 * @param catalog
	 *            The catalog to build the query for.
	 * @param schema
	 *            The schema to build the query for.
	 * @throws IllegalArgumentException
	 *             if catalog is null.
	 */
	public SparqlQueryBuilder( final Map<String, Catalog> catalogs,
			final SparqlParser parser, final RdfCatalog catalog,
			final RdfSchema schema )
	{
		if (catalog == null)
		{
			throw new IllegalArgumentException("Catalog may not be null");
		}
		if (parser == null)
		{
			throw new IllegalArgumentException("SparqlParser may not be null");
		}
		this.catalogs = catalogs;
		this.parser = parser;
		this.catalog = catalog;
		this.schema = schema;
		this.query = new Query();
		this.isBuilt = false;
		this.infoSet = new QueryInfoSet();
		this.columnsInUsing = new ArrayList<String>();
		query.setQuerySelectType();
	}

	/**
	 * Create a sub query builder
	 *
	 * @param parent
	 *            The QueryBuilder that this is a sub query for.
	 */
	public SparqlQueryBuilder( final SparqlQueryBuilder parent )
	{
		this(parent.catalogs, parent.parser, parent.catalog, parent.schema);
	}

	public void addAlias( final Expr expr, final ColumnName columnName )
	{
		QueryColumnInfo varName = null;
		Var v = null;
		ColumnName aliasName = null;
		if (expr instanceof ExprAggregator)
		{
			v = ((ExprAggregator) expr).getVar();
			varName = infoSet.getColumnByName(v.getName());
			aliasName = columnName.merge(ColumnName.FUNCTION);
		}
		else
		{
			v = expr.asVar();
			varName = infoSet.getColumnByName(v.getName());
			aliasName = columnName.merge(varName.getName());
		}

		varName.addAlias(infoSet, aliasName);
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
	public Node addColumn( final ColumnName cName, final boolean optional )
			throws SQLException
	{
		if (SparqlQueryBuilder.LOG.isDebugEnabled())
		{
			SparqlQueryBuilder.LOG.debug("adding Column {}", cName);
		}
		checkBuilt();
		final QueryColumnInfo columnInfo = infoSet.scanTablesForColumn(cName);
		if (columnInfo == null)
		{
			final TableName tName = cName.getTableName();
			if (tName.isWild())
			{
				throw new SQLException(String.format(
						SparqlQueryBuilder.NOT_FOUND_IN_ANY_, cName, "table"));
			}
			else
			{
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

	public Node addFunction( final Column column )
	{
		final TableName tableName = column.getName().getTableName();
		QueryTableInfo tableInfo = getTable(tableName);
		if (tableInfo == null)
		{
			addTable(column.getTable(), tableName, false);
			tableInfo = getTable(tableName);
		}
		final ColumnName colName = column.getName();
		final QueryColumnInfo columnInfo = new QueryColumnInfo(infoSet,
				tableInfo, column, colName, false);
		return columnInfo.getVar();
	}

	public void addGroupBy( final Expr expr )
	{
		query.addGroupBy(expr);
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
		infoSet.addRequiredColumns();
	}

	public Node addTable( final Table<Column> table, final TableName name,
			final boolean optional )
	{
		if (SparqlQueryBuilder.LOG.isDebugEnabled())
		{
			SparqlQueryBuilder.LOG.debug("Adding table {} as {}",
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

	/**
	 * Add a table to the query.
	 *
	 * @param name
	 *            The table name
	 * @param asName
	 *            The name of the table as referenced in the query.
	 * @param optional
	 *            if true table is optional.
	 * @return The node that represents the table.
	 * @throws SQLException
	 *             if the table is in multiple schemas or not found.
	 */
	public Node addTable( final TableName name, final TableName asName,
			final boolean optional ) throws SQLException
	{

		if (SparqlQueryBuilder.LOG.isDebugEnabled())
		{
			SparqlQueryBuilder.LOG.debug(String.format(
					"Adding %s table %s as %s", optional ? "optional"
							: "required", name, asName));
		}
		checkBuilt();
		final Collection<Table<Column>> tables = findTables(name);

		if (tables.size() > 1)
		{
			throw new SQLException(String.format(
					SparqlQueryBuilder.FOUND_IN_MULTIPLE_ + " of catalog '%s'",
					name, "schemas", catalog.getName()));
		}
		if (tables.isEmpty())
		{
			throw new SQLException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_ANY_ + " of catalog '%s'",
					name, "schema", catalog.getName()));
		}
		return addTable(tables.iterator().next(), asName, optional);
	}

	public void addTableColumns( final QueryTableInfo tableInfo )
	{
		tableInfo.addTableColumns(query);
	}

	public void addUnion( final List<SparqlQueryBuilder> unionBuilders )
	{
		final ElementUnion unionElement = new ElementUnion();
		for (final SparqlQueryBuilder sqb : unionBuilders)
		{
			unionElement.addElement(new ElementSubQuery(sqb.build()));
		}
		getElementGroup().addElement(unionElement);
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
		final ColumnName cName = ColumnName.getNameInstance(columnName);
		final QueryColumnInfo columnInfo = tableInfo.getColumn(cName, false);
		if (columnInfo == null)
		{
			throw new IllegalArgumentException(String.format(
					"column %s not found in %s", columnName,
					tableInfo.getSQLName()));
		}
		columnsInUsing.add(columnName);
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
		checkBuilt();
		final NodeValue nv = expr.getConstant();
		if ((name != null) || (nv == null) || !nv.getNode().isVariable())
		{
			final String s = StringUtils.defaultString(expr.getVarName());
			if (s.equals(StringUtils.defaultIfBlank(name, s)))
			{
				query.addResultVar(s);
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
		query.getResultVars();
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

	private Collection<Table<Column>> findTables( final ItemName name )
	{

		if (SparqlQueryBuilder.LOG.isDebugEnabled())
		{
			SparqlQueryBuilder.LOG.debug(String.format(
					"Looking for Table %s.%s in '%s' catalog",
					name.getSchema(), name.getTable(), catalog.getName()));
		}

		final List<Table<Column>> tables = new ArrayList<Table<Column>>();

		for (final Schema schema : catalog.findSchemas(name.getSchema()))
		{
			for (final Table<Column> table : schema.findTables(name.getTable()))
			{
				tables.add(table);
			}
		}
		return tables;
	}

	public void forceShortName()
	{
		infoSet.forceShortNames();
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

	public Catalog getCatalog( final String catalog )
	{
		return catalogs.get(catalog);
	}

	public QueryColumnInfo getColumn( final ColumnName cName )
	{
		return infoSet.getColumnByName(cName);
	}

	public QueryColumnInfo getColumn( final int i )
	{
		if (!isBuilt)
		{
			throw new IllegalStateException(
					"Columns may not be retrieved from a builder until after query is built");
		}

		final Var v = query.getProjectVars().get(i);
		return infoSet.getColumnByName(v);
	}

	// get the number of columns in the selected tables with the same name
	// TODO private int getColCount( final Column col )
	// {
	// QueryColumnInfo.Name cName =
	// QueryColumnInfo.getNameInstance(col.getName());
	//
	// int retval = 0;
	// for (final QueryColumnInfo columnInfo : infoSet.getColumns())
	// {
	// if (cName.matches( columnInfo.getName()))
	// {
	// retval++;
	// }
	// }
	// return retval;
	// }

	public int getColumnCount()
	{
		if (!isBuilt)
		{
			// return infoSet.getColumns().size();
			throw new IllegalStateException(
					"Column count may not be retrieved from a builder until after query is built");
		}

		return query.getProjectVars().size();
	}

	public int getColumnIndex( final String columnName )
	{
		final ColumnName cName = ColumnName.getNameInstance(columnName);
		return infoSet.getColumnIndex(cName);
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

	public List<QueryColumnInfo> getResultColumns()
	{
		final List<QueryColumnInfo> retval = new ArrayList<QueryColumnInfo>();
		for (final Var v : query.getProjectVars())
		{
			retval.add(infoSet.getColumnByName(v));
		}
		return retval;
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
	public QueryTableInfo getTable( final TableName name )
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
			final QueryColumnInfo colInfo = infoSet.getColumnByName(varColName);
			builder.addColumnDef(colInfo.getColumn().getColumnDef());
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

	public Expr register( final Aggregator agg, final int type )
			throws SQLException
	{
		final ExprAggregator expr = (ExprAggregator) query.allocAggregate(agg);

		final ColumnName columnName = ColumnName.getNameInstance(NameUtils
				.convertSPARQL2DB(expr.getAggVar().getVarName()));
		if (infoSet.findColumn(columnName) == null)
		{
			registerFunctionColumn(columnName, type);
		}
		return expr;
	}

	public QueryColumnInfo registerFunctionColumn( ColumnName columnName,
			final int type )
	{
		columnName = new ColumnName(StringUtils.defaultString(
				columnName.getSchema(), ""), StringUtils.defaultString(
				columnName.getTable(), ""), columnName.getShortName());
		QueryTableInfo tableInfo = infoSet.getTable(columnName.getTableName());
		Column column = null;
		if (tableInfo == null)
		{
			final Catalog cat = getCatalog(""); // FIXME define this
			Schema schema = cat.getSchema(columnName.getSchema());
			if (schema == null)
			{
				schema = new VirtualSchema(cat, columnName.getSchema());
			}
			Table<Column> table = schema.getTable(columnName.getTable());
			if (table == null)
			{
				table = new VirtualTable(schema, columnName.getTable());
			}
			column = new FunctionColumn(table, columnName.getShortName(), type);
			// new QueryTableInfo adds table to infoSet
			tableInfo = new QueryTableInfo(infoSet, null, table,
					columnName.getTableName(), false);
		}
		else
		{
			column = new FunctionColumn(tableInfo.getTable(),
					columnName.getShortName(), type);
		}
		return new QueryColumnInfo(infoSet, tableInfo, column, columnName,
				false);
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

		ColumnName name = null;
		for (final QueryTableInfo tableInfo : tableInfos)
		{
			final Iterator<Column> iter = tableInfo.getTable().getColumns();
			while (iter.hasNext())
			{
				final Column col = iter.next();
				name = infoSet.createColumnName(col.getName());

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

	public void setHaving( final Expr expr )
	{
		query.addHavingCondition(expr);
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

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

import java.lang.reflect.Field;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Key;
import org.xenei.jdbc4sparql.iface.KeySegment;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.FQName;
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.impl.virtual.VirtualCatalog;
import org.xenei.jdbc4sparql.impl.virtual.VirtualSchema;
import org.xenei.jdbc4sparql.impl.virtual.VirtualTable;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.items.QueryItemCollection;
import org.xenei.jdbc4sparql.sparql.items.QueryTableInfo;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.FunctionColumn;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.E_Equals;
import org.apache.jena.sparql.expr.E_Function;
import org.apache.jena.sparql.expr.E_LogicalAnd;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.aggregate.Aggregator;
import org.apache.jena.sparql.graph.NodeTransform;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementBind;
import org.apache.jena.sparql.syntax.ElementFilter;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementService;
import org.apache.jena.sparql.syntax.ElementSubQuery;
import org.apache.jena.sparql.syntax.ElementUnion;
import org.apache.jena.util.iterator.Filter;

/**
 * Creates a SparqlQuery while tracking naming changes between nomenclatures.
 */
public class SparqlQueryBuilder {

	private static ElementGroup getElementGroup(final Query query) {
		ElementGroup retval;
		final Element e = query.getQueryPattern();
		if (e == null) {
			retval = new ElementGroup();
			query.setQueryPattern(retval);
		}
		else if (e instanceof ElementGroup) {
			retval = (ElementGroup) e;
		}
		else {
			retval = new ElementGroup();
			retval.addElement(e);
		}
		return retval;
	}

	// a set of error messages.
	static final String NOT_FOUND_IN_QUERY = "%s not found in SPARQL query";
	public static final String FOUND_IN_MULTIPLE_ = "%s was found in multiple %s";

	static final String NOT_FOUND_IN_ANY_ = "%s was not found in any %s";
	public static final String NOT_FOUND_IN_ = "%s was not found in %s";
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
	private final Catalog catalog;

	// sparql schema for default tables
	private final Schema schema;

	// the count of aliases used in this query.
	private int aliasCount;

	// the list of columns not to be included in the "all columns" result.
	// perhaps this should store column so that tables may be checked in case
	// tables are added to the query later. But I don't think so.
	private final List<ColumnName> columnsInUsing;
	
	/*
	 * A node mapping created during build() call
	 */
	private NodeMapper nodeMapper;

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
	public SparqlQueryBuilder(final Map<String, Catalog> catalogs,
			final SparqlParser parser, final Catalog catalog,
			final Schema schema) {
		if (catalogs == null) {
			throw new IllegalArgumentException("Catalogs may not be null");
		}
		if (catalog == null) {
			throw new IllegalArgumentException("Catalog may not be null");
		}
		if (!catalogs.containsKey(catalog.getShortName())) {
			catalogs.put(catalog.getShortName(), catalog);
		}
		if (parser == null) {
			throw new IllegalArgumentException("SparqlParser may not be null");
		}
		this.aliasCount = 0;
		this.catalogs = catalogs;
		this.parser = parser;
		this.catalog = catalog;
		this.schema = schema;
		this.query = new Query();
		this.isBuilt = false;
		this.infoSet = new QueryInfoSet();
		this.columnsInUsing = new ArrayList<ColumnName>();
		query.setQuerySelectType();
	}

	/**
	 * Create a sub query builder
	 *
	 * @param parent
	 *            The QueryBuilder that this is a sub query for.
	 */
	public SparqlQueryBuilder(final SparqlQueryBuilder parent) {
		this(parent.catalogs, parent.parser, parent.catalog, parent.schema);
	}

	public QueryColumnInfo addAlias(final ColumnName orig,
			final ColumnName alias) throws SQLDataException {
		final QueryTableInfo tableInfo = infoSet.getTable(orig.getTableName());
		if (tableInfo == null) {
			throw new IllegalArgumentException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_QUERY, orig.getTableName()));
		}

		// find the column will check infoset for existing column first.
		QueryColumnInfo columnInfo = tableInfo.getColumn(orig);
		if (columnInfo == null) {
			// ok now we have to look for column with the proper name.
			final Column column = tableInfo.getTable().getColumn(
					orig.getShortName());
			if (column == null) {
				throw new IllegalArgumentException(String.format(
						SparqlQueryBuilder.NOT_FOUND_IN_QUERY, orig));
			}
			columnInfo = tableInfo.addColumnToQuery(column);
		}
		// tableInfo.addDataFilter(columnInfo);
		// infoSet.addColumn(columnInfo);

		final QueryColumnInfo aliasInfo = columnInfo.createAlias(alias);
		tableInfo.addDataFilter(aliasInfo);
		infoSet.addColumn(aliasInfo);
		return aliasInfo;
	}

	public int getAliasCount() {
		return aliasCount++;
	}

	/**
	 * Add the column to the query. Column must be in a table that has already
	 * been added to the query.
	 *
	 * @param cName
	 *            The column name.
	 * @param optional
	 *            True if the column is optional.
	 * @return The QueryColumnInfo for the column
	 * @throws SQLException
	 */
	public QueryColumnInfo addColumn(final ColumnName cName,
			final boolean optional) throws SQLException {
		if (SparqlQueryBuilder.LOG.isDebugEnabled()) {
			SparqlQueryBuilder.LOG.debug("Adding Column {}", cName);
		}
		checkBuilt();
		final QueryColumnInfo columnInfo = infoSet.scanTablesForColumn(cName);
		if (columnInfo == null) {
			final TableName tName = cName.getTableName();
			if (tName.isWild()) {
				throw new SQLException(String.format(
						SparqlQueryBuilder.NOT_FOUND_IN_ANY_, cName, "table"));
			}
			else {
				throw new SQLException(String.format(
						SparqlQueryBuilder.NOT_FOUND_IN_, cName, tName));
			}
		}
		return columnInfo;
	}

	/**
	 * Add a filter to the query.
	 *
	 * @param filter
	 *            The filter to add.
	 */
	public void addFilter(final Expr filter) {
		if (LOG.isDebugEnabled()) {
			SparqlQueryBuilder.LOG.debug("adding Filter: {}", filter);
		}
		checkBuilt();
		final ElementFilter el = new ElementFilter(filter);
		SparqlQueryBuilder.getElementGroup(query).addElementFilter(el);
	}

	public QueryColumnInfo addColumnToQuery(final ColumnName cName,
			final boolean optional) {
		cName.setUsedSegments(getSegments());
		final QueryColumnInfo columnInfo = infoSet.scanTablesForColumn(cName);
		if (columnInfo == null) {
			throw new IllegalArgumentException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_QUERY, cName));
		}
		return columnInfo;
	}

	public void addGroupBy(final Expr expr) {
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
	public void addOrderBy(final Expr expr, final boolean ascending) {
		if (LOG.isDebugEnabled()) {
			SparqlQueryBuilder.LOG.debug("Adding order by {} {}", expr,
					ascending ? "Ascending" : "Descending");
		}
		checkBuilt();
		query.addOrderBy(expr, ascending ? Query.ORDER_ASCENDING
				: Query.ORDER_DESCENDING);
	}

	public void addDefinedColumns() throws SQLDataException {
		infoSet.addDefinedColumns(columnsInUsing);
	}

	public QueryTableInfo addTable(final Table table, final TableName name,
			final boolean optional) {
		if (SparqlQueryBuilder.LOG.isDebugEnabled()) {
			SparqlQueryBuilder.LOG.debug("Adding table {} as {}",
					table.getSQLName(), name);
		}

		// make sure the table is in the query.
		QueryTableInfo tableInfo = infoSet.getTable(name);
		if (tableInfo == null) {
			tableInfo = new QueryTableInfo(infoSet, getElementGroup(), table,
					name, optional);
			infoSet.addTable(tableInfo);
		}
		return tableInfo;
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
	public QueryTableInfo addTable(final TableName name,
			final TableName asName, final boolean optional) throws SQLException {

		if (SparqlQueryBuilder.LOG.isDebugEnabled()) {
			SparqlQueryBuilder.LOG.debug(String.format(
					"Adding %s table %s as %s", optional ? "optional"
							: "required", name, asName));
		}
		checkBuilt();
		final Collection<Table> tables = findTables(name);

		if (tables.size() > 1) {
			throw new SQLException(String.format(
					SparqlQueryBuilder.FOUND_IN_MULTIPLE_ + " of catalog '%s'",
					name, "schemas", catalog.getName()));
		}
		if (tables.isEmpty()) {
			throw new SQLException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_ANY_ + " of catalog '%s'",
					name, "schema", catalog.getName()));
		}
		return addTable(tables.iterator().next(), asName, optional);
	}

	public void addTableColumns(final QueryTableInfo tableInfo) {
		tableInfo.addTableColumns(query, columnsInUsing);
	}

	public void addUnion(final List<SparqlQueryBuilder> unionBuilders)
			throws SQLDataException {
		final ElementUnion unionElement = new ElementUnion();
		for (final SparqlQueryBuilder sqb : unionBuilders) {
			unionElement.addElement(new ElementSubQuery(sqb.build()));
		}
		getElementGroup().addElement(unionElement);
	}

	/**
	 * Add the column name as a using across the tables in the current infoset.
	 * @param columnName the name to add
	 */
	public void addUsing(final String columnName) {
		if (LOG.isDebugEnabled()) {
			SparqlQueryBuilder.LOG.debug("adding Using {}", columnName);
		}
		final Collection<QueryTableInfo> tables = infoSet.getTables();
		if (tables.size() < 2) {
			throw new IllegalArgumentException(
					"There must be at least 2 tables in the query");
		}

		final Iterator<QueryTableInfo> iter = tables.iterator();

		final QueryTableInfo tableInfo = iter.next();
		ColumnName cName = tableInfo.getName().getColumnName(columnName);
		final QueryColumnInfo columnInfo = tableInfo.getColumn(cName, false);
		if (columnInfo == null) {
			throw new IllegalArgumentException(String.format(
					"column %s not found in %s", columnName,
					tableInfo.getSQLName()));
		}
		cName.setUsedSegments( NameSegments.FFFT);
		columnsInUsing.add(cName);
		while (iter.hasNext()) {
			final QueryTableInfo tableInfo2 = iter.next();
			cName = tableInfo2.getName().getColumnName(columnName);
			final QueryColumnInfo columnInfo2 = tableInfo2.getColumn(cName,
					false);
			if (columnInfo2 == null) {
				throw new IllegalArgumentException(String.format(
						"column %s not found in %s", columnInfo2,
						tableInfo2.getSQLName()));
			}
		}

	}

	/**
	 * Adds the the expression as a variable to the query. As a variable the
	 * result will be returned from the query.
	 *
	 * @param expr
	 *            The expression that defines the variable
	 * @param name
	 *            the alias for the expression, if null no alias is used.
	 */
	public void addVar(final Expr expr, final String name) {
		if (LOG.isDebugEnabled()) {
			SparqlQueryBuilder.LOG.debug("Adding Var {} as {}", expr, name);
		}
		checkBuilt();
		final NodeValue nv = expr.getConstant();
		if ((name != null) || (nv == null) || !nv.asNode().isVariable()) {
			final String s = StringUtils.defaultString(expr.getVarName());
			if (StringUtils.isNotEmpty(s)
					&& s.equals(StringUtils.defaultIfBlank(name, s))) {
				query.addResultVar(s);
			}
			else {
				if (name != null) {
					query.addResultVar(name, expr);
				}
				else {
					query.addResultVar(nv.asNode());
				}
			}
		}
		else {
			query.addResultVar(nv.asNode());
		}
		query.getResultVars();
	}

	/**
	 * Adds the the expression as a variable to the query. As a variable the
	 * result will be returned from the query.
	 *
	 * @param expr
	 *            The expression that defines the variable
	 * @param name
	 *            the alias for the expression.
	 */
	public void addVar(final Expr expr, final ColumnName name) {
		checkBuilt();
		final QueryColumnInfo columnInfo = infoSet.getColumn(name);
		if (!query.getProjectVars().contains(columnInfo.getGUIDVar())) {
			if (LOG.isDebugEnabled()) {
				SparqlQueryBuilder.LOG.debug("Adding {} as {}", expr, name);
			}
			query.addResultVar(columnInfo.getGUIDVar(), expr);
			query.getResultVars();
		}
	}

	/**
	 * Adds the the expression as a variable to the query. As a variable the
	 * result will be returned from the query.
	 *
	 * @param columnName
	 *            The column to add.
	 * @throws SQLException
	 */
	public void addVar(final ColumnName columnName) throws SQLException {
		if (LOG.isDebugEnabled()) {
			SparqlQueryBuilder.LOG.debug("Adding Var {}", columnName);
		}
		checkBuilt();

		final QueryColumnInfo columnInfo = addColumn(columnName, false);
		getTable(columnName.getTableName()).addDataFilter(columnInfo);
		query.addResultVar(columnInfo.getGUIDVar());
		query.getResultVars();
	}

	
	/**
	 * Get the SPARQL query.
	 *
	 * @return The constructed SPARQL query.
	 * @throws SQLDataException
	 */
	public Query build() throws SQLDataException {

		if (!isBuilt) {
			
		/* apply the type filters to each subpart.
		 may change variable names in result */
				for (final QueryTableInfo tableInfo : infoSet.getTables()) {
					try {
						tableInfo.addQueryFilters(infoSet);
					} catch (final SQLDataException e1) {
						throw new IllegalStateException(e1.getMessage(), e1);
					}
				}
			
		/* change the variable names in groupBy */
				nodeMapper = new NodeMapper();
				

			    // replace result values
				nodeMapper.map( query.getProject() );
				
				
				// replace Group by Expression values
				nodeMapper.map( query.getGroupBy() );
				
				// replace having expression
				nodeMapper.map( query.getHavingExprs() );			
				
				// replace orderby expressions 
				List<SortCondition> lsc = query.getOrderBy();
				if (lsc != null)
				{
					lsc.forEach( sc -> sc.expression = sc.expression.applyNodeTransform( nodeMapper ));
				}
				
				// fixup aggregators
				nodeMapper.map( query.getAggregators() );
			
				

			// renumber the Bnodes.
			final Element e = new BnodeRenumber().renumber(query.getQueryPattern());
			
			query.setQueryPattern(e);

			}
			isBuilt = true;
			if (LOG.isDebugEnabled()) {
				SparqlQueryBuilder.LOG.debug("Query parsed as {}", query);
			}
		
		return query;
	}

	private void checkBuilt() {
		if (isBuilt) {
			throw new IllegalStateException("Query was already built");
		}
	}

	private Collection<Table> findTables(final ItemName name) {

		if (SparqlQueryBuilder.LOG.isDebugEnabled()) {
			SparqlQueryBuilder.LOG.debug(String.format(
					"Looking for Table %s.%s in '%s' catalog",
					name.getSchema(), name.getTable(), catalog.getName()));
		}

		final List<Table> tables = new ArrayList<Table>();

		for (final Schema schema : catalog.findSchemas(name.getSchema())) {
			for (final Table table : schema.findTables(name.getTable())) {
				tables.add(table);
			}
		}
		return tables;
	}

	/**
	 * Get the catalog this builder is working with.
	 *
	 * @return a Catalog.
	 */
	public Catalog getCatalog() {
		return catalog;
	}

	public String getCatalogName() {
		return catalog.getShortName();
	}

	public Catalog getCatalog(final String catalog) {
		return catalogs.get(catalog);
	}

	public QueryColumnInfo getColumn(final ColumnName cName) {
		return infoSet.getColumn(cName);
	}

	public QueryColumnInfo getColumn(final int i) {
		if (!isBuilt) {
			throw new IllegalStateException(
					"Columns may not be retrieved from a builder until after query is built");
		}

		final Var v = query.getProjectVars().get(i);
		return getColumn(v);
	}

	public int getColumnCount() {
		if (!isBuilt) {
			throw new IllegalStateException(
					"Column count may not be retrieved from a builder until after query is built");
		}

		return query.getProjectVars().size();
	}

	public int getColumnIndex(final ColumnName columnName) {
		
		return infoSet.getColumnIndex(columnName);
	}

	private ElementGroup getElementGroup() {
		return SparqlQueryBuilder.getElementGroup(query);
	}

	public QueryColumnInfo getColumn(final Var v) {
		if (isBuilt)
		{
			return nodeMapper.getColumn( v );
		}
		return infoSet.findColumnByGUID(v);
	}

	public QueryTableInfo getTable(final Var v) {
		checkBuilt();
		final int segs = v.getName().split(NameUtils.SPARQL_DOT).length;
		if (segs > 3) {
			throw new IllegalArgumentException(
					"Name may not have more than 3 segments");
		}
		final TableName tName = TableName.getNameInstance("", "", v.getName());
		tName.setUsedSegments(NameSegments.getInstance(segs == 3, segs >= 2,
				true, false));
		return infoSet.getTable(tName);
	}

	/**
	 * Get the list of query columns for the variables in the project vars.  If the var is not found
	 * in the query info then null is returned.  Such values are calculated in a return function.
	 * @return
	 */
	public List<QueryColumnInfo> getResultColumns() {
		final List<QueryColumnInfo> retval = new ArrayList<QueryColumnInfo>();
		for (final Var v : query.getProjectVars()) {
			retval.add(getColumn(v));
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
	public QueryTableInfo getTable(final TableName name) {
		return infoSet.getTable(name);
	}

	/**
	 * Register an aggregator.
	 * 
	 * The aggregator will be registered as a column with in the virtual table for the 
	 * virtual schema of the current catalog.
	 * 
	 * @param agg The aggregator to register.
	 * @param type they SQL data type of the aggregator result.
	 * @param alias the alias for the aggregator.
	 * @return the aggregator function.
	 * @throws SQLException
	 */
	public ExprAggregator register(final Aggregator agg, final int type,
			final String alias) throws SQLException {
		final ExprAggregator expr = (ExprAggregator) query.allocAggregate(agg);

		final ColumnName columnName = ColumnName.getNameInstance(
				catalog.getShortName(), VirtualSchema.NAME, VirtualTable.NAME,
				alias);
		if (infoSet.findColumn(columnName) == null) {
			registerFunctionColumn(columnName, type);
		}
		return expr;
	}

	/**
	 * Register a function with the infoSet.
	 * @param funcName the name of the function.
	 * @param type the SQL return type for the function
	 * @return the QueryColumnInfo for the function.
	 */
	public QueryColumnInfo registerFunction(final ColumnName funcName,
			final int type) {
		QueryColumnInfo retval = infoSet.findColumn(funcName);
		if (retval == null) {
			retval = registerFunctionColumn(funcName, type);
		}
		return retval;
	}

	public QueryColumnInfo registerFunctionColumn(
			final ColumnName columnName, final int type) {

		QueryTableInfo tableInfo = infoSet.getTable(columnName.getTableName());
		Column column = null;
		if (tableInfo == null) {
			FQName fqName = columnName.getFQName();
			final Catalog cat = getCatalog(VirtualCatalog.NAME);
			Schema schema = cat.getSchema(fqName.getSchema());
			if (schema == null) {
				schema = new VirtualSchema(cat, fqName.getSchema());
			}
			Table table = schema.getTable(fqName.getTable());
			if (table == null) {
				table = new VirtualTable(schema, fqName.getTable());
			}
			column = new FunctionColumn(table, fqName.getColumn(), type);
			// new QueryTableInfo adds table to infoSet
			tableInfo = new QueryTableInfo(infoSet, null, table,
					columnName.getTableName(), false);
			infoSet.addTable(tableInfo);
		}
		else {
			column = new FunctionColumn(tableInfo.getTable(),
					columnName.getShortName(), type);
		}
		final QueryColumnInfo columnInfo = new QueryColumnInfo(column,
				columnName, false);
		infoSet.addColumn(columnInfo);
		return columnInfo;
	}

	/**
	 * Sets all the columns for all the tables currently defined. This method
	 * should be called after all tables have been added to the querybuilder.
	 *
	 * @throws SQLException
	 */
	public void setAllColumns() throws SQLException {
		if (LOG.isDebugEnabled()) {
			SparqlQueryBuilder.LOG.debug("Setting All Columns");
		}
		checkBuilt();

		final Collection<QueryTableInfo> tableInfos = infoSet.getTables();
		if (tableInfos.size() == 0) {
			throw new IllegalArgumentException(
					"There must be a least one table");
		}
		final QueryItemCollection<QueryColumnInfo, Column, ColumnName> colInfoList = new QueryItemCollection<QueryColumnInfo, Column, ColumnName>();
		for (final QueryTableInfo tableInfo : tableInfos) {
			final Iterator<Column> iter = tableInfo.getTable().getColumns();
			while (iter.hasNext()) {
				final Column col = iter.next();
				final ColumnName cName = tableInfo.getName().getColumnName(
						col.getName().getShortName());
				final QueryColumnInfo columnInfo = tableInfo.getColumn(cName,
						col.isOptional());
				colInfoList.add(columnInfo);
				tableInfo.addDataFilter(columnInfo);
			}
		}

		// find shortest name without name collision. skipping columns in using.
		NameSegments segs = NameSegments.FFFT;
		for (final QueryColumnInfo columnInfo : colInfoList) {
			if (!columnsInUsing.contains(columnInfo.getName().getShortName())) {
				//SearchName sn = new SearchName(columnInfo.getName(), segs);
				while ((colInfoList.count(columnInfo.getName(), segs) > 1)
						&& !segs.isCatalog()) {
					if (segs.isSchema()) {
						segs = NameSegments.ALL;
					}
					else if (segs.isTable()) {
						segs = NameSegments.FTTT;
					}
					else {
						segs = NameSegments.FFTT;
					}
					//sn = new SearchName(columnInfo.getName(), segs);
				}
			}
		}

		// remove the variables
		for (final Var v : query.getProjectVars()) {
			colInfoList.remove(v.getName());			
		}
		// anything left needs to be added

		for (final QueryColumnInfo columnInfo : colInfoList) {
			query.addResultVar(columnInfo.getGUIDVar());
		}
	}

	/**
	 * Sets the distinct flag for the SPARQL query.
	 */
	public void setDistinct() {
		if (LOG.isDebugEnabled()) {
			SparqlQueryBuilder.LOG.debug("Setting Distinct");
		}
		checkBuilt();
		query.setDistinct(true);
	}

	public void setHaving(final Expr expr) {
		query.addHavingCondition(expr);
	}

	public SparqlQueryBuilder setKey(final Key<?> key) {
		if (LOG.isDebugEnabled()) {
			SparqlQueryBuilder.LOG.debug("Setting key {}", key);
		}
		if (key != null) {
			setOrderBy(key);
			if (key.isUnique()) {
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
	public void setLimit(final Long limit) {
		if (LOG.isDebugEnabled()) {
			SparqlQueryBuilder.LOG.debug("Setting limit {}", limit);
		}
		checkBuilt();
		query.setLimit(limit);
	}

	/**
	 * Set the offset for the SPARQL query.
	 *
	 * @param offset
	 *            The number of rows to skip over.
	 */
	public void setOffset(final Long offset) {
		if (LOG.isDebugEnabled()) {
			SparqlQueryBuilder.LOG.debug("Setting Offset {}", offset);
		}
		checkBuilt();
		query.setOffset(offset);
	}

	public void setOrderBy(final Key<?> key) {
		if (LOG.isDebugEnabled()) {
			SparqlQueryBuilder.LOG.debug("Setting orderBy {}", key);
		}
		final List<Var> vars = query.getProjectVars();
		for (final KeySegment seg : key.getSegments()) {
			query.addOrderBy(vars.get(seg.getIdx()),
					seg.isAscending() ? Query.ORDER_ASCENDING
							: Query.ORDER_DESCENDING);
		}
	}

	/**
	 * Return the SPARQL query as the string value for the builder.
	 */
	@Override
	public String toString() {
		return String.format("QueryBuilder%s[%s]", (isBuilt ? "(builtargs)"
				: ""), query.toString());
	}

	public Schema getDefaultSchema() {
		return schema;
	}

	public String getDefaultSchemaName() {
		return schema.getName().getShortName();
	}

	/**
	 * Returns the table name if only one exists in the query. If multiple
	 * tables exist or no table has been added return null.
	 *
	 * @return The default table name.
	 */
	public String getDefaultTableName() {
		final Iterator<QueryTableInfo> iter = infoSet.getTables().iterator();
		if (!iter.hasNext()) {
			return null;
		}
		final QueryTableInfo table = iter.next();
		if (iter.hasNext()) {
			return null;
		}
		return table.getName().getShortName();
	}

	/**
	 * Set the number of segments necessary to display the results properly.
	 */
	public void setSegmentCount() {
		infoSet.setMinimumColumnSegments();
		for (final ColumnName columnName : columnsInUsing) {
			for (final QueryColumnInfo columnInfo : infoSet.listColumns(columnName)) {
				columnInfo.setSegments(columnName.getUsedSegments());
			}
		}
	}

	public NameSegments getSegments() {
		return infoSet.getSegments();
	}
	
	private class NodeMapper implements NodeTransform
	{
		Map<Node,QueryColumnInfo> vars = new HashMap<Node,QueryColumnInfo>();
		
		NodeMapper()
		{
			VarExprList varExpLst = query.getProject();
			for (Var v : varExpLst.getVars() )
			{
				vars.put( v.asNode(), infoSet.findColumnByGUID( v ));
				Expr exp = varExpLst.getExpr(v);
				if (exp != null)
				{
					exp.applyNodeTransform( new NodeTransform( ) {

						@Override
						public Node apply(Node t) {
							if (t.isVariable())
							{
								QueryColumnInfo qci = infoSet.findColumnByGUID( Var.alloc( t.getName() ));
								if (qci == null)
								{
									LOG.info( String.format("Can not find %s in infoSet columns -- will not map it", t));
								} else {
									vars.put( t,  qci );
								}
							}
							return t;
						}});
				}
			}
		}
	
		/*
		 * get the column info for a column named var.
		 */
		private QueryColumnInfo getColumn( Var v )
		{
			return vars.values().stream().filter( qci -> qci.getVar().equals( v )).findFirst().orElse(null);
		}

		public Var transform(Var t) {
			QueryColumnInfo retval = vars.get(t.asNode());
			return retval==null?t:retval.getVar();
		}
		
		@Override
		public Node apply(Node t) {
			QueryColumnInfo retval = vars.get(t);
			return retval==null?t:retval.getVar().asNode();
		}


		public void map(VarExprList vel) {
			for (int i=0;i<vel.getVars().size();i++)
			{
				Var v = vel.getVars().get(i);
				Var r = nodeMapper.transform( v );
				if (!v.equals(r))
				{
					vel.getVars().set( i, r );
					if (vel.getExprs().containsKey( v ))
					{
						Expr exp = vel.getExprs().get(v);
						exp = exp.applyNodeTransform(nodeMapper);
						vel.getExprs().put( r, exp);
						vel.getExprs().remove( v );
					}
				}
			}

		}
		
		public <T extends Expr> void map(List<T> exprLst) {
			for (int i=0;i<exprLst.size();i++)
			{
				exprLst.set(i, (T)exprLst.get(i).applyNodeTransform(this));
			}
		}
		
	}
}

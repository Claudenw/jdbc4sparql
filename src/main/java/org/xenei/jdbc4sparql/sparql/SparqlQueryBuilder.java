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
import com.hp.hpl.jena.sparql.expr.E_LogicalAnd;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.expr.aggregate.Aggregator;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementBind;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementService;
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;
import com.hp.hpl.jena.sparql.syntax.ElementUnion;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Filter;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.iface.name.SearchName;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfKey;
import org.xenei.jdbc4sparql.impl.rdf.RdfKeySegment;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef;
import org.xenei.jdbc4sparql.impl.virtual.VirtualCatalog;
import org.xenei.jdbc4sparql.impl.virtual.VirtualSchema;
import org.xenei.jdbc4sparql.impl.virtual.VirtualTable;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.items.QueryItemCollection;
import org.xenei.jdbc4sparql.sparql.items.QueryTableInfo;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.FunctionColumn;

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
		} else if (e instanceof ElementGroup) {
			retval = (ElementGroup) e;
		} else {
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
		this.catalogs = catalogs;
		this.parser = parser;
		this.catalog = catalog;
		this.schema = schema;
		this.query = new Query();
		this.isBuilt = false;
		this.infoSet = new QueryInfoSet();
		this.columnsInUsing = new ArrayList<String>();
		this.infoSet.setUseGUID(catalog.isService());
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
		this.infoSet.setUseGUID(parent.infoSet.useGUID());
	}

	/**
	 * Set the use GUID flag;
	 * 
	 * @param state
	 */
	public void setUseGUID(boolean state) {
		this.infoSet.setUseGUID(state);
	}

	public void addAlias(ColumnName orig, ColumnName alias) throws SQLDataException {
		QueryTableInfo tableInfo =infoSet.getTable( orig.getTableName() );
		if (tableInfo == null)
		{
			throw new IllegalArgumentException(String.format(
				SparqlQueryBuilder.NOT_FOUND_IN_QUERY, orig.getTableName()));
		}
		
		// find the column will check infoset for existing column first.
		QueryColumnInfo columnInfo = tableInfo.getColumn(infoSet, orig);
		if (columnInfo == null)
		{
			// ok now we have to look for column with the proper name.
			Column column = tableInfo.getTable().getColumn( orig.getShortName());
			if (column == null)
			{
				throw new IllegalArgumentException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_QUERY, orig));
			}
			columnInfo = tableInfo.addColumnToQuery(column);
		}
		//QueryTableInfo tableInfo = infoSet.getTable( orig.getTableName());
		tableInfo.addDataFilter(columnInfo);
		infoSet.addColumn( columnInfo );
		
		QueryColumnInfo aliasInfo = columnInfo.createAlias(alias);
		tableInfo.addDataFilter(aliasInfo);
		infoSet.addColumn(aliasInfo);
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
			} else {
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
		if (LOG.isDebugEnabled())
			SparqlQueryBuilder.LOG.debug("adding Filter: {}", filter);
		checkBuilt();
		ElementFilter el = new ElementFilter(filter);
		SparqlQueryBuilder.getElementGroup(query).addElementFilter(el);
	}
	
	

	public QueryColumnInfo addColumnToQuery( ColumnName cName, boolean optional  )
	{	
		cName.setUsedSegments(getSegments());
		QueryColumnInfo columnInfo = infoSet.scanTablesForColumn(cName);
		if (columnInfo == null)
		{
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
		if (LOG.isDebugEnabled())
			SparqlQueryBuilder.LOG.debug("Adding order by {} {}", expr,
				ascending ? "Ascending" : "Descending");
		checkBuilt();
		query.addOrderBy(expr, ascending ? Query.ORDER_ASCENDING
				: Query.ORDER_DESCENDING);
	}

	public void addRequiredColumns() {
		infoSet.addRequiredColumns();
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
		tableInfo.addTableColumns(infoSet, query);
	}

	public void addUnion(final List<SparqlQueryBuilder> unionBuilders)
			throws SQLDataException {
		final ElementUnion unionElement = new ElementUnion();
		for (final SparqlQueryBuilder sqb : unionBuilders) {
			unionElement.addElement(new ElementSubQuery(sqb.build()));
		}
		getElementGroup().addElement(unionElement);
	}

	public void addUsing(final String columnName) {
		if (LOG.isDebugEnabled())
			SparqlQueryBuilder.LOG.debug("adding Using {}", columnName);
		final Collection<QueryTableInfo> tables = infoSet.getTables();
		if (tables.size() < 2) {
			throw new IllegalArgumentException(
					"There must be at least 2 tables in the query");
		}

		Iterator<QueryTableInfo> iter = tables.iterator();

		final QueryTableInfo tableInfo = iter.next();
		ColumnName cName = tableInfo.getName().getColumnName(columnName);
		final QueryColumnInfo columnInfo = tableInfo.getColumn(infoSet, cName,
				false);
		if (columnInfo == null) {
			throw new IllegalArgumentException(String.format(
					"column %s not found in %s", columnName,
					tableInfo.getSQLName()));
		}
		columnsInUsing.add(columnName);
		while (iter.hasNext()) {
			final QueryTableInfo tableInfo2 = iter.next();
			cName = tableInfo2.getName().getColumnName(columnName);
			final QueryColumnInfo columnInfo2 = tableInfo2.getColumn(infoSet,
					cName, false);
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
		if (LOG.isDebugEnabled())
			SparqlQueryBuilder.LOG.debug("Adding Var {} as {}", expr, name);
		checkBuilt();
		final NodeValue nv = expr.getConstant();
		if ((name != null) || (nv == null) || !nv.asNode().isVariable()) {
			final String s = StringUtils.defaultString(expr.getVarName());
			if (StringUtils.isNotEmpty(s)
					&& s.equals(StringUtils.defaultIfBlank(name, s))) {
				query.addResultVar(s);
			} else {
				if (name != null) {
					query.addResultVar(name, expr);
				} else {
					query.addResultVar(nv.asNode());
				}
			}
		} else {
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
		QueryColumnInfo columnInfo = infoSet.getColumn(name);
		if (!query.getProjectVars().contains(columnInfo.getVar())) {
			if (LOG.isDebugEnabled())
				SparqlQueryBuilder.LOG.debug("Adding {} as {}", expr, name);
			query.addResultVar(columnInfo.getVar(), expr);
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
	public void addVar(ColumnName columnName) throws SQLException {
		if (LOG.isDebugEnabled())
			SparqlQueryBuilder.LOG.debug("Adding Var {}", columnName);
		checkBuilt();

		QueryColumnInfo qci = addColumn(columnName, false);
		query.addResultVar(qci.getVar());
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
			if (!catalog.isService()) {
				// apply the type filters to each subpart.
				for (final QueryTableInfo tableInfo : infoSet.getTables()) {
					try {
						tableInfo.addQueryFilters(infoSet);
					} catch (SQLDataException e1) {
						throw new IllegalStateException(e1.getMessage(), e1);
					}
				}
			}

			// renumber the Bnodes.
			final Element e = new BnodeRenumber().renumber(query
					.getQueryPattern());
			query.setQueryPattern(e);

			if (catalog.isService()) {

				// create a copy of the query so that we can verify that it is
				// good.
				Query serviceCall = query.cloneQuery();
				final VarExprList vars = serviceCall.getProject();

				final ElementService service = new ElementService(
						catalog.getServiceNode(), new ElementSubQuery(
								serviceCall), false);
				Query newResult = new Query();
				newResult.setQuerySelectType();
				ElementGroup filterGroup = SparqlQueryBuilder
						.getElementGroup(newResult);
				filterGroup.addElement(service);
				ElementGroup typeGroup = new ElementGroup();
				typeGroup.addElement(filterGroup);
				infoSet.setUseGUID(false); // we are now building complete set.
				// create the service call
				// make sure we project all vars for the filters.

				for (Var v : vars.getVars()) {
					
					//QueryColumnInfo colInfo = infoSet.findColumnByGUID(
					//		v.getName());
					//newResult.addResultVar(colInfo.getVar());
					newResult.addResultVar(v);
				}

				// add the columns to the query.
				for (QueryTableInfo tableInfo : infoSet.getTables()) {
					Collection<QueryColumnInfo> typeFilters = infoSet
							.iterateColumns(tableInfo.getName())
							.filterDrop(new Filter<QueryColumnInfo>() {

								@Override
								public boolean accept(QueryColumnInfo o) {
									return columnsInUsing.contains(o.getName()
											.getShortName());
								}
							}).toList();
					try {
						QueryTableInfo.addTypeFilters(typeFilters, typeFilters,
								filterGroup, typeGroup);
					} catch (SQLDataException e1) {
						throw new IllegalStateException(e1.getMessage(), e1);
					}
					for (QueryColumnInfo columnInfo : typeFilters) {
						if (!serviceCall.getProjectVars().contains(
								columnInfo.getGUIDVar())) {
							serviceCall
									.addProjectVars(Arrays
											.asList(new Var[] { columnInfo
													.getGUIDVar() }));
						}
					}
				}

				for (String columnName : columnsInUsing) {
					QueryColumnInfo first = null;
					Expr expr = null;

					for (QueryColumnInfo columnInfo : infoSet
							.listColumns(new SearchName(null, null, null,
									columnName))) {
						CheckTypeF ctf = columnInfo.getTypeFilter();
						if (LOG.isDebugEnabled())
							LOG.debug("Adding filter: {}", ctf);
						filterGroup.addElementFilter(new ElementFilter(ctf));

						if (first == null) {
							ForceTypeF ftf = columnInfo.getDataFilter();
							ElementBind bind = ftf.getBinding( columnInfo );
							if (LOG.isDebugEnabled())
								LOG.debug("Adding binding: {}", bind);
							typeGroup.addElement(bind);
							first = columnInfo;
						} else {
							E_Equals eq = new E_Equals(first.getDataFilter(),
									columnInfo.getDataFilter());
							if (expr == null) {
								expr = eq;
							} else {
								expr = new E_LogicalAnd(expr, eq);
							}
						}

					}
					if (expr != null) {
						if (LOG.isDebugEnabled())
							LOG.debug("Adding filter: {}", expr);
						filterGroup.addElementFilter(new ElementFilter(expr));
					}
				}

				newResult.setQueryPattern(typeGroup);
				query = newResult;
			}
			isBuilt = true;
			if (LOG.isDebugEnabled())
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

	public int getColumnIndex(final String columnName) {
		final ColumnName cName = ColumnName.getNameInstance(
				catalog.getShortName(), schema.getName().getShortName(), null,
				columnName);
		return infoSet.getColumnIndex(cName);
	}

	private ElementGroup getElementGroup() {
		return SparqlQueryBuilder.getElementGroup(query);
	}
	
	private ColumnName createColumnName( Var v )
	{
		int segs = v.getName().split( NameUtils.SPARQL_DOT).length;
		if (segs>4)
		{
			throw new IllegalArgumentException( "Name may not have more than 4 segments");
		}
		ColumnName cName = ColumnName.getNameInstance("", "", "", v.getName());
		cName.setUsedSegments(NameSegments.getInstance( segs==4, segs>=3, segs>=2, true ));
		return cName;
	}

	public QueryColumnInfo getColumn(final Var v) {
		return infoSet.getColumn( createColumnName( v ) );
	}

	public QueryTableInfo getTable(final Var v) {
		checkBuilt();
		int segs = v.getName().split( NameUtils.SPARQL_DOT).length;
		if (segs>3)
		{
			throw new IllegalArgumentException( "Name may not have more than 3 segments");
		}
		TableName tName = TableName.getNameInstance("", "", v.getName());
		tName.setUsedSegments(NameSegments.getInstance( segs==3, segs>=2, true, false ));
		return infoSet.getTable( tName );
	}

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

	public ExprAggregator register(final Aggregator agg, final int type)
			throws SQLException {
		final ExprAggregator expr = (ExprAggregator) query.allocAggregate(agg);

		final ColumnName columnName = ColumnName.getNameInstance(
				VirtualCatalog.NAME, VirtualSchema.NAME, VirtualTable.NAME,
				expr.getAggVar().getVarName());
		if (infoSet.findColumn(columnName) == null) {
			registerFunctionColumn(columnName, type);
		}
		return expr;
	}

	public QueryColumnInfo registerFunction(ColumnName funcName, final int type)
			throws SQLException {
		QueryColumnInfo retval = infoSet.findColumn(funcName);
		if (retval == null) {
			retval = registerFunctionColumn(funcName, type);
		}
		return retval;
	}

	public QueryColumnInfo registerFunctionColumn(ColumnName columnNameArg,
			final int type) {
		ColumnName columnName = new ColumnName(StringUtils.defaultString(
				columnNameArg.getCatalog(), VirtualCatalog.NAME),
				StringUtils.defaultString(columnNameArg.getSchema(),
						VirtualSchema.NAME), StringUtils.defaultString(
						columnNameArg.getTable(), VirtualTable.NAME),
				columnNameArg.getShortName());
		QueryTableInfo tableInfo = infoSet.getTable(columnName.getTableName());
		Column column = null;
		if (tableInfo == null) {
			final Catalog cat = getCatalog(VirtualCatalog.NAME);
			Schema schema = cat.getSchema(columnName.getSchema());
			if (schema == null) {
				schema = new VirtualSchema(cat, columnName.getSchema());
			}
			Table table = schema.getTable(columnName.getTable());
			if (table == null) {
				table = new VirtualTable(schema, columnName.getTable());
			}
			column = new FunctionColumn(table, columnName.getShortName(), type);
			// new QueryTableInfo adds table to infoSet
			tableInfo = new QueryTableInfo(infoSet, null, table,
					columnName.getTableName(), false);
			infoSet.addTable(tableInfo);
		} else {
			column = new FunctionColumn(tableInfo.getTable(),
					columnName.getShortName(), type);
		}
		QueryColumnInfo columnInfo = new QueryColumnInfo(column, columnName,
				false);
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
		if (LOG.isDebugEnabled())
			SparqlQueryBuilder.LOG.debug("Setting All Columns");
		checkBuilt();

		final Collection<QueryTableInfo> tableInfos = infoSet.getTables();
		if (tableInfos.size() == 0) {
			throw new IllegalArgumentException(
					"There must be a least one table");
		}
		QueryItemCollection<QueryColumnInfo,Column,ColumnName> colInfoList = new QueryItemCollection<QueryColumnInfo,Column,ColumnName>();
		for (final QueryTableInfo tableInfo : tableInfos) {
			final Iterator<Column> iter = tableInfo.getTable().getColumns();
			while (iter.hasNext()) {
				final Column col = iter.next();
				final ColumnName cName = tableInfo.getName().getColumnName(
						col.getName().getShortName());
				final QueryColumnInfo columnInfo = tableInfo.getColumn(infoSet,
						cName, col.isOptional());
				colInfoList.add(columnInfo);
				tableInfo.addDataFilter(columnInfo);
			}
		}

		// find shortest name without name collision. skipping columns in using.
		NameSegments segs = NameSegments.getInstance(false, false, false, true);
		for (QueryColumnInfo columnInfo : colInfoList) {
			if (!columnsInUsing.contains(columnInfo.getName().getShortName())) {
				SearchName sn = new SearchName(columnInfo.getName(), segs);
				while (colInfoList.count(sn) > 1
						&& !sn.getUsedSegments().isCatalog()) {
					if (segs.isSchema()) {
						segs = NameSegments.getInstance(true, true, true, true);
					} else if (segs.isTable()) {
						segs = NameSegments.getInstance(false, true, true, true);
					} else {
						segs = NameSegments.getInstance(false, false, true, true);
					}
					sn = new SearchName(columnInfo.getName(), segs);
				}
			}
		}

		// remove the variables 
		for (Var v : query.getProjectVars())
		{
			for (QueryColumnInfo columnInfo : colInfoList.match( createColumnName(v) ).toList())
			{
				colInfoList.remove( columnInfo );
			}
		}
		// anything left needs to be added
		
		for (QueryColumnInfo columnInfo : colInfoList) {
			query.addResultVar( columnInfo.getVar() );
		}
	}

	/**
	 * Sets the distinct flag for the SPARQL query.
	 */
	public void setDistinct() {
		if (LOG.isDebugEnabled())
			SparqlQueryBuilder.LOG.debug("Setting Distinct");
		checkBuilt();
		query.setDistinct(true);
	}

	public void setHaving(final Expr expr) {
		query.addHavingCondition(expr);
	}

	public SparqlQueryBuilder setKey(final Key<?> key) {
		if (LOG.isDebugEnabled())
			SparqlQueryBuilder.LOG.debug("Setting key {}", key);
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
		if (LOG.isDebugEnabled())
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
	public void setOffset(final Long offset) {
		if (LOG.isDebugEnabled())
			SparqlQueryBuilder.LOG.debug("Setting Offset {}", offset);
		checkBuilt();
		query.setOffset(offset);
	}

	public void setOrderBy(final Key<?> key) {
		if (LOG.isDebugEnabled())
			SparqlQueryBuilder.LOG.debug("Setting orderBy {}", key);
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
	 * Returns the table name if only one exists in the query. If multiple tables exist or no
	 * table has been added return null.
	 * @return The default table name.
	 */
	public String getDefaultTableName() {
		Iterator<QueryTableInfo> iter = infoSet.getTables().iterator();
		if (!iter.hasNext()) {
			return null;
		}
		QueryTableInfo table = iter.next();
		if (iter.hasNext()) {
			return null;
		}
		return table.getName().getShortName();
	}

	public void setSegmentCount() {
		infoSet.setMinimumColumnSegments();
		for (String columnName : columnsInUsing) {
			SearchName sn = new SearchName(null, null, null, columnName);
			for (QueryColumnInfo columnInfo : infoSet.listColumns(sn)) {
				columnInfo.setSegments(sn.getUsedSegments());
			}
		}
	}

	public NameSegments getSegments() {
		return infoSet.getSegments();
	}
}

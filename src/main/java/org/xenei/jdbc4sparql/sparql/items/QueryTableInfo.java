package org.xenei.jdbc4sparql.sparql.items;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.sparql.CheckTypeF;
import org.xenei.jdbc4sparql.sparql.ForceTypeF;
import org.xenei.jdbc4sparql.sparql.QueryInfoSet;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.E_Function;
import com.hp.hpl.jena.sparql.expr.E_LogicalAnd;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.lang.sparql_11.ParseException;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementBind;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;

/**
 * Contains the information for the table in the query. If a defined table is
 * used multiple times with different aliases there will be multiple
 * QueryTableInfo instances with the same RdfTable but different names.
 */
public class QueryTableInfo extends QueryItemInfo<Table, TableName> {

	private final ElementGroup eg;
	private final ElementGroup egWrapper;
	private final QueryInfoSet infoSet;

	// list of bindings to add at the end of the query
	private final Set<QueryColumnInfo> dataFilterList;

	private final Set<Element> joinElementList;
	
	private static Logger LOG = LoggerFactory.getLogger(QueryTableInfo.class);

	/**
	 * Verify that the table object is not null.
	 *
	 * @param table
	 *            The table to check
	 * @return the table param
	 * @throws IllegalArgumentException
	 */
	private static Table checkTable(final Table table)
			throws IllegalArgumentException {
		if (table == null) {
			throw new IllegalArgumentException("table may not be null");
		}
		return table;
	}

	/**
	 * Create QueryTableInfo.
	 *
	 * @param infoSet
	 *            The QueryInfoSet to add the table to.
	 * @param queryElementGroup
	 *            The queryElementGroup that the table info will be added to.
	 * @param table
	 *            The table that this QueyrTableInfo wraps.
	 * @param optional
	 *            true if this table is optional.
	 * @throws IllegalArgumentException
	 */
	public QueryTableInfo(final QueryInfoSet infoSet,
			final ElementGroup queryElementGroup, final Table table,
			final boolean optional) throws IllegalArgumentException {
		this(infoSet, queryElementGroup, table, checkTable(table).getName(),
				optional);
	}

	/**
	 * Constructor
	 *
	 * @param infoSet
	 *            The QueryInfoSet to add the table to.
	 * @param queryElementGroup
	 *            The queryElementGroup that the table info will be added to.
	 * @param table
	 *            The table that this QueyrTableInfo wraps.
	 * @param alias
	 *            The table name alias.
	 * @param optional
	 *            true if this table is optional.
	 * @throws IllegalArgumentException
	 */
	public QueryTableInfo(final QueryInfoSet infoSet,
			final ElementGroup queryElementGroup, final Table table,
			final TableName alias, final boolean optional)
					throws IllegalArgumentException {
		super(table, alias, optional);
		this.infoSet = infoSet;
		// this.table = checkTable(table);
		this.egWrapper = new ElementGroup();
		this.eg = new ElementGroup();
		egWrapper.addElement(eg);
		this.dataFilterList = new HashSet<QueryColumnInfo>();
		this.joinElementList = new HashSet<Element>();

		if (queryElementGroup == null) {
			if (LOG.isDebugEnabled()) {
				QueryTableInfo.LOG.debug("marking {} as not in query.", this);
			}
		}
		else {
			if (optional) {
				if (LOG.isDebugEnabled()) {
					QueryTableInfo.LOG.debug("marking {} as optional.", this);
				}
				queryElementGroup.addElement(new ElementOptional(egWrapper));
			}
			else {
				if (LOG.isDebugEnabled()) {
					QueryTableInfo.LOG.debug("marking {} as required.", this);
				}
				queryElementGroup.addElement(egWrapper);
			}
		}
	}

	/**
	 * Set the namesegments. The name segments will have the table set true and
	 * the column name false.
	 */
	@Override
	public void setSegments(final NameSegments usedSegments) {
		final NameSegments ourSegments = NameSegments.getInstance(
				usedSegments.isCatalog(), usedSegments.isSchema(), true, false);

		super.setSegments(ourSegments);
		final List<QueryColumnInfo> lst = infoSet.listColumns(getName());
		for (final QueryColumnInfo col : lst) {
			col.setSegments(usedSegments);
		}
	}

	/**
	 * Adds the column to the columns defined in the query.
	 *
	 * @param column
	 *            The column to add.
	 * @return The variable Node for the column.
	 */
	public QueryColumnInfo addColumnToQuery(final Column column) {
		return addColumnToQuery(column, column.getName(), column.isOptional());
	}

	/**
	 * Adds the column to the columns defined in the query. Result will be
	 * optional if the Column is optional.
	 *
	 * @param column
	 *            The column to add
	 * @param cName
	 *            The name for the column in the query.
	 * @param optional
	 *            if true the column is optional.
	 * @return The variable Node for the column.
	 */
	public QueryColumnInfo addColumnToQuery(final Column column,
			final ColumnName cName, final boolean optional) {

		QueryColumnInfo columnInfo = new QueryColumnInfo(column, getName()
				.getColumnName(column.getName().getShortName()), optional);

		// see if this is an alias
		if (!column.getName().getGUID().equals(cName.getGUID())) {
			columnInfo = columnInfo.createAlias(cName);
		}

		if (LOG.isDebugEnabled()) {
			QueryTableInfo.LOG.debug("Adding column: {} as {} ({})",
					columnInfo, columnInfo.getGUIDVar(),
					columnInfo.isOptional() ? "optional" : "required");
		}

		// only add to group if it was not already in the column list.
		if (infoSet.addColumn(columnInfo) && column.hasQuerySegments()) {
			if (!columnInfo.isAlias()) {
				Element el = getQuerySegments(columnInfo.getColumn(),
						getGUIDVar(), columnInfo.getGUIDVar());
				if ((el instanceof ElementGroup)
						&& (((ElementGroup) el).getElements().size() == 1)) {
					final ElementGroup subGroup = (ElementGroup) el;
					el = subGroup.getElements().get(0);
				}
				if (optional) {
					eg.addElement(new ElementOptional(el));
				}
				else {
					eg.addElement(el);
				}
			}
		}
		return columnInfo;
	}

	/**
	 * Adds the column to the columns defined in the query. Result will be
	 * optional if the Column is optional.
	 *
	 * @param column
	 *            The column to add
	 * @param cName
	 *            The name for the column in the query.
	 * @return The variable Node for the column.
	 */
	public QueryColumnInfo addColumnToQuery(final Column column,
			final ColumnName cName) {
		return addColumnToQuery(column, cName, column.isOptional());
	}

	/**
	 * Adds the column to the columns defined in the query.
	 *
	 * @param column
	 *            The column to add
	 * @param optional
	 *            if True the column is optional
	 * @return The variable Node for the column.
	 */
	public QueryColumnInfo addColumnToQuery(final Column column,
			final boolean optional) {
		return addColumnToQuery(column, column.getName(), optional);

	}

	/**
	 * Add the column to the datafilter.
	 *
	 * @param columnInfo
	 *            The columninfo to add.
	 */
	public void addDataFilter(final QueryColumnInfo columnInfo) {
		if (columnInfo == null) {
			throw new IllegalArgumentException("ColumnInfo may not be null");
		}
		dataFilterList.add(columnInfo);
	}

	/**
	 * Add a filter to this table.
	 *
	 * @param expr
	 *            The expression to add.
	 */
	public void addFilter(final Expr expr) {
		if (LOG.isDebugEnabled()) {
			QueryTableInfo.LOG.debug("Adding filter: {}", expr);
		}
		egWrapper.addElementFilter(new ElementFilter(expr));

	}
	
	/**
	 * Add a filter to this table.
	 *
	 * @param expr
	 *            The expression to add.
	 */
	public void addJoinElement(final Expr expr) {
		if (LOG.isDebugEnabled()) {
			QueryTableInfo.LOG.debug("Adding join element: {}", expr);
		}
		joinElementList.add(new ElementFilter(expr));

	}

	/**
	 * Adds the defined column and table definition to the query.
	 * <p>
	 * Columns that are in a SQL "USING" clause are added to the datafilter
	 * list.
	 * </p>
	 *
	 * @param columnsInUsing
	 *            columns that are in a SQL "Using" clause.
	 * @throws SQLDataException
	 */
	public void addDefinedColumns(final List<String> columnsInUsing)
			throws SQLDataException {
		if (LOG.isDebugEnabled()) {
			QueryTableInfo.LOG
					.debug("adding defined columns for {}", getName());
		}

		// add the table definition
		if (StringUtils.isNotBlank(getTable().getQuerySegmentFmt())) {
			final String queryFmt = new StringBuilder("{ ")
					.append(getTable().getQuerySegmentFmt()).append("}")
					.toString();
			final String queryStr = String.format(queryFmt, getGUIDVar());
			try {
				final ElementGroup subGroup = (ElementGroup) SparqlParser.Util
						.parse(queryStr);
				for (final Element subEl : subGroup.getElements()) {
					eg.addElement(subEl);
				}
			} catch (final ParseException e) {
				throw new IllegalStateException(getTable().getName()
						+ " query segment " + queryStr, e);
			} catch (final QueryException e) {
				throw new IllegalStateException(getTable().getName()
						+ " query segment " + queryStr, e);
			}
		}

		// add the column definitions
		for (final Iterator<Column> colIter = getTable().getColumns(); colIter
				.hasNext();) {
			final Column column = colIter.next();
			if (columnsInUsing.contains(column.getName().getShortName())) {
				final QueryColumnInfo columnInfo = getColumn(column.getName());
				if (columnInfo == null) {
					throw new SQLDataException(String.format(
							"Column in USING (%s) is not found in table %s",
							column.getName().getShortName(), this.getName()));
				}
				addDataFilter(columnInfo);
			}
			else {
				addColumnToQuery(column);
			}
		}

		if (LOG.isDebugEnabled()) {
			QueryTableInfo.LOG.debug("finished adding defined columns for {}",
					getName());
		}
	}

	/**
	 * Adds all of the table columns to the query.
	 *
	 * @param query
	 *            The query to add the columns to
	 *
	 */
	public void addTableColumns(final Query query,
			final List<String> columnsInUsing) {
		final Iterator<Column> iter = getTable().getColumns();
		while (iter.hasNext()) {
			final Column column = iter.next();
			QueryColumnInfo columnInfo = null;
			if (columnsInUsing.contains(column.getName().getShortName())) {
				columnInfo = infoSet.getColumn(column.getName());
			}
			else {
				columnInfo = addColumnToQuery(column);
			}
			final Var v = columnInfo.getVar();
			if (!query.getResultVars().contains(v.toString())) {
				addDataFilter(columnInfo);
				query.addResultVar(v);
			}
		}
	}

	/**
	 * Add the filters for the columns in the table.
	 *
	 * @param infoSet
	 * @throws SQLDataException
	 */
	public void addQueryFilters(final QueryInfoSet infoSet)
			throws SQLDataException {
		final List<QueryColumnInfo> columnInfoList = new ArrayList<QueryColumnInfo>();
		final Iterator<Column> iter = getTable().getColumns();
		while (iter.hasNext()) {
			final QueryColumnInfo columnInfo = infoSet.findColumn(iter.next());
			if (columnInfo != null) {
				columnInfoList.add(columnInfo);
			}
		}

		addTypeFilters(infoSet, columnInfoList, dataFilterList, joinElementList, eg, egWrapper);
	}

	/**
	 * Add the type and data filters for the table.
	 *
	 * Type filters are columns that need to be filtered to ensure that the data
	 * type in the graph matches or can be converted to the data type for JDBC
	 * driver.
	 *
	 * Data filters are columns that need to be filtered to ensure that the data
	 * is not null.
	 *
	 * @param typeFilterList
	 *            The list of QueryColumnInfos to be added to the type filter.
	 * @param dataFilterList
	 *            The list of QueryColumnInfos to be added to the data filter.
	 * @param filterGroup
	 *            The ElementGroup for the data filters.
	 * @param typeGroup
	 *            The ElementGroup for the type filters.
	 * @throws SQLDataException
	 */
	public static void addTypeFilters(final QueryInfoSet infoSet,
			final Collection<QueryColumnInfo> typeFilterList,
			final Collection<QueryColumnInfo> dataFilterList,
			final Collection<Element> joinFilterList,
			final ElementGroup filterGroup, final ElementGroup typeGroup)
			throws SQLDataException {

		Expr expr = null;
		for (final QueryColumnInfo columnInfo : typeFilterList) {
			final E_Function f = CheckTypeF.getFunction(columnInfo);
			if (LOG.isDebugEnabled()) {
				QueryTableInfo.LOG.debug("Adding filter: {} ({})", f,
						columnInfo);
			}
			if (expr == null) {
				expr = f;
			}
			else {
				expr = new E_LogicalAnd(expr, f);
			}
		}
		if (expr != null) {
			filterGroup.addElementFilter(new ElementFilter(expr));
		}

		for (final QueryColumnInfo columnInfo : dataFilterList) {
			final ElementBind bind = ForceTypeF.getBinding(columnInfo);
			if (LOG.isDebugEnabled()) {
				QueryTableInfo.LOG.debug("Adding binding: {}", bind);
			}
			typeGroup.addElement(bind);
		}
		
		for (final Element element : joinFilterList )
		{
			typeGroup.addElement( element );
		}
	}

	/**
	 * Returns the column or null if not found
	 *
	 * @param name
	 *            The name of the column to look for.
	 * @return QueryColumnInfo for the column
	 */
	public QueryColumnInfo getColumn(final ColumnName cName) {
		return getColumn(cName, true);
	}

	/**
	 * Returns the column or null if not found
	 *
	 * @param cName
	 *            The name of the column to look for.
	 * @param optional
	 *            If false then column is required, if true then the column
	 *            isOptional flag defines value
	 * @return The QueryColumnInfo for the column.
	 * @throws SQLException
	 */
	public QueryColumnInfo getColumn(final ColumnName cName,
			final boolean optional) {
		// columns not in tables are not found.
		if (!(cName.getTableName().matches(this.getName()))) {
			return null;
		}
		QueryColumnInfo retval = infoSet.findColumn(cName);
		if (retval == null) {
			// we have to check for the case where the column has the schema or
			// table def and the
			// infoSet does not.
			final Column col = getTable().getColumn(cName.getColumn());
			if (col != null) {
				final boolean opt = optional ? col.isOptional()
						: SparqlQueryBuilder.REQUIRED;
				retval = addColumnToQuery(col, cName, opt);
			}
		}
		if ((retval != null)
				&& retval.getName().getTableName().matches(this.getName())) {
			if (retval.isOptional() && !optional) {
				retval.setOptional(false);
			}
			return retval;
		}
		return null;
	}

	/**
	 * Get the element query segments for the
	 *
	 * @param column
	 *            The column to add query segments for.
	 * @param tableVar
	 *            The table variable.
	 * @param columnVar
	 *            The column variable.
	 * @return The Element
	 */
	private Element getQuerySegments(final Column column, final Var tableVar,
			final Var columnVar) {
		final String fmt = "{" + column.getQuerySegmentFmt() + "}";

		try {
			return SparqlParser.Util.parse(String.format(fmt, tableVar,
					columnVar));
		} catch (final ParseException e) {
			throw new IllegalStateException(column.getName()
					+ " query segment " + fmt, e);
		} catch (final QueryException e) {
			throw new IllegalStateException(column.getName()
					+ " query segment " + fmt, e);
		}
	}

	/**
	 * Get the SQL name for the table.
	 *
	 * @return The name.
	 */
	public String getSQLName() {
		return getName().getDBName();
	}

	/**
	 * Get the table this QueryTableInfo wraps.
	 *
	 * @return The innter table.
	 */
	public Table getTable() {
		return getBaseObject();
	}

	/**
	 * Adds the variable for aliasTableInfo as the value of the tableColumnInfo
	 * property. Optional state is determined from the optional state of the
	 * tableColumnInfo.
	 *
	 * @param tableColumnInfo
	 *            The column to alias
	 * @param aliasName
	 *            The name of the alias column
	 * @throws IllegalArgumentException
	 */
	public void setEquals(final QueryColumnInfo tableColumnInfo,
			final ColumnName aliasName) {
		final QueryColumnInfo ci = infoSet.getColumn(aliasName);
		if (ci != null) {
			setEquals(tableColumnInfo, ci);
		}
		else {
			throw new IllegalArgumentException(String.format(
					"%s is not a Query Column", aliasName));
		}

	}

	/**
	 * Adds the variable for aliasTableInfo as the value of the tableColumnInfo
	 * property. Optional state is determined from the optional state of the
	 * tableColumnInfo.
	 *
	 * @param tableColumnInfo
	 *            The column to alias
	 * @param aliasTableInfo
	 *            The alias column
	 */
	public void setEquals(final QueryColumnInfo tableColumnInfo,
			final QueryColumnInfo aliasTableInfo) {
		setEquals(tableColumnInfo, aliasTableInfo, tableColumnInfo.isOptional());
	}

	/**
	 * Adds the variable for aliasTableInfo as the value of the tableColumnInfo
	 * property.
	 *
	 * @param tableColumnInfo
	 *            The column to alias
	 * @param aliasColumnInfo
	 *            The alias column
	 * @param optional
	 *            determines if the entry should be optional.
	 */
	public void setEquals(final QueryColumnInfo tableColumnInfo,
			final QueryColumnInfo aliasColumnInfo, final boolean optional) {
		if (!infoSet.listColumns(getName().getColumnName(null)).contains(
				tableColumnInfo)) {
			throw new IllegalStateException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_,
					tableColumnInfo.getName(), getName()));
		}
		if (!tableColumnInfo.getColumn().hasQuerySegments()) {
			throw new IllegalArgumentException(String.format(
					"%s may not be aliased", tableColumnInfo.getName()));
		}
		if (optional) {
			eg.addElement(new ElementOptional(getQuerySegments(
					tableColumnInfo.getColumn(), getVar(),
					aliasColumnInfo.getVar())));
		}
		else {
			eg.addElement(new ElementOptional(getQuerySegments(
					tableColumnInfo.getColumn(), getVar(),
					aliasColumnInfo.getVar())));
		}
	}

	@Override
	public String toString() {
		return String.format("QueryTableInfo[%s(%s)]", getTable().getSQLName(),
				getName());
	}

}
package org.xenei.jdbc4sparql.sparql.items;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.lang.sparql_11.ParseException;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementBind;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Filter;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

import java.sql.SQLDataException;
import java.sql.SQLException;
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
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.sparql.CheckTypeF;
import org.xenei.jdbc4sparql.sparql.ForceTypeF;
import org.xenei.jdbc4sparql.sparql.QueryInfoSet;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

/**
 * Contains the information for the table in the query. If a defined table is
 * used multiple times with different aliases there will be multiple
 * QueryTableInfo instances with the same RdfTable but different names.
 */
public class QueryTableInfo extends QueryItemInfo<TableName> {

	/**
	 *
	 */
	// private final QueryInfoSet infoSet;
	private final Table table;
	private final ElementGroup eg;
	private final ElementGroup egWrapper;
	private final QueryInfoSet infoSet;

	// list of type filters to add at the end of the query
	private final Set<QueryColumnInfo> dataFilterList;

	private static Logger LOG = LoggerFactory.getLogger(QueryTableInfo.class);

	private static Table checkTable(Table table) {
		if (table == null) {
			throw new IllegalArgumentException("table may not be null");
		}
		return table;
	}

	public QueryTableInfo(final QueryInfoSet infoSet,
			final ElementGroup queryElementGroup, final Table table,
			final boolean optional) {
		this(infoSet, queryElementGroup, table, checkTable(table).getName(),
				optional);
	}

	public QueryTableInfo(final QueryInfoSet infoSet,
			final ElementGroup queryElementGroup, final Table table,
			final TableName alias, final boolean optional) {
		super(alias, optional);
		this.infoSet = infoSet;
		this.table = checkTable(table);
		this.egWrapper = new ElementGroup();
		this.eg = new ElementGroup();
		egWrapper.addElement(eg);
		this.dataFilterList = new HashSet<QueryColumnInfo>();

		if (queryElementGroup == null) {
			QueryTableInfo.LOG.debug("marking {} as not in query.", this);
		} else {
			if (optional) {
				QueryTableInfo.LOG.debug("marking {} as optional.", this);
				queryElementGroup.addElement(new ElementOptional(egWrapper));
			} else {
				QueryTableInfo.LOG.debug("marking {} as required.", this);
				queryElementGroup.addElement(egWrapper);
			}
		}
	}

	@Override
	public void setSegments(NameSegments usedSegments) {
		NameSegments ourSegments = new NameSegments(usedSegments.isCatalog(),
				usedSegments.isSchema(), true, false);

		super.setSegments(ourSegments);
		List<QueryColumnInfo> lst = infoSet.listColumns(getName());
		for (QueryColumnInfo col : lst) {
			col.setSegments(usedSegments);
		}
	}

	/**
	 * Adds the column to the columns defined in the query. If shortName is true
	 * then only the column name is used as the name, otherwise the SQL name for
	 * the column is used.
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
	 * optional if the RdfColumn is optional.
	 *
	 * @param column
	 *            The column to add
	 * @param cName
	 *            The name for the column in the query.
	 * @return The variable Node for the column.
	 */
	public QueryColumnInfo addColumnToQuery(final Column column,
			final ColumnName cName, boolean optional) {
		final QueryColumnInfo columnInfo = new QueryColumnInfo(column,
				getName().getColumnName(cName.getShortName()), optional);
		infoSet.addColumn(columnInfo);
		QueryTableInfo.LOG.debug("Adding column: {} as {} ({})", columnInfo,
				columnInfo.getGUID(), columnInfo.isOptional() ? "optional"
						: "required");

		if (column.hasQuerySegments() && optional) {
			eg.addElement(new ElementOptional(getQuerySegments(
					columnInfo.getColumn(), getVar(), columnInfo.getGUIDVar())));
		}

		return columnInfo;
	}

	/**
	 * Adds the column to the columns defined in the query. Result will be
	 * optional if the RdfColumn is optional.
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
	 * @param cName
	 *            The name for the column in the query
	 * @param optional
	 *            if True the column is optional
	 * @return The variable Node for the column.
	 */
	public QueryColumnInfo addColumnToQuery(final Column column,
			final boolean optional) {
		return addColumnToQuery(column, column.getName(), optional);

	}

	public void addDataFilter(QueryColumnInfo columnInfo) {
		dataFilterList.add(columnInfo);
	}

	public void addFilter(final Expr expr) {
		QueryTableInfo.LOG.debug("Adding filter: {}", expr);
		eg.addElementFilter(new ElementFilter(expr));

	}

	/**
	 * Adds the required columns to the query. Also adds the table definition.
	 *
	 * @param segments
	 *            The segments that must be used in the column names.
	 */
	public void addRequiredColumns() {
		QueryTableInfo.LOG.debug("adding required columns for {}", getName());
		final String eol = System.getProperty("line.separator");
		final StringBuilder queryFmt = new StringBuilder("{ ")
				.append(StringUtils.defaultString(table.getQuerySegmentFmt(),
						""));

		for (final Iterator<Column> colIter = table.getColumns(); colIter
				.hasNext();) {
			final Column column = colIter.next();
			if (!column.isOptional()) {
				addDataFilter(addColumnToQuery(column));
			}
		}

		// now add all the columns specified in the infoSet that are not
		// optional

		for (final QueryColumnInfo columnInfo : infoSet.listColumns(getName())) {
			if (!columnInfo.isOptional()) {
				final String fmt = columnInfo.getColumn().getQuerySegmentFmt();
				if (StringUtils.isNotBlank(fmt)) {
					queryFmt.append(
							String.format(fmt, getVar(),
									columnInfo.getGUIDVar())).append(eol);
				}
			}
		}
		queryFmt.append("}");
		final String queryStr = String.format(queryFmt.toString(), getVar());
		try {
			eg.addElement(SparqlParser.Util.parse(queryStr));
		} catch (final ParseException e) {
			throw new IllegalStateException(table.getName() + " query segment "
					+ queryStr, e);
		} catch (final QueryException e) {
			throw new IllegalStateException(table.getName() + " query segment "
					+ queryStr, e);
		}
		QueryTableInfo.LOG.debug("finished adding required columns for {}",
				getName());
	}

	/**
	 * Adds all of the table columns to the query.
	 *
	 * @param query
	 *            The query to add the columns to
	 *
	 */
	public void addTableColumns(final QueryInfoSet infoSet, final Query query) {
		final Iterator<Column> iter = table.getColumns();
		while (iter.hasNext()) {
			final QueryColumnInfo columnInfo = addColumnToQuery(iter.next());
			final Var v = columnInfo.getVar();
			if (!query.getResultVars().contains(v.toString())) {
				addDataFilter(columnInfo);
				query.addResultVar(v);
			}
		}
	}

	public void addQueryFilters(final QueryInfoSet infoSet)
			throws SQLDataException {
		ExtendedIterator<QueryColumnInfo> iter = WrappedIterator
				.create(table.getColumns())
				.mapWith(new Map1<Column, QueryColumnInfo>() {

					@Override
					public QueryColumnInfo map1(Column column) {
						return infoSet.findColumnByGUID(column.getName());
					}
				}).filterDrop(new Filter<QueryColumnInfo>() {

					@Override
					public boolean accept(QueryColumnInfo o) {
						return o == null;
					}
				});

		addTypeFilters(iter.toList(), dataFilterList, eg, egWrapper);
	}

	public static void addTypeFilters(
			Collection<QueryColumnInfo> typeFilterList,
			Collection<QueryColumnInfo> dataFilterList,
			ElementGroup filterGroup, ElementGroup typeGroup)
			throws SQLDataException {
		for (QueryColumnInfo columnInfo : typeFilterList) {
			CheckTypeF f = columnInfo.getTypeFilter();
			QueryTableInfo.LOG.debug("Adding filter: {}", f);
			filterGroup.addElementFilter(new ElementFilter(f));
		}

		for (QueryColumnInfo columnInfo : dataFilterList) {
			ForceTypeF f = columnInfo.getDataFilter();
			ElementBind bind = f.getBinding();
			QueryTableInfo.LOG.debug("Adding binding: {}", bind);
			typeGroup.addElement(bind);
		}
	}

	/**
	 * Returns the column or null if not found
	 *
	 * @param name
	 *            The name of the column to look for.
	 * @return QueryColumnInfo for the column
	 */
	public QueryColumnInfo getColumn(final QueryInfoSet infoSet,
			final ColumnName cName) {
		return getColumn(infoSet, cName, true);
	}

	/**
	 * Returns the column or null if not found
	 *
	 * @param name
	 *            The name of the column to look for.
	 * @param optional
	 *            If false then column is required, if true then the column
	 *            isOptional flag defines value
	 * @return
	 * @throws SQLException
	 */
	public QueryColumnInfo getColumn(final QueryInfoSet infoSet,
			final ColumnName cName, final boolean optional) {
		// columns not in tables are not found.
		if (!(cName.getTableName().matches(this.getName()))) {
			return null;
		}
		QueryColumnInfo retval = infoSet.findColumn(cName);
		if (retval == null) {
			// we have to check for the case where the column has the schema or
			// table def and the
			// infoSet does not.
			final Column col = table.getColumn(cName.getCol());
			if (col != null) {
				final boolean opt = optional ? col.isOptional()
						: SparqlQueryBuilder.REQUIRED;
				addColumnToQuery(col, cName, opt);
				retval = infoSet.findColumnByGUID(col.getName());
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

	private Element getQuerySegments(final Column column, final Node tableVar,
			final Node columnVar) {
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

	public String getSQLName() {
		return getName().getDBName();
	}

	public Table getTable() {
		return table;
	}

	/**
	 * Adds the variable for aliasTableInfo as the value of the tableColumnInfo
	 * property. Optional state is determined from the optional state of the
	 * tableColumnInfo.
	 *
	 * @param tableColumnInfo
	 *            The column to alias
	 * @param alias
	 *            The name of the alias column
	 */
	public void setEquals(final QueryColumnInfo tableColumnInfo,
			final ColumnName aliasName) {
		final QueryColumnInfo ci = infoSet.getColumn(aliasName);
		if (ci != null) {
			setEquals(tableColumnInfo, ci);
		} else {
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
		} else {
			eg.addElement(new ElementOptional(getQuerySegments(
					tableColumnInfo.getColumn(), getVar(),
					aliasColumnInfo.getVar())));
		}
	}

	@Override
	public String toString() {
		return String.format("QueryTableInfo[%s(%s)]", table.getSQLName(),
				getName());
	}

}
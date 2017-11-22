package org.xenei.jdbc4sparql.sparql;

import java.sql.SQLDataException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.items.QueryItemCollection;
import org.xenei.jdbc4sparql.sparql.items.QueryItemInfo;
import org.xenei.jdbc4sparql.sparql.items.QueryTableInfo;

import org.apache.jena.util.iterator.ExtendedIterator;

/**
 * A collection of all tables and columns in the query. The column names are
 * index by full aliased table name based names (no short names)
 */
public class QueryInfoSet {

	// the list of tables in the query indexed by SQL name.
	private final QueryItemCollection<QueryTableInfo, Table, TableName> tablesInQuery;

	// the list of columns in the query indexed by SQL name.
	private final QueryItemCollection<QueryColumnInfo, Column, ColumnName> columnsInQuery;

	private static final Logger LOG = LoggerFactory
			.getLogger(QueryInfoSet.class);

	// private final Set<String> schemasInQuery;
	/**
	 * The segments to display. Will be reset by setMinimumColumnSegments.
	 */
	private NameSegments segments;

	//private boolean guidFlg;

	public QueryInfoSet() {
		this.tablesInQuery = new QueryItemCollection<QueryTableInfo, Table, TableName>();
		this.columnsInQuery = new QueryItemCollection<QueryColumnInfo, Column, ColumnName>();
		this.segments = NameSegments.ALL;
	}

	public boolean addColumn(final QueryColumnInfo columnInfo) {
		//columnInfo.getName().setUseGUID(guidFlg);
		columnInfo.setSegments(segments);
		if (LOG.isDebugEnabled()) {
			QueryInfoSet.LOG.debug("adding column: {}", columnInfo);
		}
		return columnsInQuery.add(columnInfo);
	}

//	public boolean setUseGUID(final boolean state) {
//		boolean retval = this.guidFlg;
//		if (this.guidFlg != state) {
//			this.guidFlg = state;
//			for (final QueryItemInfo<Column, ColumnName> columnInfo : columnsInQuery) {
//				columnInfo.getName().setUseGUID(state);
//			}
//			for (final QueryItemInfo<Table, TableName> tableInfo : tablesInQuery) {
//				tableInfo.getName().setUseGUID(state);
//			}
//		}
//		return retval;
//	}
//
//	public boolean useGUID() {
//		return guidFlg;
//	}

	public void addDefinedColumns(final List<String> columnsInUsing)
			throws SQLDataException {
		if (tablesInQuery.isEmpty()) {
			throw new IllegalArgumentException("Must have at least one table");
		}
		for (final QueryItemInfo<Table, TableName> tableInfo : tablesInQuery) {
			((QueryTableInfo) tableInfo).addDefinedColumns(columnsInUsing);
		}
	}

	public void addTable(final QueryTableInfo tbl) {
		if (!tablesInQuery.contains(tbl)) {
//			tbl.getName().setUseGUID(guidFlg);
			if (LOG.isDebugEnabled()) {
				QueryInfoSet.LOG.debug("adding table: {}", tbl);
			}
			tablesInQuery.add(tbl);
			tbl.setSegments(NameSegments.ALL);
		}
	}

	/**
	 * Returns true if the name matches any of the column names.
	 *
	 * @see {QueryItemName.findMatch()}
	 * @param name
	 *            The name to match
	 * @return true if the name matches is found.
	 */
	public boolean containsColumn(final ItemName name) {
		return columnsInQuery.contains(name);
	}

	/**
	 * Find the column in the query. Returns null if not found.
	 *
	 * @param name
	 *            The column Name to search for.
	 * @return columnInfo for column or null if not found.
	 */
	public QueryColumnInfo findColumn(final ColumnName name) {
		return columnsInQuery.get(name);
	}

	/**
	 * Find the column in the query by its GUID. returns null if not found
	 *
	 * @param name
	 *            The column name defining the GUID
	 * @return the QueryColumnInfo or null.
	 */
	public QueryColumnInfo findColumnByGUIDVar(final String guid) {
		return columnsInQuery.findGUIDVar(guid);
	}

	/**
	 * Force this infoset to use short names for columns.
	 */
	public void setMinimumColumnSegments() {
		segments = NameSegments.WILD;
		if (tablesInQuery.size() == 0) {
			throw new IllegalArgumentException(
					"There must be at lease 1 table in the query.");
		}
		if (tablesInQuery.size() == 1) {
			segments = NameSegments.getInstance(false, false, false, true);
		}
		else {
			boolean duplicateTable = false;
			boolean duplicateColumn = false;
			final Set<String> names = new HashSet<String>();
			// if there are duplicate table names and multiple schemas then we
			// have to specify schema
			for (final QueryItemInfo<Table, TableName> qti : tablesInQuery) {
				final TableName sn = qti.getName().clone(
						NameSegments.getInstance(false, true, true, true));
				duplicateTable |= names.contains(sn.getTable());
				names.add(sn.getTable());
			}
			names.clear();
			for (final QueryColumnInfo qci : columnsInQuery) {
				final ColumnName sn = qci.getName().clone(
						NameSegments.getInstance(false, true, true, true));
				duplicateColumn |= names.contains(sn.getTable());
				names.add(sn.getTable());
			}
			segments = NameSegments.getInstance(false, duplicateTable,
					duplicateTable | duplicateColumn, true);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Setting default segments to %s", segments));
		}
		for (final QueryItemInfo<Table, TableName> qti : tablesInQuery) {
			((QueryTableInfo) qti).setSegments(segments);
		}

	}

	/**
	 * Retrieves the column form the list of query columns.
	 *
	 * Uses the match algorithm.
	 *
	 * @param name
	 *            The name to retrieve
	 * @return The column info for the named column
	 * @throws IllegalArgumentException
	 *             if the column is not found
	 */
	public QueryColumnInfo getColumn(final ColumnName name) {

		final QueryColumnInfo retval = columnsInQuery.get(name);
		if (retval == null) {
			throw new IllegalArgumentException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_QUERY, name));
		}
		return retval;
	}

	/**
	 * Get the index of the column in the column name list.
	 *
	 * @param name
	 * @return the index (0 based) or -1 if not present
	 */
	public int getColumnIndex(final ColumnName name) {
		return columnsInQuery.indexOf(name);
	}

	/**
	 * Get the list of columns.
	 *
	 * @return the list of columns
	 */
	public QueryItemCollection<QueryColumnInfo, Column, ColumnName> getColumns() {
		return new QueryItemCollection<QueryColumnInfo, Column, ColumnName>(
				columnsInQuery.iterator().toList());
	}

	/**
	 * Get the table info from the tables in the query
	 *
	 * @param name
	 *            The table name to find.
	 * @return The query table info for the name or null if none found
	 * @throws IllegalArgumentException
	 *             if more than one object matches.
	 */
	public QueryTableInfo getTable(final TableName name) {
		return tablesInQuery.get(name);
	}

	public QueryItemCollection<QueryTableInfo, Table, TableName> getTables() {
		return new QueryItemCollection<QueryTableInfo, Table, TableName>(
				tablesInQuery.iterator().toList());
	}

	/**
	 * List the columns that match the name.
	 *
	 * @param name
	 *            The Column name to match
	 * @return The list o
	 */
	public List<QueryColumnInfo> listColumns(final ItemName name) {
		return columnsInQuery.match(name).toList();
	}

	public Set<Column> uniqueColumns() {
		return columnsInQuery.getNamedObjectSet();
	}

	public ExtendedIterator<QueryColumnInfo> iterateColumns(final ItemName name) {
		return columnsInQuery.match(name);
	}

	public Collection<QueryTableInfo> listTables(final ItemName name) {
		if (name.getUsedSegments().isColumn()) {
			final TableName searchName = new TableName(name);
			final NameSegments segs = name.getUsedSegments();
			final NameSegments newSegs = NameSegments.getInstance(
					segs.isCatalog(), segs.isSchema(), segs.isTable(), false);
			searchName.setUsedSegments(newSegs);
			return tablesInQuery.match(searchName).toList();
		}
		else {
			return tablesInQuery.match(name).toList();
		}

	}

	public NameSegments getSegments() {
		return segments;
	}

	/**
	 * find the first QueryColumnInfo that references the Column
	 *
	 * @param column
	 *            The column to find.
	 * @return The first matching QueryColumnInfo or null if not found.
	 */
	public QueryColumnInfo findColumn(final Column column) {
		final Iterator<QueryColumnInfo> iter = columnsInQuery
				.findBaseObject(column);
		return iter.hasNext() ? iter.next() : null;
	}

	/**
	 * Returns the column from the query if it exists. Otherwise scan across the
	 * tables in the query looking for the column. If found add it to the query
	 * and return it. otherwise return null.
	 *
	 * @param name
	 * @return
	 */
	public QueryColumnInfo scanTablesForColumn(final ColumnName cName) {
		// check exact match
		QueryColumnInfo retval = findColumn(cName);
		if (retval == null) {
			// get the table and see if column is in it
			final TableName tName = cName.getTableName();
			QueryTableInfo tableInfo = null;
			// table name may be wild so use list.
			for (final QueryTableInfo testTableInfo : listTables(tName)) {
				if (testTableInfo.getColumn(cName) != null) {
					if (tableInfo != null) {
						QueryInfoSet.LOG.info(String.format(
								SparqlQueryBuilder.FOUND_IN_MULTIPLE_, cName,
								"tables"));
						return null;
					}
					tableInfo = testTableInfo;
				}
			}

			if (tableInfo != null) {
				retval = tableInfo.getColumn(cName);
			}

		}
		return retval;
	}
}

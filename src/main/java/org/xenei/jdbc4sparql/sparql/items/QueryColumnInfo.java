package org.xenei.jdbc4sparql.sparql.items;

import java.sql.SQLDataException;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.sparql.CheckTypeF;
import org.xenei.jdbc4sparql.sparql.ForceTypeF;

/**
 * A column in a table in the query. This class maps the column to an alias
 * name. This may be a column in a base table or a function.
 */
public class QueryColumnInfo extends QueryItemInfo<Column, ColumnName> {

	private static Column checkColumn(final Column column) {
		if (column == null) {
			throw new IllegalArgumentException("Column may not be null");
		}
		return column;
	}

	public static NameSegments createSegments(final NameSegments segments) {
		return NameSegments.getInstance(segments.isCatalog(),
				segments.isSchema(), segments.isSchema() || segments.isTable(),
				true);
	}

	private CheckTypeF typeFilter;

	public QueryColumnInfo(final Column column) {
		this(column, column.isOptional());
	}

	/**
	 * Create a QueryColumnInfo not associated with a QueryInfoSet
	 *
	 * @param tableInfo
	 * @param column
	 * @param alias
	 * @param optional
	 */
	public QueryColumnInfo(final Column column, final boolean optional) {
		super(column, checkColumn(column).getName(), optional);
	}

	public QueryColumnInfo(final Column column, final ColumnName alias) {
		this(column, alias, column.isOptional());
	}

	public QueryColumnInfo(final Column column, final ColumnName alias,
			final boolean optional) {
		super(column, alias, optional);

	}

	public QueryColumnInfo createAlias(final ColumnName alias)
			throws SQLDataException {
		final QueryColumnInfo retval = new QueryColumnInfo(this.getColumn(),
				alias);
		retval.typeFilter = getTypeFilter();
		return retval;
	}

	@Override
	public boolean equals(final Object o) {
		if ((o != null) && (o instanceof QueryColumnInfo)) {
			final QueryColumnInfo colInfo = (QueryColumnInfo) o;
			return getName().equals(colInfo.getName())
					&& getColumn().equals(colInfo.getColumn());
		}
		return false;
	}

	public Column getColumn() {
		return getBaseObject();
	}

	public ForceTypeF getDataFilter() throws SQLDataException {
		return new ForceTypeF(getTypeFilter());
	}

	public CheckTypeF getTypeFilter() throws SQLDataException {
		if (typeFilter == null) {
			typeFilter = new CheckTypeF(this);
		}
		return typeFilter;
	}

	@Override
	public int hashCode() {
		return getName().hashCode();
	}

	@Override
	public void setOptional(final boolean optional) {
		super.setOptional(optional);
	}

	@Override
	public void setSegments(final NameSegments segments) {
		super.setSegments(createSegments(segments));
	}

	@Override
	public String toString() {
		return String.format("QueryColumnInfo[%s(%s)]", getColumn()
				.getSQLName(), getName());
	}

}

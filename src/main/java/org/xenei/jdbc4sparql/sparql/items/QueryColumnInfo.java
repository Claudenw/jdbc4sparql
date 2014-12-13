package org.xenei.jdbc4sparql.sparql.items;

import java.sql.SQLDataException;
import java.util.List;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.sparql.CheckTypeF;
import org.xenei.jdbc4sparql.sparql.ForceTypeF;

import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Filter;

/**
 * A column in a table in the query. This class maps the column to an alias
 * name. This may be a column in a base table or a function.
 */
public class QueryColumnInfo extends QueryItemInfo<Column, ColumnName> {

	private CheckTypeF typeFilter;

	public static NameSegments createSegments(NameSegments segments) {
		return NameSegments.getInstance(segments.isCatalog(), segments.isSchema(),
				segments.isSchema() || segments.isTable(), true);
	}

	private static Column checkColumn(Column column) {
		if (column == null) {
			throw new IllegalArgumentException("Column may not be null");
		}
		return column;
	}

	public QueryColumnInfo(final Column column) {
		this(column, column.isOptional());
	}

	public QueryColumnInfo(final Column column, final ColumnName alias) {
		this(column, alias, column.isOptional());
	}

	public QueryColumnInfo(final Column column, final ColumnName alias,
			boolean optional) {
		super(column, alias, optional);

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

	public QueryColumnInfo createAlias(final ColumnName alias) throws SQLDataException {
		QueryColumnInfo retval = new QueryColumnInfo(this.getColumn(), alias);
		retval.typeFilter = getTypeFilter();
		return retval;
	}

	@Override
	public void setSegments(NameSegments segments) {
		super.setSegments(createSegments(segments));
	}

	public CheckTypeF getTypeFilter() throws SQLDataException {
		if (typeFilter == null) {
			typeFilter = new CheckTypeF(this);
		}
		return typeFilter;
	}

	public ForceTypeF getDataFilter() throws SQLDataException {
		return new ForceTypeF(getTypeFilter());
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

	@Override
	public int hashCode() {
		return getName().hashCode();
	}

	@Override
	public void setOptional(final boolean optional) {
		super.setOptional(optional);
	}

	@Override
	public String toString() {
		return String.format("QueryColumnInfo[%s(%s)]", getColumn().getSQLName(),
				getName());
	}

}

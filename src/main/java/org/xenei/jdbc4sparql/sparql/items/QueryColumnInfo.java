package org.xenei.jdbc4sparql.sparql.items;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;

import com.hp.hpl.jena.sparql.core.Var;

/**
 * A column in a table in the query. This class maps the column to an alias
 * name. This may be a column in a base table or a function.
 */
public class QueryColumnInfo extends QueryItemInfo<Column, ColumnName> {

	private QueryColumnInfo aliasFor;

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
		aliasFor = null;
	}

	public QueryColumnInfo(final Column column, final ColumnName alias) {
		this(column, alias, column.isOptional());
	}

	public QueryColumnInfo(final Column column, final ColumnName alias,
			final boolean optional) {
		super(column, alias, optional);
		aliasFor = null;
	}

	public QueryColumnInfo createAlias(final ColumnName alias) {
		final QueryColumnInfo retval = new QueryColumnInfo(this.getColumn(),
				alias);
		retval.aliasFor = this;
		return retval;
	}

	/**
	 * Returns true if this ItemInfo is an alias for another ItemInfo
	 *
	 * @return true if alias, false otherwise.
	 */
	public boolean isAlias() {
		return aliasFor != null;
	}

//	public QueryColumnInfo getAliasFor() {
//		return aliasFor;
//	}
	
	public QueryColumnInfo getBaseColumnInfo() {
		return aliasFor != null? aliasFor.getBaseColumnInfo() : this;
	}

	/**
	 * Get the GUID variable based on the name of this column.
	 *
	 * If this column is an alias for another column returns the GUIDVar for
	 * that column.
	 *
	 * @return The var for is column.
	 */
	@Override
	public Var getGUIDVar() {
		return aliasFor != null ? aliasFor.getGUIDVar() : super.getGUIDVar();
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
	public void setSegments(final NameSegments segments) {
		super.setSegments(createSegments(segments));
	}

	@Override
	public String toString() {
		return String.format("QueryColumnInfo[%s(%s)]", getColumn()
				.getSQLName(), getName());
	}

}

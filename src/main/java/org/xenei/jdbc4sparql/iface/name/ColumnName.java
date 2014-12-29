package org.xenei.jdbc4sparql.iface.name;

import org.apache.commons.lang.StringUtils;
import org.xenei.jdbc4sparql.impl.NameUtils;

/**
 * The ColumnName implementation of ItemName
 */
public class ColumnName extends ItemName {

	/**
	 * Create a column name in the item name. The default catalog, schema and
	 * table will be taken from the basename for the item. The name parameter
	 * may consist of the column name by itself, a column and table name, or a
	 * column, table and schema name. the name parameter segments may be
	 * separated by either a JDBC or a SPARLQ "dot" character. if the name
	 * contains both a JDBC and a SPARQL "dot" character an
	 * IllegalArgumentException is thrown. if the catalog, the final schema name
	 * or name are null an IllegalArgumentException is thrown.
	 *
	 * @param item
	 *            The ItemName instance to create a column name in.
	 * @param name
	 *            the name string for the column
	 * @return The ColumnName.
	 * @throws IllegalArgumentException
	 */
	public static ColumnName getNameInstance(final ItemName item,
			final String name) throws IllegalArgumentException {
		final FQName bn = item.getFQName();
		final ColumnName retval = getNameInstance(bn.getCatalog(),
				bn.getSchema(), bn.getTable(), name);
		retval.setUsedSegments(adjustSegments(item.getUsedSegments()));
		return retval;
	}

	/**
	 * Create an instance of a ColumnName given a potentially fully qualified
	 * name and default catalog, schema and table names. The name parameter may
	 * consist of the column name by itself, a column and table name, or a
	 * column, table and schema name. the name parameter segments may be
	 * separated by either a JDBC or a SPARLQ "dot" character. if the name
	 * parameter specifies the table name the table parameter may be null. if
	 * the name parameter specifies the schema name the schema parameter may be
	 * null. if the name contains both a JDBC and a SPARQL "dot" character an
	 * IllegalArgumentException is thrown. if the catalog, the final schema name
	 * or name are null an IllegalArgumentException is thrown.
	 *
	 * @param catalog
	 *            The catalog name string. May not be null.
	 * @param schema
	 *            The schema name string. May be null.
	 * @param table
	 *            The table name string. May be null.
	 * @param name
	 *            the potentially fully qualified name. may not be null.
	 * @return The ColumnName.
	 * @throws IllegalArgumentException
	 */
	public static ColumnName getNameInstance(final String catalog,
			final String schema, final String table, final String name)
					throws IllegalArgumentException {
		checkNotNull(name, "column");
		if (name.contains(NameUtils.DB_DOT)
				&& name.contains(NameUtils.SPARQL_DOT)) {
			throw new IllegalArgumentException(String.format(
					"Name may not cointain both '%s' and '%s'",
					NameUtils.DB_DOT, NameUtils.SPARQL_DOT));
		}

		final String separator = name.contains(NameUtils.DB_DOT) ? "\\"
				+ NameUtils.DB_DOT : NameUtils.SPARQL_DOT;

		final String[] parts = name.split(separator);
		switch (parts.length) {
			case 4:
				return new ColumnName(parts[0], parts[1], parts[2], parts[4]);

			case 3:
				return new ColumnName(catalog, parts[0], parts[1], parts[2]);

			case 2:
				return new ColumnName(catalog, schema, parts[0], parts[1]);

			case 1:
				return new ColumnName(catalog, schema, table, parts[0]);

			default:
				throw new IllegalArgumentException(String.format(
						"Column name must be 1 to 3 segments not %s as in %s",
						parts.length, name));
		}
	}

	/**
	 * Check the column name. Checks that the itemName column, table, schema and
	 * catalog name segments are not null.
	 *
	 * @param name
	 *            The ItemName to check.
	 * @return the ItemName
	 * @Throws IllegalArgumentException
	 */
	static ItemName checkItemName(final ItemName name)
			throws IllegalArgumentException {
		if (name == null) {
			throw new IllegalArgumentException("name may not be null");
		}
		TableName.checkItemName(name);
		checkNotNull(name.getFQName().getColumn(), "column");
		return name;
	}

	/**
	 * Ensure that the column segment is on.
	 *
	 * @param segments
	 *            The segments to adjust
	 * @return the adjusted segments.
	 * @Throws IllegalArgumentException if segments is null.
	 */
	private static NameSegments adjustSegments(final NameSegments segments)
			throws IllegalArgumentException {
		if (segments == null) {
			throw new IllegalArgumentException("segments may not be null");
		}
		if (segments.isColumn()) {
			return segments;
		}
		return NameSegments.getInstance(segments.isCatalog(),
				segments.isSchema(), segments.isTable(), true);
	}

	/**
	 * Create a ColumnName from an ItemName.
	 *
	 * @param name
	 *            the ItemName, must not be null.
	 * @Throws IllegalArgumentException is name is null.
	 */
	public ColumnName(final ItemName name) throws IllegalArgumentException {
		this(name, name.getUsedSegments());
	}

	/**
	 * Create a ColumnName from an ItemName with specific name segments.
	 *
	 * @param name
	 *            the ItemName, must not be null.
	 * @param segments
	 *            the name segments to use.
	 * @Throws IllegalArgumentException is name or segments are null.
	 */
	public ColumnName(final ItemName name, final NameSegments segments)
			throws IllegalArgumentException {
		super(checkItemName(name), adjustSegments(segments));
	}

	/**
	 * Create a TableName from a catalog name string, a schema name string and a
	 * table name string and a column name string. Uses the default namesegments
	 * for a column.
	 *
	 * @param catalog
	 *            the catalog name string.
	 * @param schema
	 *            the schema name string.
	 * @param table
	 *            the table name string.
	 * @param column
	 *            the column name string.
	 * @throws IllegalArgumentException
	 *             if any string is null.
	 */
	public ColumnName(final String catalog, final String schema,
			final String table, final String column)
					throws IllegalArgumentException {
		this(catalog, schema, table, column, NameSegments.COLUMN);
	}

	/**
	 * Create a TableName from a catalog name string, a schema name string and a
	 * table name string and a column name string.
	 *
	 * @param catalog
	 *            the catalog name string.
	 * @param schema
	 *            the schema name string.
	 * @param table
	 *            the table name string.
	 * @param column
	 *            the column name string.
	 * @param segments
	 *            The name segments to use.
	 * @throws IllegalArgumentException
	 *             if any string is null.
	 */
	public ColumnName(final String catalog, final String schema,
			final String table, final String column, final NameSegments segments)
					throws IllegalArgumentException {
		super(new FQNameImpl(checkNotNull(catalog, "catalog"), checkNotNull(
				schema, "schema"), checkNotNull(table, "table"), checkNotNull(
				column, "column")), segments);
	}

	@Override
	public String getShortName() {
		return getColumn();
	}

	/**
	 * Returns the TableName object for column
	 *
	 * @return the TableName.
	 */
	public TableName getTableName() {
		return new TableName(this);
	}

	@Override
	protected String createName(final String separator) {

		final StringBuilder sb = new StringBuilder();

		if (StringUtils.isNotEmpty(getSchema())) {
			sb.append(getSchema()).append(separator);
		}

		final String tbl = StringUtils.defaultString(getTable());
		if ((tbl.length() > 0) || (sb.length() > 0)) {
			sb.append(tbl).append(separator);
		}

		if (StringUtils.isNotEmpty(getColumn())) {
			sb.append(getColumn());
		}
		return sb.toString();

	}

	/**
	 * Clone this column name with different segments.
	 */
	@Override
	public ColumnName clone(final NameSegments segs) {
		return new ColumnName(this, segs);
	}
}
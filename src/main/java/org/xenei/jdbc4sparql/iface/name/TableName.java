package org.xenei.jdbc4sparql.iface.name;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.impl.NameUtils;

/**
 * Table Name implementation of ItemName
 */
public class TableName extends ItemName {

	/**
	 * Create an instance of a TableName given a potentially fully qualified
	 * name and default catalog and schema names. The name may consist of the
	 * table name by itself, or a schema name and the table name separated by
	 * either a JDBC or a SPARLQ "dot" character. If the name contains the
	 * schema name the schema parameter may be null. if the name contains both a
	 * JDBC and a SPARQL "dot" character an IllegalArgumentException is thrown.
	 * if the catalog, the final schema name or name are null an
	 * IllegalArgumentException is thrown.
	 *
	 * @param catalog
	 *            The default catalog name string. may not be null.
	 * @param schema
	 *            The default schema name string. may be null if name contains
	 *            schema segment.
	 * @param name
	 *            the potentially fully qualified name. may not be null.
	 * @return The TableName.
	 * @throws IllegalArgumentException
	 *             if any conditions fail.
	 */
	public static TableName getNameInstance(final String catalog,
			final String schema, final String name)
			throws IllegalArgumentException {
		if (name == null) {
			throw new IllegalArgumentException("name must be provided");
		}
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
			case 2:
				return new TableName(catalog, parts[0], parts[1]);
			case 1:
				return new TableName(catalog, schema, parts[0]);

			default:
				throw new IllegalArgumentException(String.format(
						"Column name must be 1 or 2 segments not %s as in %s",
						parts.length, name));

		}
	}

	/**
	 * Ensure that the table segment is on and the column segment is off.
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
		if (segments.isTable() && !segments.isColumn()) {
			return segments;
		}
		return NameSegments.getInstance(segments.isCatalog(),
				segments.isSchema(), true, false);
	}

	/**
	 * Check the table name. Checks that the itemName table, schema and catalog
	 * name segments are not null.
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
		SchemaName.checkItemName(name);
		checkNotNull(name.getFQName().getTable(), "table");
		return name;
	}

	/**
	 * Create a TableName from an ItemName.
	 *
	 * @param name
	 *            the ItemName, must not be null.
	 * @Throws IllegalArgumentException is name is null.
	 */
	public TableName(final ItemName name) throws IllegalArgumentException {
		this(name, name.getUsedSegments());
	}

	/**
	 * Create a TableName from an ItemName with specific name segments.
	 *
	 * @param name
	 *            the ItemName, must not be null.
	 * @param segments
	 *            the name segments to use.
	 * @Throws IllegalArgumentException is name or segments are null.
	 */
	public TableName(final ItemName name, final NameSegments segments)
			throws IllegalArgumentException {
		super(checkItemName(name), adjustSegments(segments));
	}

	/**
	 * Create a TableName from a catalog name string, a schema name string and a
	 * table name string. Uses the default namesegments for a table.
	 *
	 * @param catalog
	 *            the catalog name string.
	 * @param schema
	 *            the schema name string.
	 * @param table
	 *            the table name string.
	 * @throws IllegalArgumentException
	 *             if any string is null.
	 */
	public TableName(final String catalog, final String schema,
			final String table) throws IllegalArgumentException {
		super(new FQNameImpl(checkNotNull(catalog, "catalog"), checkNotNull(
				schema, "schema"), checkNotNull(table, "table"), null),
				NameSegments.TABLE);
	}

//	@Override
//	public String createName(final String separator) {
//		final StringBuilder sb = new StringBuilder();
//
//		if (StringUtils.isNotEmpty(getSchema())) {
//			sb.append(getSchema()).append(separator);
//		}
//
//		if (StringUtils.isNotEmpty(getTable()) || (sb.length() > 0)) {
//			sb.append(getTable());
//		}
//
//		return sb.toString();
//	}

	/**
	 * Create a column name in this table.
	 *
	 * @param column
	 *            the columnName string
	 * @return the ColumnName
	 * @throws IllegalArgumentException
	 */
	public ColumnName getColumnName(final String column)
			throws IllegalArgumentException {
		return ColumnName.getNameInstance(this, column);
	}

//	@Override
//	public String getShortName() {
//		return getTable();
//	}

	/**
	 * clone this table name with different segments.
	 */
	@Override
	public TableName clone(final NameSegments segs) {
		return new TableName(this, segs);
	}
}
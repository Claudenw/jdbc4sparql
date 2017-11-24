package org.xenei.jdbc4sparql.iface.name;

import org.apache.commons.lang3.StringUtils;

/**
 * Schema Name implementation.
 */
public class SchemaName extends ItemName {
	/**
	 * Check the schema name. Checks that the itemName schema and catalog name
	 * segments are not null.
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
		CatalogName.checkItemName(name);
		checkNotNull(name.getFQName().getSchema(), "schema");
		return name;
	}

	protected NameSegments modifyNameSegments( NameSegments segs ) {
		return adjustSegments( segs );
	}
	
	/**
	 * Ensure that the schema segment is on, and the table and column segments
	 * are off.
	 *
	 * @param segments
	 *            The segments to adjust
	 * @return the adjusted segments.
	 * @Throws IllegalArgumentException if segments is null.
	 */
	private static NameSegments adjustSegments(final NameSegments segments)
			throws IllegalArgumentException {
		if (segments == null) {
			throw new IllegalArgumentException("Segments may not be null");
		}
		return segments.or( NameSegments.FTFF ).and( NameSegments.TTFF);
	}

	/**
	 * Create a SchmeaName from an ItemName.
	 *
	 * @param name
	 *            the ItemName, must not be null.
	 * @Throws IllegalArgumentException is name is null.
	 */
	public SchemaName(final ItemName name) throws IllegalArgumentException {
		this(checkItemName(name), name.getUsedSegments());
	}

	/**
	 * Create a SchmeaName from an ItemName with specific name segments.
	 *
	 * @param name
	 *            the ItemName, must not be null.
	 * @param segments
	 *            the name segments to use.
	 * @Throws IllegalArgumentException is name or segments are null.
	 */
	public SchemaName(final ItemName name, final NameSegments segments)
			throws IllegalArgumentException {
		super(checkItemName(name), adjustSegments(segments));
	}

	/**
	 * Create a SchemaNamefrom a catalog name string and a schema name string.
	 * Uses the default namesegments for a schema.
	 *
	 * @param catalog
	 *            the catalog name string.
	 * @param schema
	 *            the schema name string.
	 * @throws IllegalArgumentException
	 *             if either string is null.
	 */
	public SchemaName(final String catalog, final String schema)
			throws IllegalArgumentException {
		super(new FQNameImpl(checkNotNull(catalog, "catalog"), checkNotNull(
				schema, "schema"), null, null), NameSegments.SCHEMA);
	}

	/**
	 * Create a table name in this schema.
	 *
	 * @param tblName
	 *            the table name string.
	 * @return the Table Name
	 * @throws IllegalArgumentException
	 *             if either name is null.
	 */
	public TableName getTableName(final String tblName)
			throws IllegalArgumentException {
		final FQName baseName = getFQName();
		return new TableName(baseName.getCatalog(), baseName.getSchema(),
				tblName);
	}

//	@Override
//	protected String createName(final String separator) {
//		return StringUtils.defaultString(getSchema());
//	}

	/**
	 * Returns the schema name.
	 */
//	@Override
//	public String getShortName() {
//		return getSchema();
//	}

	@Override
	public SchemaName clone(final NameSegments segs) {
		return new SchemaName(this, segs);
	}
}
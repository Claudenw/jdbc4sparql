package org.xenei.jdbc4sparql.iface.name;

import org.apache.commons.lang3.StringUtils;

/**
 * Catalog ItemName implementation.
 */
public class CatalogName extends ItemName {
	/**
	 * Check the catalog name. Checks that the itemName catalog name segment is
	 * not null.
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
		checkNotNull(name.getFQName().getCatalog(), "catalog");
		return name;
	}

	/**
	 * Create a CatalogName from an ItemName.
	 *
	 * @param name
	 *            the ItemName, must not be null.
	 * @Throws IllegalArgumentException is name is null.
	 */
	public CatalogName(final ItemName name) throws IllegalArgumentException {
		super(checkItemName(name), NameSegments.CATALOG);
	}

	/**
	 * Create a CatalogName from a catalog name string. Uses the default
	 * namesegments for a catalog.
	 *
	 * @param catalog
	 *            the catalog name string.
	 * @throws IllegalArgumentException
	 *             if the catalog name string is null.
	 */
	public CatalogName(final String catalog) throws IllegalArgumentException {
		super(
				new FQNameImpl(checkNotNull(catalog, "catalog"), null, null,
						null), NameSegments.CATALOG);
	}

	protected NameSegments modifyNameSegments( NameSegments segs ) {
		return NameSegments.CATALOG;		
	}
	
	/**
	 * Create a schemas name in this catalog.
	 *
	 * @param column
	 *            the columnName string
	 * @return the ColumnName
	 * @throws IllegalArgumentException
	 */
	public SchemaName getSchemaName(final String name) {
		return new SchemaName(getFQName().getCatalog(), name);
	}

//	@Override
//	protected String createName(final String separator) {
//		return StringUtils.defaultString(getCatalog(), "");
//	}

//	@Override
//	public String getShortName() {
//		return getCatalog();
//	}

	@Override
	public String toString() {
		return getCatalog();
	}

	/**
	 * Clone this catalog name with different segments.
	 */
	@Override
	public CatalogName clone(final NameSegments segs) {
		return new CatalogName(this);
	}
}
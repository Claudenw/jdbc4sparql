package org.xenei.jdbc4sparql.iface.name;

import org.apache.commons.lang.StringUtils;

/**
 * Name implementation.
 */
public class CatalogName extends ItemName {

	static ItemName checkItemName(ItemName name) {
		checkNotNull(name.getBaseName().getCatalog(), "catalog");
		return name;
	}

	public CatalogName(final ItemName name) {
		super(checkItemName(name), NameSegments.CATALOG);
	}

	public CatalogName(final String catalog) {
		super(new BaseNameImpl(checkNotNull(catalog, "catalog"), null, null,
				null), NameSegments.CATALOG);
	}

	public SchemaName getSchemaName(String name) {
		return new SchemaName(getBaseName().getCatalog(), name);
	}

	@Override
	protected String createName(String separator) {
		return StringUtils.defaultString(getCatalog(), "");
	}

	@Override
	public String getShortName() {
		return getCatalog();
	}

	@Override
	public String toString() {
		return getCatalog();
	}

	@Override
	public CatalogName clone(NameSegments segs) {
		return new CatalogName(this);
	}
}
package org.xenei.jdbc4sparql.iface.name;

import org.apache.commons.lang.StringUtils;

/**
 * Name implementation.
 */
public class SchemaName extends ItemName {
	static ItemName checkItemName(ItemName name) {
		CatalogName.checkItemName(name);
		checkNotNull(name.getBaseName().getSchema(), "schema");
		return name;
	}

	private static NameSegments adjustSegments(NameSegments segments) {
		if (segments.isSchema() && !segments.isTable() && !segments.isColumn()) {
			return segments;
		}
		return new NameSegments(segments.isCatalog(), true, false, false);
	}

	public SchemaName(final ItemName name) {
		this(checkItemName(name), name.getUsedSegments());
	}

	public SchemaName(final ItemName name, NameSegments segments) {
		super(checkItemName(name), adjustSegments(segments));
	}

	public SchemaName(final String catalog, final String schema) {
		super(new BaseNameImpl(checkNotNull(catalog, "catalog"), checkNotNull(
				schema, "schema"), null, null), NameSegments.SCHEMA);
	}

	public TableName getTableName(final String tblName) {
		BaseName baseName = getBaseName();
		return new TableName(baseName.getCatalog(), baseName.getSchema(),
				tblName);
	}

	@Override
	protected String createName(final String separator) {
		return StringUtils.defaultString(getSchema());
	}

	@Override
	public String getShortName() {
		return getSchema();
	}

	@Override
	public SchemaName clone(NameSegments segs) {
		return new SchemaName(this, segs);
	}
}
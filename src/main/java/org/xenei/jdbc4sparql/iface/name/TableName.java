package org.xenei.jdbc4sparql.iface.name;

import org.apache.commons.lang.StringUtils;
import org.xenei.jdbc4sparql.impl.NameUtils;

/**
 * Name implementation.
 */
public class TableName extends ItemName {
	public static TableName getNameInstance(String catalog, String schema,
			final String alias) {
		if (alias == null) {
			throw new IllegalArgumentException("Alias must be provided");
		}
		if (alias.contains(NameUtils.DB_DOT)
				&& alias.contains(NameUtils.SPARQL_DOT)) {
			throw new IllegalArgumentException(String.format(
					"Name may not cointain both '%s' and '%s'",
					NameUtils.DB_DOT, NameUtils.SPARQL_DOT));
		}

		String separator = alias.contains(NameUtils.DB_DOT) ? "\\"
				+ NameUtils.DB_DOT : NameUtils.SPARQL_DOT;

		final String[] parts = alias.split(separator);
		switch (parts.length) {
		case 2:
			return new TableName(catalog, parts[0], parts[1]);
		case 1:
			return new TableName(catalog, schema, parts[0]);

		default:
			throw new IllegalArgumentException(String.format(
					"Column name must be 1 or 2 segments not %s as in %s",
					parts.length, alias));

		}
	}

	static ItemName checkItemName(ItemName name) {
		SchemaName.checkItemName(name);
		checkNotNull(name.getBaseName().getTable(), "table");
		return name;
	}

	private static NameSegments adjustSegments(NameSegments segments) {
		if (segments.isTable() && !segments.isColumn()) {
			return segments;
		}
		return new NameSegments(segments.isCatalog(), segments.isSchema(),
				true, false);
	}

	public TableName(final ItemName name) {
		this(name, name.getUsedSegments());
	}

	public TableName(final ItemName name, NameSegments segments) {
		super(checkItemName(name), adjustSegments(segments));
	}

	public TableName(final String catalog, final String schema,
			final String table) {
		super(new BaseNameImpl(checkNotNull(catalog, "catalog"), checkNotNull(
				schema, "schema"), checkNotNull(table, "table"), null),
				NameSegments.TABLE);
	}

	@Override
	public String createName(final String separator) {
		final StringBuilder sb = new StringBuilder();

		if (StringUtils.isNotEmpty(getSchema())) {
			sb.append(getSchema()).append(separator);
		}

		if (StringUtils.isNotEmpty(getTable()) || (sb.length() > 0)) {
			sb.append(getTable());
		}

		return sb.toString();
	}

	public ColumnName getColumnName(final String colName) {
		return ColumnName.getNameInstance(this, colName);
	}

	@Override
	public String getShortName() {
		return getTable();
	}

	@Override
	public TableName clone(NameSegments segs) {
		return new TableName(this, segs);
	}
}
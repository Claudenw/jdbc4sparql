package org.xenei.jdbc4sparql.iface.name;

import org.apache.commons.lang.StringUtils;
import org.xenei.jdbc4sparql.impl.NameUtils;

/**
 * The name for the QueryColumnInfo.
 */
public class ColumnName extends ItemName {
	public static ColumnName getNameInstance(ItemName item, final String name) {
		BaseName bn = item.getBaseName();
		ColumnName retval = getNameInstance(bn.getCatalog(), bn.getSchema(),
				bn.getTable(), name);
		retval.setUsedSegments(adjustSegments(item.getUsedSegments()));
		return retval;
	}

	public static ColumnName getNameInstance(final String catalog,
			final String schema, final String table, final String name) {
		checkNotNull(name, "column");
		if (name.contains(NameUtils.DB_DOT)
				&& name.contains(NameUtils.SPARQL_DOT)) {
			throw new IllegalArgumentException(String.format(
					"Name may not cointain both '%s' and '%s'",
					NameUtils.DB_DOT, NameUtils.SPARQL_DOT));
		}

		String separator = name.contains(NameUtils.DB_DOT) ? "\\"
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

	static ItemName checkItemName(ItemName name) {
		TableName.checkItemName(name);
		checkNotNull(name.getBaseName().getCol(), "column");
		return name;
	}

	private static NameSegments adjustSegments(NameSegments segments) {
		if (segments.isColumn()) {
			return segments;
		}
		return new NameSegments(segments.isCatalog(), segments.isSchema(),
				segments.isTable(), true);
	}

	public ColumnName(final ItemName name) {
		this(name, name.getUsedSegments());
	}

	public ColumnName(final ItemName name, NameSegments segments) {
		super(checkItemName(name), adjustSegments(segments));
	}

	public ColumnName(final String catalog, final String schema,
			final String table, final String col) {
		super(new BaseNameImpl(checkNotNull(catalog, "catalog"), checkNotNull(
				schema, "schema"), checkNotNull(table, "table"), checkNotNull(
				col, "column")), NameSegments.COLUMN);
	}

	@Override
	public String getShortName() {
		return getCol();
	}

	public TableName getTableName() {
		return new TableName(this);
	}

	public ColumnName merge(final ColumnName other) {
		BaseName otherBase = other.getBaseName();

		return new ColumnName(StringUtils.defaultIfEmpty(getBaseName()
				.getCatalog(), otherBase.getCatalog()),
				StringUtils.defaultIfEmpty(getBaseName().getSchema(),
						otherBase.getSchema()), StringUtils.defaultIfEmpty(
						getBaseName().getTable(), otherBase.getTable()),
				StringUtils.defaultIfEmpty(getBaseName().getCol(),
						otherBase.getCol()));
	}

	@Override
	protected String createName(String separator) {

		final StringBuilder sb = new StringBuilder();

		if (StringUtils.isNotEmpty(getSchema())) {
			sb.append(getSchema()).append(separator);
		}

		String tbl = StringUtils.defaultString(getTable());
		if (tbl.length() > 0 || (sb.length() > 0)) {
			sb.append(tbl).append(separator);
		}

		if (StringUtils.isNotEmpty(getCol())) {
			sb.append(getCol());
		}
		return sb.toString();

	}

	@Override
	public ColumnName clone(NameSegments segs) {
		return new ColumnName(this, segs);
	}
}
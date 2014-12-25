package org.xenei.jdbc4sparql.iface.name;

/**
 * An ItemName implementation that is used for searching lists of ItemNames.
 *
 */
public class SearchName extends ItemName {

	public SearchName(final String catalog, final String schema,
			final String table, final String column) {
		this(catalog, schema, table, column, NameSegments.getInstance(
				catalog != null, schema != null, table != null, column != null));
	}

	public SearchName(final String catalog, final String schema,
			final String table, final String col, final NameSegments segs) {
		super(catalog, schema, table, col, segs);
	}

	public SearchName(final ItemName name, final NameSegments segments) {
		super(name, segments);
	}

	public SearchName(final FQName name, final NameSegments segments) {
		super(name, segments);
	}

	@Override
	protected String createName(final String separator) {
		return String.format("%s%s%s%s%s%s%s", getCatalog(), separator,
				getSchema(), separator, getTable(), separator, getColumn());
	}

	@Override
	public String getShortName() {
		final NameSegments ns = getUsedSegments();
		if (ns.isColumn()) {
			return getColumn();
		}
		if (ns.isTable()) {
			return getTable();
		}
		if (ns.isSchema()) {
			return getSchema();
		}
		if (ns.isCatalog()) {
			return getCatalog();
		}
		return "";
	}

	@Override
	public ItemName clone(final NameSegments segs) {
		return new SearchName(this, segs);
	}

}

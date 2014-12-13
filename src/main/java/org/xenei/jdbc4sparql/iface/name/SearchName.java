package org.xenei.jdbc4sparql.iface.name;

/**
 * An ItemName implementation that is used for searching lists of ItemNames.
 *
 */
public class SearchName extends ItemName {

	public SearchName(String catalog, String schema, String table, String column) {
		this(catalog, schema, table, column, NameSegments.getInstance(catalog != null,
				schema != null, table != null, column != null));
	}

	public SearchName(String catalog, String schema, String table, String col,
			NameSegments segs) {
		super(catalog, schema, table, col, segs);
	}

	public SearchName(ItemName name, NameSegments segments) {
		super(name, segments);
	}

	public SearchName(FQName name, NameSegments segments) {
		super(name, segments);
	}

	@Override
	protected String createName(String separator) {
		return String.format("%s%s%s%s%s%s%s", getCatalog(), separator,
				getSchema(), separator, getTable(), separator, getColumn());
	}

	@Override
	public String getShortName() {
		NameSegments ns = getUsedSegments();
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
	public ItemName clone(NameSegments segs) {
		return new SearchName(this, segs);
	}

}

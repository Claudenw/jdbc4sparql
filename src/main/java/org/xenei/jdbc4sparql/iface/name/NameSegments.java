package org.xenei.jdbc4sparql.iface.name;

public class NameSegments {
	public static final NameSegments ALL = new NameSegments(true, true, true,
			true);
	public static final NameSegments WILD = new NameSegments(false, false,
			false, false);
	public static final NameSegments CATALOG = new NameSegments(true, false,
			false, false);
	public static final NameSegments SCHEMA = new NameSegments(false, true,
			false, false);
	public static final NameSegments TABLE = new NameSegments(false, true,
			true, false);
	public static final NameSegments COLUMN = new NameSegments(false, true,
			true, true);

	private boolean catalog;
	private boolean schema;
	private boolean table;
	private boolean column;

	public NameSegments(final boolean catalog, final boolean schema,
			final boolean table, final boolean column) {
		this.catalog = catalog;
		this.schema = schema;
		this.table = table;
		this.column = column;
	}

	public String getCatalog(final BaseName name) {
		return catalog ? name.getCatalog() : null;
	}

	public String getColumn(final BaseName name) {
		return column ? name.getCol() : null;
	}

	public String getSchema(final BaseName name) {
		return schema ? name.getSchema() : null;
	}

	public String getTable(final BaseName name) {
		return table ? name.getTable() : null;
	}

	public boolean isCatalog() {
		return catalog;
	}

	public boolean isSchema() {
		return schema;
	}

	public boolean isTable() {
		return table;
	}

	public boolean isColumn() {
		return column;
	}

	@Override
	public String toString() {
		return String.format("C:%s S:%s T:%s C:%s", catalog, schema, table,
				column);
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof NameSegments) {
			NameSegments other = (NameSegments) o;
			return this.isCatalog() == other.isCatalog()
					&& this.isSchema() == other.isSchema()
					&& this.isTable() == other.isTable()
					&& this.isColumn() == other.isColumn();
		}
		return false;
	}

	@Override
	public int hashCode() {
		return (isCatalog() ? 8 : 0) + (isSchema() ? 4 : 0)
				+ (isTable() ? 2 : 0) + (isColumn() ? 1 : 0);

	}

}
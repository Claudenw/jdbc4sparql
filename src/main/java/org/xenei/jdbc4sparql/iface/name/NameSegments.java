package org.xenei.jdbc4sparql.iface.name;

/**
 * An immutable bitmap of name segments to be displayed.
 *
 */
public class NameSegments {

	public static final NameSegments FFFF = new NameSegments(false, false,
			false, false);
	public static final NameSegments FFFT = new NameSegments(false, false,
			false, true);
	public static final NameSegments FFTF = new NameSegments(false, false,
			true, false);
	public static final NameSegments FFTT = new NameSegments(false, false,
			true, true);
	public static final NameSegments FTFF = new NameSegments(false, true,
			false, false);
	public static final NameSegments FTFT = new NameSegments(false, true,
			false, true);
	public static final NameSegments FTTF = new NameSegments(false, true, true,
			false);
	public static final NameSegments FTTT = new NameSegments(false, true, true,
			true);
	public static final NameSegments TFFF = new NameSegments(true, false,
			false, false);
	public static final NameSegments TFFT = new NameSegments(true, false,
			false, true);
	public static final NameSegments TFTF = new NameSegments(true, false, true,
			false);
	public static final NameSegments TFTT = new NameSegments(true, false, true,
			true);
	public static final NameSegments TTFF = new NameSegments(true, true, false,
			false);
	public static final NameSegments TTFT = new NameSegments(true, true, false,
			true);
	public static final NameSegments TTTF = new NameSegments(true, true, true,
			false);
	public static final NameSegments TTTT = new NameSegments(true, true, true,
			true);

	private static final NameSegments[] LST = {
			FFFF, FFFT, FFTF, FFTT, FTFF, FTFT, FTTF, FTTT, TFFF, TFFT, TFTF,
			TFTT, TTFF, TTFT, TTTF, TTTT
	};

	/**
	 * All segments on.
	 */
	public static final NameSegments ALL = TTTT;
	/**
	 * All segments off
	 */
	public static final NameSegments WILD = FFFF;
	/**
	 * Standard catalog name
	 */
	public static final NameSegments CATALOG = TFFF;
	/**
	 * Standard schema name
	 */
	public static final NameSegments SCHEMA = FTFF;
	/**
	 * Standard table name.
	 */
	public static final NameSegments TABLE = FTTF;
	/**
	 * Standard column name.
	 */
	public static final NameSegments COLUMN = FTTT;

	// the catalog flag
	private final boolean catalog;
	// the schema flag
	private final boolean schema;
	// the table flag
	private final boolean table;
	// the column flag
	private final boolean column;

	/**
	 * Get an instance of the Name segments.
	 *
	 * @param catalog
	 *            the display catalog flag.
	 * @param schema
	 *            the display schema flag.
	 * @param table
	 *            the display table flag.
	 * @param column
	 *            the display column flag.
	 */
	public static NameSegments getInstance(final boolean catalog,
			final boolean schema, final boolean table, final boolean column) {
		final int idx = (catalog ? 8 : 0) + (schema ? 4 : 0) + (table ? 2 : 0)
				+ (column ? 1 : 0);
		return LST[idx];
	}

	/**
	 * Constructor.
	 *
	 * @param catalog
	 *            the display catalog flag.
	 * @param schema
	 *            the display schema flag.
	 * @param table
	 *            the display table flag.
	 * @param column
	 *            the display column flag.
	 */
	private NameSegments(final boolean catalog, final boolean schema,
			final boolean table, final boolean column) {
		this.catalog = catalog;
		this.schema = schema;
		this.table = table;
		this.column = column;
	}

	/**
	 * Get the catalog to display from the name.
	 *
	 * @param name
	 *            The name to filter.
	 * @return The catalog name or null if not displayed.
	 */
	public String getCatalog(final FQName name) {
		return catalog ? name.getCatalog() : null;
	}

	/**
	 * Get the column to display from the name.
	 *
	 * @param name
	 *            The name to filter.
	 * @return The column name or null if not displayed.
	 */
	public String getColumn(final FQName name) {
		return column ? name.getColumn() : null;
	}

	/**
	 * Get the schema to display from the name.
	 *
	 * @param name
	 *            The name to filter.
	 * @return The schema name or null if not displayed.
	 */
	public String getSchema(final FQName name) {
		return schema ? name.getSchema() : null;
	}

	/**
	 * Get the table to display from the name.
	 *
	 * @param name
	 *            The name to filter.
	 * @return The table name or null if not displayed.
	 */
	public String getTable(final FQName name) {
		return table ? name.getTable() : null;
	}

	/**
	 * get the catalog flag state.
	 *
	 * @return true if the catalog is displayed, false otherwise.
	 */
	public boolean isCatalog() {
		return catalog;
	}

	/**
	 * get the schema flag state.
	 *
	 * @return true if the schema is displayed, false otherwise.
	 */
	public boolean isSchema() {
		return schema;
	}

	/**
	 * get the table flag state.
	 *
	 * @return true if the table is displayed, false otherwise.
	 */
	public boolean isTable() {
		return table;
	}

	/**
	 * get the column flag state.
	 *
	 * @return true if the column is displayed, false otherwise.
	 */
	public boolean isColumn() {
		return column;
	}

	@Override
	public String toString() {
		return String.format("C:%s S:%s T:%s C:%s", catalog, schema, table,
				column);
	}

	/**
	 * Equality is determined by the flags matching.
	 */
	@Override
	public boolean equals(final Object o) {
		if (o instanceof NameSegments) {
			final NameSegments other = (NameSegments) o;
			return (this.isCatalog() == other.isCatalog())
					&& (this.isSchema() == other.isSchema())
					&& (this.isTable() == other.isTable())
					&& (this.isColumn() == other.isColumn());
		}
		return false;
	}

	@Override
	public int hashCode() {
		return (isCatalog() ? 8 : 0) + (isSchema() ? 4 : 0)
				+ (isTable() ? 2 : 0) + (isColumn() ? 1 : 0);

	}

	/**
	 * And this name segment with the other. All segments that are on in both
	 * will be on in result. All others will be off.
	 * 
	 * @param other
	 *            The other segment to be ANDed with this one.
	 * @return merged NameSegments object
	 */
	NameSegments and(final NameSegments other) {
		final int idx = hashCode() & other.hashCode();
		return LST[idx];
	}
}
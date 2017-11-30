package org.xenei.jdbc4sparql.iface;

import org.xenei.jdbc4sparql.iface.name.FQName;

/**
 * An immutable bitmap of name segments to be displayed.
 *
 */
public enum NameSegments {

	 FFFF( (byte)0 ),
	 FFFT( (byte)1 ),
	FFTF( (byte)2 ),
	FFTT( (byte)3 ),
	FTFF( (byte)4 ),
	FTFT( (byte)5 ),
	FTTF( (byte)6 ),
	FTTT( (byte)7 ),
	TFFF( (byte)8 ),
	TFFT( (byte)9 ),
	TFTF( (byte)10 ),
	TFTT( (byte)11 ),
	TTFF( (byte)12 ),
	TTFT( (byte)13 ),
	TTTF( (byte)14 ),
	TTTT( (byte)15 );
	
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

	private final byte flag;
	
	private static final byte CAT_B = (byte)1 << 3;
	private static final byte SCH_B = (byte)1 << 2;
	private static final byte TBL_B = (byte)1 << 1;
	private static final byte COL_B = (byte)1;
	
	

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
		return LST[ makeIdx( catalog, schema, table, column )];
	}
	
	private static byte makeIdx( final boolean catalog,
			final boolean schema, final boolean table, final boolean column) {
		return (byte)((catalog?CAT_B:0) | (schema?SCH_B:0) | (table?TBL_B:0) | (column?COL_B:0));
	}
	
	private NameSegments( byte flag) {
		this.flag=flag;
	}

	public NameSegments getLastSegment()
	{
		if (isColumn())
		{
			return LST[COL_B];
		}
		if (isTable())
		{
			return LST[TBL_B];
		}
		if (isSchema())
		{
			return LST[SCH_B];
		}
		if (isCatalog())
		{
			return LST[CAT_B];
		}
		return FFFF;
	}
	/**
	 * Get the catalog to display from the name.
	 *
	 * @param name
	 *            The name to filter.
	 * @return The catalog name or null if not displayed.
	 */
	public String getCatalog(final FQName name) {
		return isCatalog() ? name.getCatalog() : null;
	}
	
	public boolean match( NameSegments other )
	{
		return (flag & other.flag) > 0;
	}

	/**
	 * Get the column to display from the name.
	 *
	 * @param name
	 *            The name to filter.
	 * @return The column name or null if not displayed.
	 */
	public String getColumn(final FQName name) {
		return isColumn() ? name.getColumn() : null;
	}

	/**
	 * Get the schema to display from the name.
	 *
	 * @param name
	 *            The name to filter.
	 * @return The schema name or null if not displayed.
	 */
	public String getSchema(final FQName name) {
		return isSchema() ? name.getSchema() : null;
	}

	/**
	 * Get the table to display from the name.
	 *
	 * @param name
	 *            The name to filter.
	 * @return The table name or null if not displayed.
	 */
	public String getTable(final FQName name) {
		return isTable() ? name.getTable() : null;
	}

	/**
	 * get the catalog flag state.
	 *
	 * @return true if the catalog is displayed, false otherwise.
	 */
	public boolean isCatalog() {
		return ((flag & CAT_B) == CAT_B);
	}

	/**
	 * get the schema flag state.
	 *
	 * @return true if the schema is displayed, false otherwise.
	 */
	public boolean isSchema() {
		return ((flag & SCH_B) == SCH_B);
	}

	/**
	 * get the table flag state.
	 *
	 * @return true if the table is displayed, false otherwise.
	 */
	public boolean isTable() {
		return ((flag & TBL_B) == TBL_B);
	}

	/**
	 * get the column flag state.
	 *
	 * @return true if the column is displayed, false otherwise.
	 */
	public boolean isColumn() {
		return ((flag & COL_B) == COL_B);
	}

	@Override
	public String toString() {
		return String.format("%s%s%s%s", isCatalog()?"T":"F", isSchema()?"T":"F", isTable()?"T":"F",
				isColumn()?"T":"F");
	}

	/**
	 * And this name segment with the other. All segments that are on in both
	 * will be on in result. All others will be off.
	 *
	 * @param other
	 *            The other segment to be ANDed with this one.
	 * @return merged NameSegments object
	 */
	public NameSegments and(final NameSegments other) {
		final int idx = flag & other.flag;
		return LST[idx];
	}
	
	/**
	 * And this name segment with the other. All segments that are on in both
	 * will be on in result. All others will be off.
	 *
	 * @param other
	 *            The other segment to be ANDed with this one.
	 * @return merged NameSegments object
	 */
	public NameSegments or(final NameSegments other) {
		final int idx = flag | other.flag;
		return LST[idx];
	}
}
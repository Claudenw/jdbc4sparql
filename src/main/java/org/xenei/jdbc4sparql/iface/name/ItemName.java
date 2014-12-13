package org.xenei.jdbc4sparql.iface.name;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.xenei.jdbc4sparql.impl.NameUtils;

import java.util.Comparator;

/**
 * A name for an item in a JDBC query.  This name wraps an FQName for the underlying object with 
 * an FQName used in the query.  The FQName used in the query may contain NULLs.
 *
 */
public abstract class ItemName implements GUIDObject {
	/**
	 * Compaes ItemNames.
	 */
	public static Comparator<ItemName> COMPARATOR = new Comparator<ItemName>() {

		@Override
		public int compare(ItemName arg0, ItemName arg1) {
			return new CompareToBuilder()
					.append(arg0.getCatalog(), arg1.getCatalog())
					.append(arg0.getSchema(), arg1.getSchema())
					.append(arg0.getTable(), arg1.getTable())
					.append(arg0.getColumn(), arg1.getColumn()).toComparison();
		}
	};

	/**
	 * Filters an ExtendedIterator for items that match this name using the compare filter.
	 * @author claude
	 *
	 * @param <T> and ItemName implementation.
	 */
	public static class Filter<T extends ItemName> extends
			com.hp.hpl.jena.util.iterator.Filter<T> {
		private final ItemName compareTo;

		public Filter(final ItemName compareTo) {
			this.compareTo = compareTo;
		}

		@Override
		public boolean accept(T o) {
			return COMPARATOR.compare(o, compareTo) == 0;
		};
	}

	/**
	 * Verify that a segment is not null.  Similar to FQNameImpl.verifyOK except that it does not
	 * check for the presence of the JDBC or SPARQL "Dot" characters.  Used in constructors
	 * to verify values.
	 * @param value value.
	 * @param segment the segment we are checking.
	 * @return the value
	 * @Throws IllegalArgumentException if a null value is passed
	 */
	public static String checkNotNull(String value, String segment) throws IllegalArgumentException {
		if (value == null) {
			throw new IllegalArgumentException(String.format(
					"Segment %s may not be null", segment));
		}
		return value;
	}

	// the base name for the object.
	private final FQName baseName;
	// the bitmap of the used segments.
	private NameSegments usedSegments;
	// if true then this object reports its name as it's GUID.
	private boolean useGUID;

	/**
	 * A wild item name.  All display segments are null so it matches all other item names.
	 */
	public static final ItemName WILD = new ItemName("", "", "", "",
			NameSegments.WILD) {

		@Override
		protected String createName(String separator) {
			return getShortName();
		}

		@Override
		public String getShortName() {
			return "Wild Name";
		}

		@Override
		public void setUsedSegments(NameSegments usedSegments) {
			throw new UnsupportedOperationException(
					"WILD may not change used segments");
			// do nothing
		}

		@Override
		public ItemName clone(NameSegments segs) {
			return WILD;
		}

	};

	/**
	 * Constructor.
	 * Creates an Item name with the associated name segments and a Column name segment pattern.
	 * None of the column segments may be null.
	 * @param catalog
	 * @param schema
	 * @param table
	 * @param column
	 * @throws IllegalArgumentException
	 */
	protected ItemName(final String catalog, final String schema,
			final String table, final String column) throws IllegalArgumentException {
		this(catalog, schema, table, column, NameSegments.COLUMN);
	}

	/**
	 * Constructor.
	 * Creates an ItemName with the associated name segments and segment pattern. 
	 * None of the arguments may be null.
	 * @param catalog
	 * @param schema
	 * @param table
	 * @param column
	 * @param segs
	 * @throws IllegalArgumentException
	 */
	protected ItemName(final String catalog, final String schema,
			final String table, final String column, NameSegments segs) throws IllegalArgumentException {
		baseName = new FQNameNameImpl(catalog, schema, table, column);
		if (segs == null)
		{
			throw new IllegalArgumentException( "segs may not be null.");
		}
		this.usedSegments = segs;
	}

	/**
	 * Constructor.
	 * Creates an ItemName from the base name of the provided name as along with the segments.
	 * none of the values may be null.
	 * @param name The base name
	 * @param segments Then name segments.
	 * @throws IllegalArgumentException
	 */
	protected ItemName(final ItemName name, NameSegments segments) throws IllegalArgumentException{
		if (name == null)
		{
			throw new IllegalArgumentException( "name may not be null.");
		}
		if (segments == null)
		{
			throw new IllegalArgumentException( "segments may not be null.");
		}
		this.baseName = name.baseName;
		this.useGUID = name.useGUID;
		this.usedSegments = segments;
	}

	/**
	 * Constructor.
	 * Creates an ItemName from a FQName and a name segments.  None of the values may be null.
	 * @param name THe FQName for the base name.
	 * @param segments The name segments.
	 * @throws IllegalArgumentException
	 */
	protected ItemName(final FQName name, NameSegments segments) throws IllegalArgumentException{
		this.baseName = name;
		this.usedSegments = segments;
	}

	/** 
	 * If set true this item will refer to itself by its GUID.
	 * @param state
	 */
	public void setUseGUID(boolean state) {
		this.useGUID = state;
	}

	/**
	 * Change the used segments.  
	 * This effectively changes the name of the object as seen in the query.
	 * @param usedSegments the name segments to use.
	 */
	public void setUsedSegments(NameSegments usedSegments) {
		this.usedSegments = usedSegments;
	}

	/**
	 * Get the namesegments.
	 * @return The current name segments.
	 */
	public NameSegments getUsedSegments() {
		return usedSegments;
	}

	/**
	 * create the fully qualified name for this item.
	 * 
	 * @param separator
	 *            The string to use between name segments
	 * @return The fully qualified name
	 */
	abstract protected String createName(final String separator);

	/**
	 * Get the base name for this item name.
	 * @return
	 */
	protected FQName getBaseName() {
		return baseName;
	}

	/**
	 * Get the column name string
	 *
	 * @return
	 */
	public String getColumn() {
		return usedSegments.getColumn(baseName);
	}

	/**
	 * Get the name in JDBC format
	 *
	 * @return
	 */
	public String getDBName() {
		return useGUID ? getGUID() : createName(NameUtils.DB_DOT);
	}

	/**
	 * Get the catalog name segment.
	 * @return The catalog name string.
	 */
	public String getCatalog() {
		return usedSegments.getCatalog(baseName);
	}

	/**
	 * Get the schema segment of the name.
	 *
	 * @return the schema segment string.
	 */
	public String getSchema() {
		return usedSegments.getSchema(baseName);
	}

	/**
	 * Get the shrot name.  This is the last name segment generally used by the ItemName type.
	 * (e.g. the columnName for a column object).
	 * @return the short name for the object.
	 */
	abstract public String getShortName();

	/**
	 * Get the complete name in SPARQL format
	 *
	 * @return
	 */
	public String getSPARQLName() {
		return useGUID ? getGUID() : createName(NameUtils.SPARQL_DOT);
	}

	/**
	 * Get the name as a UUID of the base name.
	 * 
	 * @return the UUID based name
	 */
	@Override
	public String getGUID() {
		return baseName.getGUID();
	}

	/**
	 * Get the table portion of the complete name.
	 *
	 * @return the table name string
	 */
	public String getTable() {
		return usedSegments.getTable(baseName);
	}

	/**
	 * See if any displayed name segment is the wild value (null).
	 * @return true if this has a wildcard (null) segment
	 */
	public boolean hasWild() {
		return (getCatalog() == null) || (getSchema() == null)
				|| (getTable() == null) || (getColumn() == null);
	}

	/**
	 * See if this is the wild card.
	 * @return true if this is a complete wildcard name (e.g. all segments are
	 *         null)
	 */
	public boolean isWild() {
		return (getCatalog() == null) && (getSchema() == null)
				&& (getTable() == null) && (getColumn() == null);
	}

	/**
	 * See if we are using the GUID as the name.
	 * @return true if GUID is used as the name.
	 */
	public boolean isUseGUID() {
		return useGUID;
	}

	/**
	 * check if this matches that.
	 *
	 * AND the usedSegments from this ItemName with the usedSegments from that ItemName.  Any values
	 * that are then selected are compared from both ItemNames.
	 * @param that the other ItemName.
	 * @return true if they match, false otherwise.
	 */
	public boolean matches(final ItemName that) {
		if (that == null) {
			return false;
		}
		NameSegments matchSegs = usedSegments.and( that.getUsedSegments() );
		final EqualsBuilder eb = new EqualsBuilder();
		if (matchSegs.isCatalog()) {
			eb.append(matchSegs.getCatalog( this.baseName), matchSegs.getCatalog(that.getBaseName()));
		}
		if (matchSegs.isSchema()) {
			eb.append(matchSegs.getSchema( this.baseName), matchSegs.getSchema(that.getBaseName()));
		}
		if (matchSegs.isTable()) {
			eb.append(matchSegs.getTable( this.baseName), matchSegs.getTable(that.getBaseName()));
		}
		if (matchSegs.isColumn()) {
			eb.append(matchSegs.getColumn( this.baseName), matchSegs.getColumn(that.getBaseName()));
		}
		return eb.isEquals();
	}

	@Override
	public String toString() {
		if (isWild()) {
			return "Wildcard Name";
		}
		return StringUtils.defaultIfBlank(getDBName(), "Blank Name");
	}

	@Override
	public int hashCode() {
		return getGUID().hashCode();
	}

	/**
	 * Items are equal if their sparqlNames are equal.
	 */
	@Override
	public boolean equals(Object o) {
		return (o instanceof ItemName) ? getSPARQLName().equals(
				((ItemName) o).getSPARQLName()) : false;
	}

	/**
	 * Clone this name but change the segments being used.
	 * @param segs
	 * @return
	 */
	abstract public ItemName clone(NameSegments segs);
}
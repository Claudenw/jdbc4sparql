package org.xenei.jdbc4sparql.iface.name;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.xenei.jdbc4sparql.impl.NameUtils;

import java.util.Comparator;

public abstract class ItemName implements GUIDObject {
	public static Comparator<ItemName> COMPARATOR = new Comparator<ItemName>() {

		@Override
		public int compare(ItemName arg0, ItemName arg1) {
			return new CompareToBuilder()
					.append(arg0.getCatalog(), arg1.getCatalog())
					.append(arg0.getSchema(), arg1.getSchema())
					.append(arg0.getTable(), arg1.getTable())
					.append(arg0.getCol(), arg1.getCol()).toComparison();
		}
	};

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

	public static String checkNotNull(String value, String segment) {
		if (value == null) {
			throw new IllegalArgumentException(String.format(
					"Segment %s may not be null", segment));
		}
		return value;
	}

	public static final String FOUND_IN_MULTIPLE_ = "%s was found in multiple %s";
	private final BaseName baseName;
	private NameSegments usedSegments;
	private boolean useGUID;

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

	protected ItemName(final String catalog, final String schema,
			final String table, final String col) {
		this(catalog, schema, table, col, NameSegments.COLUMN);
	}

	protected ItemName(final String catalog, final String schema,
			final String table, final String col, NameSegments segs) {
		baseName = new BaseNameImpl(catalog, schema, table, col);
		this.usedSegments = segs;
	}

	protected ItemName(final ItemName name, NameSegments segments) {
		this.baseName = name.baseName;
		this.useGUID = name.useGUID;
		this.usedSegments = segments;
	}

	protected ItemName(final BaseName name, NameSegments segments) {
		this.baseName = name;
		this.usedSegments = segments;
	}

	public void setUseGUID(boolean state) {
		this.useGUID = state;
	}

	public void setUsedSegments(NameSegments usedSegments) {
		this.usedSegments = usedSegments;
	}

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

	protected BaseName getBaseName() {
		return baseName;
	}

	/**
	 * Get the column name string
	 *
	 * @return
	 */
	public String getCol() {
		return usedSegments.getColumn(baseName);
	}

	/**
	 * Get the name in DB format
	 *
	 * @return
	 */
	public String getDBName() {
		return useGUID ? getGUID() : createName(NameUtils.DB_DOT);
	}

	public String getCatalog() {
		return usedSegments.getCatalog(baseName);
	}

	/**
	 * Get the schema segment of the name.
	 *
	 * @return
	 */
	public String getSchema() {
		return usedSegments.getSchema(baseName);
	}

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
	 * Get the name as a UUID based on the real name.
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
	 * @return
	 */
	public String getTable() {
		return usedSegments.getTable(baseName);
	}

	/**
	 * @return true if this has a wildcard (null) segment
	 */
	public boolean hasWild() {
		return (getCatalog() == null) || (getSchema() == null)
				|| (getTable() == null) || (getCol() == null);
	}

	/**
	 * @return true if this is a complete wildcard name (e.g. all segments are
	 *         null)
	 */
	public boolean isWild() {
		return (getCatalog() == null) && (getSchema() == null)
				&& (getTable() == null) && (getCol() == null);
	}

	public boolean isUseGUID() {
		return useGUID;
	}

	/**
	 * check if this matches that.
	 *
	 * this matches that if this.equals( that ) or if this.schema, this.table,
	 * this.col == null then that segment is not used in the comparison.
	 *
	 * Note that this is a.matches(b) does not imply b.matches(a)
	 *
	 * @param n
	 * @return
	 */
	public boolean matches(final ItemName that) {
		if (that == null) {
			return false;
		}
		final EqualsBuilder eb = new EqualsBuilder();
		if (this.getCatalog() != null) {
			eb.append(this.getCatalog(), that.getCatalog());
		}
		if (this.getSchema() != null) {
			eb.append(this.getSchema(), that.getSchema());
		}
		if (this.getTable() != null) {
			eb.append(this.getTable(), that.getTable());
		}
		if (this.getCol() != null) {
			eb.append(this.getCol(), that.getCol());
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

	@Override
	public boolean equals(Object o) {
		return (o instanceof ItemName) ? getSPARQLName().equals(
				((ItemName) o).getSPARQLName()) : false;
	}

	abstract public ItemName clone(NameSegments segs);
}
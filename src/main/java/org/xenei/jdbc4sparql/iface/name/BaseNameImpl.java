package org.xenei.jdbc4sparql.iface.name;

import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.impl.NameUtils;

public class BaseNameImpl implements BaseName {
	protected static String verifyOK(final String segment, final String value) {
		if (value != null) {
			for (final String badChar : NameUtils.DOT_LIST) {
				if (value.contains(badChar)) {
					throw new IllegalArgumentException(String.format(
							"%s name may not contain '%s'", segment, badChar));
				}
			}
		}
		return value;
	}

	static final String FOUND_IN_MULTIPLE_ = "%s was found in multiple %s";

	private final String catalog;

	private final String schema;

	private final String table;

	private final String col;

	private final String guid;

	private transient Integer _hashCode;

	protected BaseNameImpl(final String catalog, final String schema,
			final String table, final String col) {
		this.catalog = verifyOK("Catalog", catalog);
		this.schema = verifyOK("Schema", schema);
		this.table = verifyOK("Table", table);
		this.col = verifyOK("Column", col);
		String t = StringUtils.defaultString(catalog)
				+ StringUtils.defaultString(schema)
				+ StringUtils.defaultString(table)
				+ StringUtils.defaultString(col);
		guid = "v_"
				+ (UUID.nameUUIDFromBytes(t.getBytes()).toString().replace("-",
						"_"));
	}

	/**
	 * Find the object matching the key in the map. Uses matches() method to
	 * determine match.
	 *
	 * @param map
	 *            The map to find the object in.
	 * @return The Object (T) or null if not found
	 * @throws IllegalArgumentException
	 *             if more than one object matches.
	 */
	public <T> T findGUID(final Map<? extends GUIDObject, T> map) {

		for (GUIDObject name : map.keySet()) {
			if (name.getGUID().equals(this.getGUID())) {
				return map.get(name);
			}
		}
		return null;
	}

	@Override
	public String getCatalog() {
		return catalog;
	}

	/**
	 * Get the column name string
	 *
	 * @return
	 */
	@Override
	public String getCol() {
		return col;
	}

	/**
	 * Get the schema segment of the name.
	 *
	 * @return
	 */
	@Override
	public String getSchema() {
		return schema;
	}

	/**
	 * Get the name as a UUID based on the real name.
	 * 
	 * @return the UUID based name
	 */
	@Override
	public String getGUID() {
		return guid;
	}

	/**
	 * Get the table portion of the complete name.
	 *
	 * @return
	 */
	@Override
	public String getTable() {
		return table;
	}

	@Override
	public int hashCode() {
		if (_hashCode == null) {
			_hashCode = BaseName.Comparator.hashCode(this);
		}
		return _hashCode;
	}

	@Override
	public boolean equals(Object o) {
		return BaseName.Comparator.equals(this, o);
	}

	@Override
	public String toString() {
		return String.format("%s.%s.%s.%s", catalog, schema, table, col);
	}

}
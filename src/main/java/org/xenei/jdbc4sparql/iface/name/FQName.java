package org.xenei.jdbc4sparql.iface.name;

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.xenei.jdbc4sparql.impl.NameUtils;

/**
 * A class that represents the Fully qualified name of a JDBC object.
 *
 * No segment of the FQName may be null.
 *
 * This is the name that included the Catalog, Schema, Table, and Column name
 * segments.
 *
 */
public interface FQName extends GUIDObject {

	/**
	 * A helper class for comparing FQNames. Provides implementations of
	 * compare(), equals() and hashCode().
	 *
	 */
	public static class Comparator implements java.util.Comparator<FQName> {
		@Override
		public int compare(final FQName fqName1, final FQName fqName2) {
			return new CompareToBuilder()
			.append(fqName1.getCatalog(), fqName2.getCatalog())
			.append(fqName1.getSchema(), fqName2.getSchema())
			.append(fqName1.getTable(), fqName2.getTable())
			.append(fqName1.getColumn(), fqName2.getColumn())
					.toComparison();
		}

		/**
		 * Determines if 2 FQNames are equal. A FQName is equal if all the name
		 * segments are equal.
		 *
		 * @param fqName1
		 *            the first FQName
		 * @param fqName2
		 *            the second FQName
		 * @return true if equal, false if not
		 */
		public static boolean equals(final Object fqName1, final Object fqName2) {
			if ((fqName1 instanceof FQName) && (fqName2 instanceof FQName)) {
				return ((FQName) fqName1).toString().equals(
						((FQName) fqName2).toString());
			}
			return false;
		}

		/**
		 * Generates a common hash code for the FQName based on all the name
		 * segments.
		 *
		 * @param fqName
		 * @return
		 */
		public static int hashCode(final FQName fqName) {
			return fqName.getGUID().hashCode();
		}

		public static String makeGUID(final String catalog,
				final String schema, final String table, final String column) {
			final String t = String.format( "%s%s%s%s%s%s%s", 
					StringUtils.defaultString(catalog),
					NameUtils.DB_DOT,
					 StringUtils.defaultString(schema),
						NameUtils.DB_DOT,
					 StringUtils.defaultString(table),
						NameUtils.DB_DOT,
					 StringUtils.defaultString(column));
			return "v_"
			+ (UUID.nameUUIDFromBytes(t.getBytes()).toString().replace(
							"-", "_"));
		}
	}

	/**
	 * Get the catalog name segment.
	 *
	 * @return The catalog name string.
	 */
	public String getCatalog();

	/**
	 * Get the column name segment.
	 *
	 * @return the column name string
	 */
	public String getColumn();

	/**
	 * Get the schema name segment.
	 *
	 * @return schema name string.
	 */
	public String getSchema();

	/**
	 * Get the table name segment.
	 *
	 * @return the talbe name string.
	 */
	public String getTable();

}

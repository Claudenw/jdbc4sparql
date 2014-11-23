package org.xenei.jdbc4sparql.iface.name;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public interface BaseName extends GUIDObject {

	public static class Comparator implements java.util.Comparator<BaseName> {
		@Override
		public int compare(BaseName arg0, BaseName arg1) {
			return new CompareToBuilder()
					.append(arg0.getCatalog(), arg1.getCatalog())
					.append(arg0.getSchema(), arg1.getSchema())
					.append(arg0.getTable(), arg1.getTable())
					.append(arg0.getCol(), arg1.getCol()).toComparison();
		}

		public static boolean equals(Object o1, Object o2) {
			if (o1 instanceof BaseName && o2 instanceof BaseName) {
				BaseName bn1 = (BaseName) o1;
				BaseName bn2 = (BaseName) o2;

				return new EqualsBuilder()
						.append(bn1.getCatalog(), bn2.getCatalog())
						.append(bn1.getSchema(), bn2.getSchema())
						.append(bn1.getTable(), bn2.getTable())
						.append(bn1.getCol(), bn2.getCol()).isEquals();
			}
			return false;
		}

		public static int hashCode(BaseName bn) {
			return new HashCodeBuilder().append(bn.getCatalog())
					.append(bn.getSchema()).append(bn.getTable())
					.append(bn.getCol()).toHashCode();

		}
	}

	public String getCatalog();

	/**
	 * Get the column name string
	 *
	 * @return
	 */
	public String getCol();

	/**
	 * Get the schema segment of the name.
	 *
	 * @return
	 */
	public String getSchema();

	public String getTable();

}

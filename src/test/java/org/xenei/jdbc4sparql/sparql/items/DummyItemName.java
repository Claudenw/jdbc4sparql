package org.xenei.jdbc4sparql.sparql.items;


import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;

public class DummyItemName extends ItemName {
	

	public DummyItemName(String catalog, String schema, String table, String column, NameSegments segs)
			throws IllegalArgumentException {
		super(catalog, schema, table, column, segs);
	}

		@Override
		public ItemName clone(NameSegments segs) {
			return new DummyItemName( this.getFQName().getCatalog(), this.getFQName().getSchema(),
					this.getFQName().getTable(), this.getFQName().getColumn(), segs);
		}
}
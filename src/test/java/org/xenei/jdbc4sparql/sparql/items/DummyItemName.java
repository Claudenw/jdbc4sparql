package org.xenei.jdbc4sparql.sparql.items;


import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;

public class DummyItemName extends ItemName {
	
	private final NameSegments dflt;
	public DummyItemName(String catalog, String schema, String table, String column, NameSegments segs)
			throws IllegalArgumentException {
		super(catalog, schema, table, column, segs);
		dflt = segs;
	}

		@Override
		public ItemName clone(NameSegments segs) {
			return new DummyItemName( this.getFQName().getCatalog(), this.getFQName().getSchema(),
					this.getFQName().getTable(), this.getFQName().getColumn(), segs);
		}

		@Override
		public NameSegments getDefaultSegments() {
			return dflt;
		}
}

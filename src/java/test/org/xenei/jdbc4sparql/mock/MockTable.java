package org.xenei.jdbc4sparql.mock;

import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.impl.DataTable;
import org.xenei.jdbc4sparql.impl.TableDefImpl;

public class MockTable extends DataTable
{
	public MockTable( final Schema schema, final String name )
	{
		super(MockCatalog.NS, schema, new TableDefImpl(MockCatalog.NS, name));
	}

	@Override
	public TableDefImpl getTableDef()
	{
		return (TableDefImpl) super.getTableDef();
	}

	@Override
	public String getType()
	{
		return "MOCK TABLE";
	}
}

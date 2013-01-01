package org.xenei.jdbc4sparql.mock;

import java.util.Iterator;
import java.util.List;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.SortKey;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.ColumnImpl;
import org.xenei.jdbc4sparql.impl.DataTable;
import org.xenei.jdbc4sparql.impl.TableDefImpl;
import org.xenei.jdbc4sparql.impl.NamespaceImpl;

public class MockTable extends DataTable
{
	public MockTable( Schema schema, String name )
	{
		super( MockCatalog.NS, schema, new TableDefImpl( name ));
	}

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

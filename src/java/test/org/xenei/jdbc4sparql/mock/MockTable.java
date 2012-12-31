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
import org.xenei.jdbc4sparql.impl.TableDefImpl;
import org.xenei.jdbc4sparql.impl.NamespaceImpl;

public class MockTable extends NamespaceImpl implements Table
{
	private Schema schema;
	private TableDefImpl tableDef;
	
	public MockTable( Schema schema, String name )
	{
		super(schema.getNamespace()+"Table/",name);
		tableDef = new TableDefImpl( name );
		this.schema = schema;
	}

	public TableDefImpl getTableDef()
	{
		return tableDef;
	}
	
	@Override
	public Schema getSchema()
	{
		return schema;
	}

	@Override
	public Catalog getCatalog()
	{
		return schema.getCatalog();
	}


	@Override
	public String getType()
	{
		return "MOCK TABLE";
	}

	@Override
	public boolean isEmpty()
	{
		return true;
	}

	public String getName()
	{
		return tableDef.getName();
	}

	public List<? extends ColumnDef> getColumnDefs()
	{
		return tableDef.getColumnDefs();
	}

	public ColumnDef getColumnDef( int idx )
	{
		return tableDef.getColumnDef(idx);
	}

	public ColumnDef getColumnDef( String name )
	{
		return tableDef.getColumnDef(name);
	}

	public int getColumnCount()
	{
		return tableDef.getColumnCount();
	}

	public SortKey getSortKey()
	{
		return tableDef.getSortKey();
	}

	public void verify( Object[] row )
	{
		tableDef.verify(row);
	}

	public int getColumnIndex( ColumnDef column )
	{
		return tableDef.getColumnIndex(column);
	}

	public int getColumnIndex( String columnName )
	{
		return tableDef.getColumnIndex(columnName);
	}

	@Override
	public Iterator<Column> getColumns()
	{
		return new Table.ColumnIterator(this, getColumnDefs());
	}

	@Override
	public Column getColumn( int idx )
	{
		return new ColumnImpl( this, getColumnDef(idx));
	}

	@Override
	public Column getColumn( String name )
	{
		return new ColumnImpl( this, getColumnDef(name));
	}

	@Override
	public NameFilter<Column> findColumns( String columnNamePattern )
	{
		return new NameFilter<Column>( columnNamePattern, getColumns());
	}
}

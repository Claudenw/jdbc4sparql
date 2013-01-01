package org.xenei.jdbc4sparql.sparql;

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
import org.xenei.jdbc4sparql.impl.NamespaceImpl;

public class SparqlTable extends NamespaceImpl implements Table
{
	private SparqlSchema schema;
	private SparqlTableDef tableDef;
	
	protected SparqlTable( SparqlSchema schema, SparqlTableDef tableDef )
	{
		super(schema.getNamespace(), tableDef.getName());
		this.schema = schema;
		this.tableDef = tableDef;
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
		return "VIRTUAL TABLE";
	}

	public String getDBName()
	{
		return String.format( "%s.%s", getSchema().getLocalName(), getLocalName());
	}

	public boolean equals( Object obj )
	{
		return tableDef.equals(obj);
	}

	public String getName()
	{
		return tableDef.getName();
	}

	public List<ColumnDef> getColumnDefs()
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

	public int hashCode()
	{
		return tableDef.hashCode();
	}

	public String toString()
	{
		return tableDef.toString();
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

package org.xenei.jdbc4sparql.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
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

public abstract class AbstractTable extends NamespaceImpl implements Table
{
	private final TableDef tableDef;
	private final Schema schema;

	@SuppressWarnings( "unchecked" )
	public AbstractTable( final Schema schema, final TableDef tableDef )
	{
		this(schema.getNamespace(), schema, tableDef);
	}

	public AbstractTable( final String namespace, final Schema schema,
			final TableDef tableDef )
	{
		super(namespace, tableDef.getLocalName());
		this.schema = schema;
		this.tableDef = tableDef;
	}

	@Override
	public NameFilter<Column> findColumns( final String columnNamePattern )
	{
		return new NameFilter<Column>(columnNamePattern, getColumns());
	}

	@Override
	public Catalog getCatalog()
	{
		return schema.getCatalog();
	}

	@Override
	public Column getColumn( final int idx )
	{
		return new ColumnImpl(this, getColumnDef(idx));
	}

	@Override
	public Column getColumn( final String name )
	{
		return new ColumnImpl(this, getColumnDef(name));
	}

	@Override
	public int getColumnCount()
	{
		return tableDef.getColumnCount();
	}

	@Override
	public ColumnDef getColumnDef( final int idx )
	{
		return tableDef.getColumnDef(idx);
	}

	@Override
	public ColumnDef getColumnDef( final String name )
	{
		return tableDef.getColumnDef(name);
	}

	@Override
	public List<? extends ColumnDef> getColumnDefs()
	{
		return tableDef.getColumnDefs();
	}

	@Override
	public int getColumnIndex( final ColumnDef column )
	{
		return tableDef.getColumnIndex(column);
	}

	@Override
	public int getColumnIndex( final String columnName )
	{
		return tableDef.getColumnIndex(columnName);
	}

	@Override
	public Iterator<? extends Column> getColumns()
	{
		return new Table.ColumnIterator(this, getColumnDefs());
	}

	@Override
	public String getDBName()
	{
		return String.format("%s.%s", schema.getLocalName(), getLocalName());
	}

	@Override
	public String getLocalName()
	{
		return tableDef.getLocalName();
	}

	abstract public ResultSet getResultSet() throws SQLException;

	@Override
	public Schema getSchema()
	{
		return schema;
	}

	@Override
	public SortKey getSortKey()
	{
		return tableDef.getSortKey();
	}

	public TableDef getTableDef()
	{
		return tableDef;
	}

	@Override
	public String getType()
	{
		return "TABLE";
	}

	@Override
	public String toString()
	{
		return String.format("Table[ %s.%s ]", getCatalog().getLocalName(),
				getDBName());
	}

	@Override
	public void verify( final Object[] row )
	{
		tableDef.verify(row);
	}

}

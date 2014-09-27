package org.xenei.jdbc4sparql.impl;

import java.util.Iterator;
import java.util.List;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;

public abstract class AbstractTable<T extends Column> implements Table<T> {

	
	public AbstractTable() {
		
	}
	
	@Override
	public NameFilter<Column> findColumns( final String columnNamePattern )
	{
		return new NameFilter<Column>(columnNamePattern, getColumns());
	}

	@Override
	public Catalog getCatalog()
	{
		return getSchema().getCatalog();
	}
	
	@Override
	public String getSPARQLName()
	{
		return NameUtils.getSPARQLName(this);
	}
	
	@Override
	public String getSQLName()
	{
		return NameUtils.getDBName(this);
	}

	@Override
	public T getColumn( final int idx )
	{
		return getColumnList().get(idx);
	}

	@Override
	public T getColumn( final String name )
	{
		for (final T col : getColumnList())
		{
			if (col.getName().equals(name))
			{
				return col;
			}
		}
		return null;
	}

	@Override
	public int getColumnCount()
	{
		return getColumnList().size();
	}

	@Override
	public int getColumnIndex( final Column column )
	{
		return getColumnList().indexOf(column);
	}

	/**
	 * Returns the column index for hte name or -1 if not found
	 * 
	 * @param columnName
	 *            The name to search for
	 * @return the column index (0 based) or -1 if not found.
	 */
	@Override
	public int getColumnIndex( final String columnName )
	{
		final List<? extends Column> cols = getColumnList();
		for (int i = 0; i < cols.size(); i++)
		{
			if (cols.get(i).getName().equals(columnName))
			{
				return i;
			}
		}
		return -1;
	}

	@Override
	public Iterator<T> getColumns()
	{
		return getColumnList().iterator();
	}

	@Override
	public String toString()
	{
		return String.format("Table[ %s.%s ]", getCatalog().getName(),
				getSQLName());
	}
}

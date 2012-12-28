package org.xenei.jdbc4sparql.meta;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.KeySegment;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.SortKey;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.iface.TypeConverter;

public class MetaTableDef implements TableDef
{
	private String name;
	private List<Column> columns;
	private SortKey sortKey;
	
	public MetaTableDef(String name)
	{
		this.name =name;
		this.columns = new ArrayList<Column>();
		this.sortKey = null;
	}

	@Override
	public String getName()
	{
		return name;
	}

	@Override
	public List<Column> getColumns()
	{
		return columns;
	}

	@Override
	public SortKey getSortKey()
	{
		return sortKey;
	}
	
	public void setUnique()
	{
		if (sortKey == null)
		{
			sortKey = new SortKey();
		}
		sortKey.setUnique();
	}
	
	public void add(Column column)
	{
		columns.add(column);
	}
	
	public void addKey(String columnName)
	{
		addKey( getColumn(columnName));
	}
	
	public void addKey(Column column)
	{
		int idx = columns.indexOf(column);
		if (idx == -1)
		{
			throw new IllegalArgumentException( column.getLabel()+" is not in table");
		}
		if (sortKey == null)
		{
			sortKey = new SortKey();
		}
		sortKey.addSegment( new KeySegment( idx, column ));
	}

	public void verify( Object[] row )
	{
		if (row.length != columns.size())
		{
			throw new IllegalArgumentException( String.format( "Expected %s columns but got %s", columns.size(), row.length ));
		}
		for (int i=0;i<row.length;i++)
		{
			Column c = columns.get(i);
			
			if (row[i] == null)
			{
				if (c.getNullable() == DatabaseMetaData.columnNoNulls)
				{
					throw new IllegalArgumentException( String.format( "Column %s may not be null", c.getLabel()));
				}
			}
			else
			{
				Class<?> clazz = TypeConverter.getJavaType( c.getType() );
				if (! clazz.isAssignableFrom( row[i].getClass() )) 
				{
					throw new IllegalArgumentException( String.format( "Column %s can not recieve values of class %s", c.getLabel(), row[i].getClass()));
				}
			}
		}
		
	}

	public int getColumnIndex( String columnName )
	{
		for (int i=0;i<columns.size();i++)
		{
			if (columns.get(i).getLocalName().equals( columnName ))
			{
				return i;
			}
		}
		return -1;
	}

	@Override
	public int getColumnIndex( Column column )
	{
		return columns.indexOf( column );
	}

	@Override
	public Column getColumn( int idx )
	{
		return columns.get(idx);
	}

	@Override
	public Column getColumn( String name )
	{
		for (Column retval : new NameFilter<Column>( name, columns ))
		{
			return retval;
		}
		return null;
	}

	@Override
	public int getColumnCount()
	{
		return columns.size();
	}

}
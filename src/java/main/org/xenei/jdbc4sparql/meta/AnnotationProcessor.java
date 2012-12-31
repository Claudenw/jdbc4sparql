package org.xenei.jdbc4sparql.meta;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.iface.TypeConverter;
import org.xenei.jdbc4sparql.impl.TableDefImpl;

public class AnnotationProcessor
{
	private Entity getEntity( Class<?> tableClass )
	{
		Entity entity = tableClass.getAnnotation( Entity.class );
		if (entity == null)
		{
			throw new IllegalArgumentException( String.format( "%s must have an %s Annotation", tableClass, Entity.class));
		}
		return entity;
	}
	
	private String getTableName( Class<?> tableClass, Entity entity)
	{
		return StringUtils.defaultString( entity.name(), tableClass.getSimpleName());
	}
	
	private String getColumnName( Method method, Column column )
	{
		String columnName = null;
		if (StringUtils.isEmpty( column.name() ))
		{
			if (method.getName().startsWith( "get") )
			{
				columnName = method.getName().substring(3);
			}
			else if (method.getName().startsWith( "is") )
			{
				columnName = method.getName().substring(2);
			}
		}
		else
		{
			columnName = column.name();
		}
		if (columnName == null)
		{
			throw new IllegalArgumentException( String.format( "Unable to determine column name for %s.%s", method.getClass(),method.getName()));
		}
		return columnName;
		
	}
	
	public TableDef parseTableDef( Class<?> tableClass )
	{
		Entity entity = getEntity( tableClass );
	
		String tableName = getTableName( tableClass, entity );

		TableDefImpl tableDef = new TableDefImpl( tableName );
		for (Method method : tableClass.getMethods())
		{
			if (method.getName().startsWith("get") || method.getName().startsWith("is"))
			{
				Column column = method.getAnnotation(Column.class);
				if (column != null)
				{
					String columnName = getColumnName( method, column );
					int sqlType = TypeConverter.getSqlType(method.getReturnType());
					MetaColumn columnDef = new MetaColumn( columnName, sqlType, column );
					tableDef.add( columnDef );
				}				
			}
		}
		return tableDef;
	}
	
	public Object[] getRowData( TableDef tableDef, Object o) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
	{
		Object[] retval = new Object[tableDef.getColumnCount()];
	
		Entity entity = getEntity( o.getClass() );
		
		String tableName = getTableName( o.getClass(), entity );
		if (!tableName.equals(tableDef.getName()))
		{
			throw new IllegalArgumentException( String.format( "%s does not implement %s", o.getClass(), tableDef.getName()));
		}
		for (Method method : o.getClass().getMethods())
		{
			if (method.getName().startsWith("get") || method.getName().startsWith("is"))
			{
				Column column = method.getAnnotation(Column.class);
				if (column != null)
				{
					String columnName = getColumnName( method, column );
					int idx = tableDef.getColumnIndex(columnName);
					if (idx > -1 )
					{
						retval[ idx ]=method.invoke(o);
					}
				}
			}
		}
		return retval;
	}
}

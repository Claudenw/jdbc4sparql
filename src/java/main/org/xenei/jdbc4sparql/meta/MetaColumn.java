package org.xenei.jdbc4sparql.meta;

import java.sql.DatabaseMetaData;
import java.sql.Types;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;

public class MetaColumn implements Column
{
	/**
	 * 
	 */
	private String localName;
	private int displaySize;
	private int type;
	private int precision;
	private int scale;
	private boolean signed;
	private int nullable;
	private Table table;

	public static MetaColumn getStringInstance( String localName ) {
		return new MetaColumn(localName, Types.VARCHAR, 0, 0, 0,  false );
		}

	public static MetaColumn getIntInstance( String localName ) {
		return new MetaColumn( localName, Types.INTEGER, 0, 0, 0,  true );
	}

	public MetaColumn( String localName, int type )
	{
		this(  localName, type, 0, 0, 0, true );
	}

	public MetaColumn( String localName, int type, int displaySize,  int precision, int scale, boolean signed )
	{
		this.localName=localName;
		this.displaySize = displaySize;
		this.type  = type;
		this.precision = precision;
		this.scale = scale;
		this.signed = signed;
		this.nullable = DatabaseMetaData.columnNullableUnknown;
	}
	
	public MetaColumn setNullable(int state)
	{
		nullable = state;
		return this;
	}
	
	@Override
	public String getNamespace()
	{
		return "";
	}

	@Override
	public String getLocalName()
	{
		return localName;
	}

	void setTable( Table table )
	{
		this.table=table;
	}
	
	@Override
	public Catalog getCatalog()
	{
		return table.getCatalog();
	}

	@Override
	public Schema getSchema()
	{
		return table.getSchema();
	}

	@Override
	public Table getTable()
	{
		return table;
	}

	@Override
	public String getColumnClassName()
	{
		return "";
	}

	@Override
	public int getDisplaySize()
	{
		return displaySize;
	}

	@Override
	public String getLabel()
	{
		return getLocalName();
	}

	@Override
	public int getType()
	{
		return type;
	}

	@Override
	public String getTypeName()
	{
		return "";
	}

	@Override
	public int getPrecision()
	{
		return precision;
	}

	@Override
	public int getScale()
	{
		return scale;
	}

	@Override
	public boolean isAutoIncrement()
	{
		return false;
	}

	@Override
	public boolean isCaseSensitive()
	{
		return true;
	}

	@Override
	public boolean isCurrency()
	{
		return false;
	}

	@Override
	public boolean isDefinitelyWritable()
	{
		return false;
	}

	@Override
	public int getNullable()
	{
		return nullable;
	}

	@Override
	public boolean isReadOnly()
	{
		return true;
	}

	@Override
	public boolean isSearchable()
	{
		return false;
	}

	@Override
	public boolean isSigned()
	{
		return signed;
	}

	@Override
	public boolean isWritable()
	{
		return false;
	}
	
}
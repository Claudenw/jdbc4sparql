package org.xenei.jdbc4sparql.impl;

import java.sql.DatabaseMetaData;
import java.sql.Types;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.iface.meta.ColumnsTableRow;
import org.xenei.jdbc4sparql.meta.MetaColumn;

public class ColumnDefImpl extends NamespaceImpl implements ColumnDef
{

	private int displaySize;
	private int type;
	private int precision;
	private int scale;
	private boolean signed;
	private int nullable;
	
	
	public static ColumnDefImpl getStringInstance(String namespace, String localName ) {
		return new ColumnDefImpl(namespace, localName, Types.VARCHAR, 0, 0, 0,  false );
		}

	public static ColumnDefImpl getIntInstance(String namespace, String localName ) {
		return new ColumnDefImpl(namespace, localName, Types.INTEGER, 0, 0, 0,  true );
	}
	
	public ColumnDefImpl(String namespace, String localName, int type )
	{
		this(namespace, localName, type, 0, 0, 0, true );
	}

	public ColumnDefImpl(String namespace, String localName, int type, javax.persistence.Column columnDef )
	{
		this(namespace, localName, type, 0, columnDef.precision(), columnDef.scale(), true );
		setNullable(columnDef.nullable()?DatabaseMetaData.columnNullable:DatabaseMetaData.columnNoNulls);
	}
	
	public ColumnDefImpl(String namespace, String localName, int type, int displaySize,  int precision, int scale, boolean signed )
	{
		super( namespace, localName );
		this.displaySize = displaySize;
		this.type  = type;
		this.precision = precision;
		this.scale = scale;
		this.signed = signed;
		this.nullable = DatabaseMetaData.columnNoNulls;
	}
	
	
	public ColumnDefImpl setNullable(int state)
	{
		nullable = state;
		return this;
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
package org.xenei.jdbc4sparql.impl;

import java.sql.DatabaseMetaData;
import java.sql.Types;

import org.xenei.jdbc4sparql.iface.ColumnDef;

public class ColumnDefImpl extends NamespaceImpl implements ColumnDef
{

	public static ColumnDefImpl getIntInstance( final String namespace,
			final String localName )
	{
		return new ColumnDefImpl(namespace, localName, Types.INTEGER, 0, 0, 0,
				true);
	}

	public static ColumnDefImpl getStringInstance( final String namespace,
			final String localName )
	{
		return new ColumnDefImpl(namespace, localName, Types.VARCHAR, 0, 0, 0,
				false);
	}

	private final int displaySize;
	private final int type;
	private final int precision;
	private final int scale;
	private final boolean signed;

	private int nullable;

	private String label;

	public ColumnDefImpl( final String namespace, final String localName,
			final int type )
	{
		this(namespace, localName, type, 0, 0, 0, true);
	}

	public ColumnDefImpl( final String namespace, final String localName,
			final int type, final int displaySize, final int precision,
			final int scale, final boolean signed )
	{
		super(namespace, localName);
		this.displaySize = displaySize;
		this.type = type;
		this.precision = precision;
		this.scale = scale;
		this.signed = signed;
		this.nullable = DatabaseMetaData.columnNoNulls;
		this.label = localName;
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
		return label;
	}

	@Override
	public int getNullable()
	{
		return nullable;
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

	public void setLabel( final String label )
	{
		this.label = label;
	}

	public ColumnDefImpl setNullable( final int state )
	{
		nullable = state;
		return this;
	}

	@Override
	public String toString()
	{
		return String.format("ColumnDef[%s]", getLabel());
	}

}
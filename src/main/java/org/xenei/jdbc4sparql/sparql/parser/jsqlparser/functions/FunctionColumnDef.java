package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import java.sql.ResultSetMetaData;

import org.xenei.jdbc4sparql.iface.ColumnDef;

public class FunctionColumnDef implements ColumnDef
{
	private int displaySize = 0;
	private int nullable = ResultSetMetaData.columnNoNulls;
	private int precision = 0;
	private int scale = 0;
	private final int type;
	private boolean autoIncrement = false;
	private boolean caseSensitive = false;
	private boolean currency = false;
	private boolean signed = false;

	public FunctionColumnDef( final int type )
	{
		this.type = type;
	}

	@Override
	public String getColumnClassName()
	{
		return null;
	}

	@Override
	public int getDisplaySize()
	{
		return displaySize;
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isAutoIncrement()
	{
		return autoIncrement;
	}

	@Override
	public boolean isCaseSensitive()
	{
		return caseSensitive;
	}

	@Override
	public boolean isCurrency()
	{
		return currency;
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

	public void setAutoIncrement( final boolean autoIncrement )
	{
		this.autoIncrement = autoIncrement;
	}

	public void setCaseSensitive( final boolean caseSensitive )
	{
		this.caseSensitive = caseSensitive;
	}

	public void setCurrency( final boolean currency )
	{
		this.currency = currency;
	}

	public void setDisplaySize( final int displaySize )
	{
		this.displaySize = displaySize;
	}

	public void setNullable( final int nullable )
	{
		this.nullable = nullable;
	}

	public void setPrecision( final int precision )
	{
		this.precision = precision;
	}

	public void setScale( final int scale )
	{
		this.scale = scale;
	}

	public void setSigned( final boolean signed )
	{
		this.signed = signed;
	}

}

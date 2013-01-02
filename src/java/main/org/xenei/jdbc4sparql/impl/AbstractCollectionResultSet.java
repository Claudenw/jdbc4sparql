package org.xenei.jdbc4sparql.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.AbstractResultSet;

public abstract class AbstractCollectionResultSet extends AbstractResultSet
{
	private Collection<?> data;

	private int position;
	
	public AbstractCollectionResultSet( Collection<?> rows, Table table ) throws SQLException
	{
		super( table );
		setTableData( rows );
	}

	protected void setTableData(Collection<?> tableData) throws SQLException
	{
		this.data = tableData;
		switch (getFetchDirection())
		{
			case ResultSet.FETCH_REVERSE:
				last();
				break;
			case ResultSet.FETCH_FORWARD:
			default:
				first();
				break;
		}
	}
	
	protected int getPosition()
	{
		return position;
	}
	
	protected Collection<?> getDataCollection()
	{
		return data;
	}
	
	abstract protected Object getRowObject() throws SQLException;
	
	@Override
	public boolean isWrapperFor( Class<?> iface ) throws SQLException
	{
		return false;
	}

	@Override
	public <T> T unwrap( Class<T> iface ) throws SQLException
	{
		return null;
	}

	private boolean isValidPosition()
	{
		return position < data.size() && position>-1;
	}
	
	/**
	 * Always called after a positioning change.
	 */
	protected void fixupPosition() throws SQLException
	{
		if (position < -1)
		{
			position = -1;
		}
		if (position > data.size())
		{
			position = data.size();
		}
	}
	
	
	@Override
	public boolean absolute( int pos ) throws SQLException
	{
		if ( pos < 0 )
		{
			this.position = data.size()-pos-1;;
		}
		else
		{
			this.position = pos-1;
		}
		fixupPosition();
		return isValidPosition();
	}

	@Override
	public void afterLast() throws SQLException
	{
		
		position = data.size();
	}

	@Override
	public void beforeFirst() throws SQLException
	{
		position = -1;
	}

	@Override
	public void cancelRowUpdates() throws SQLException
	{
		// do nothing -- not updatable
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		// do nothing.
	}

	@Override
	public void close() throws SQLException
	{
		// do nothing
	}

	@Override
	public void deleteRow() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean first() throws SQLException
	{
		position = 0;
		boolean retval =isValidPosition();
		fixupPosition();
		return retval;
	}
	
	protected void checkPosition() throws SQLException
	{
		if (! isValidPosition() )
		{
			throw new SQLException( "Cursor not positioned on a result");
		}
	}

	@Override
	public int getRow() throws SQLException
	{
		return position;
	}

	@Override
	public int getType() throws SQLException
	{
		return ResultSet.TYPE_SCROLL_SENSITIVE;
	}

	@Override
	public boolean isAfterLast() throws SQLException
	{
		return position == data.size();
	}

	@Override
	public boolean isBeforeFirst() throws SQLException
	{
		return position == -1;
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		return false;
	}

	@Override
	public boolean isFirst() throws SQLException
	{
		return position == 0;
	}

	@Override
	public boolean isLast() throws SQLException
	{
		return position == data.size()-1;
	}

	@Override
	public boolean last() throws SQLException
	{
		position = data.size()-1;
		boolean retval = isValidPosition();
		fixupPosition();
		return retval;
	}

	@Override
	public void moveToCurrentRow() throws SQLException
	{
		// do nothing
	}

	
	@Override
	public boolean next() throws SQLException
	{
		return relative( 1 );
	}

	@Override
	public boolean previous() throws SQLException
	{
		return relative( -1 );
	}

	@Override
	public boolean relative( int rows ) throws SQLException
	{
		switch (getFetchDirection())
		{
			case ResultSet.FETCH_REVERSE:
				position -= rows;
				break;
			case ResultSet.FETCH_FORWARD:
			default:
				position += rows;
				break;
		}
		boolean retval = isValidPosition();
		fixupPosition();
		return retval;
	}
}

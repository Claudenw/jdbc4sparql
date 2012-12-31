package org.xenei.jdbc4sparql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;

public abstract class AbstractResultSet implements ResultSet
{
	private final Table table;
	private final Map<String, Integer> columnNameIdx;

	public AbstractResultSet( final Table table )
	{
		this.table = table;
		columnNameIdx = new HashMap<String, Integer>();
		for (int i = 0; i < table.getColumnDefs().size(); i++)
		{
			columnNameIdx.put(table.getColumn(i).getLabel(), i);
		}
	}

	protected void checkColumn( final int idx ) throws SQLException
	{
		if (!isValidColumn(idx))
		{
			throw new SQLException("Invalid column idex: " + idx);
		}
	}

	protected void checkType( final int idx, final int type )
			throws SQLException
	{
		final Column c = getColumn(idx);
		if (c.getType() != type)
		{
			throw new SQLException("Column type (" + c.getType() + ") is not "
					+ type);
		}
	}

	@Override
	public int findColumn( final String columnName ) throws SQLException
	{
		final Integer idx = columnNameIdx.get(columnName);
		if (idx == null)
		{
			throw new SQLException(columnName + " is not a column");
		}
		return idx;
	}

	protected Column getColumn( final int idx ) throws SQLException
	{
		checkColumn(idx);
		return table.getColumn(idx - 1);
	}

	protected Column getColumn( final String name ) throws SQLException
	{
		return table.getColumn(name);
	}

	protected boolean isValidColumn( final int idx )
	{
		return (idx > 0) && (idx <= table.getColumnCount());
	}
}

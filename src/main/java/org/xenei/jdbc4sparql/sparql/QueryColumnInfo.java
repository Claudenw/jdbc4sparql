package org.xenei.jdbc4sparql.sparql;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnName;

/**
 * A column in a table in the query.  This class maps the column to an alias name.
 * This may be a column in a base table or a function.
 */
public class QueryColumnInfo extends QueryItemInfo<ColumnName> 
{
	private final Column column;

	QueryColumnInfo( final QueryInfoSet infoSet, final QueryTableInfo tableInfo, final Column column,
			final ColumnName alias, boolean optional)
	{
		super(alias, optional);
		if (column == null)
		{
			throw new IllegalArgumentException( "Column may not be null");
		}
		if (infoSet == null)
		{
			throw new IllegalArgumentException( "QueryTableInfo may not be null");
		}
		this.column = column;
		infoSet.addColumn(tableInfo, this);
	}

	@Override
	public void setOptional( boolean optional )
	{
		super.setOptional( optional );
	}
	
	public Column getColumn()
	{
		return column;
	}

	@Override
	public String toString()
	{
		return String.format("QueryColumnInfo[%s(%s)]", column.getSQLName(),
				getName());
	}
	
	@Override
	public boolean equals( Object o )
	{
		if (o != null && o instanceof QueryColumnInfo)
		{
			QueryColumnInfo colInfo = (QueryColumnInfo) o;
			return getName().equals( colInfo.getName() ) && column.equals(colInfo.getColumn());
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return getName().hashCode();
	}

}

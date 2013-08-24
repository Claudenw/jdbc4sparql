package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.sparql.expr.Expr;

import java.sql.DatabaseMetaData;
import java.sql.Types;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.impl.NameUtils;

class ExprColumn implements Column
{

	private class ExprColumnDef implements ColumnDef
	{

		ExprColumnDef( final Expr expr )
		{
		}

		@Override
		public String getColumnClassName()
		{
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public int getDisplaySize()
		{
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int getNullable()
		{
			return DatabaseMetaData.columnNoNulls;
		}

		@Override
		public int getPrecision()
		{
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int getScale()
		{
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int getType()
		{
			return Types.VARCHAR;
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
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isCaseSensitive()
		{
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isCurrency()
		{
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isDefinitelyWritable()
		{
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isReadOnly()
		{
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isSearchable()
		{
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isSigned()
		{
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isWritable()
		{
			// TODO Auto-generated method stub
			return false;
		}

	}

	private final String name;
	private Table table;

	private final ColumnDef colDef;

	public ExprColumn( final String name, final Expr expr )
	{
		this.name = name;
		this.colDef = new ExprColumnDef(expr);
	}

	@Override
	public Catalog getCatalog()
	{
		return table.getCatalog();
	}

	@Override
	public ColumnDef getColumnDef()
	{
		return colDef;
	}

	@Override
	public String getName()
	{
		return name;
	}

	@Override
	public String getRemarks()
	{
		return "ExprColumn built column";
	}

	@Override
	public Schema getSchema()
	{
		return table.getSchema();
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
	public Table getTable()
	{
		return table;
	}

	@Override
	public String toString()
	{
		return getSQLName();
	}
}
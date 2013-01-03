package org.xenei.jdbc4sparql.meta;

import java.sql.DatabaseMetaData;
import java.sql.Types;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.impl.ColumnDefImpl;

/**
 * Meta column can be created without a table being specified first.
 */
public class MetaColumn extends ColumnDefImpl implements Column
{

	public static MetaColumn getIntInstance( final String localName )
	{
		return new MetaColumn(localName, Types.INTEGER, 0, 0, 0, true);
	}

	public static MetaColumn getStringInstance( final String localName )
	{
		return new MetaColumn(localName, Types.VARCHAR, 0, 0, 0, false);
	}

	private Table table;

	public MetaColumn( final String localName, final int type )
	{
		this(localName, type, 0, 0, 0, true);
	}

	public MetaColumn( final String localName, final int type,
			final int displaySize, final int precision, final int scale,
			final boolean signed )
	{
		super(MetaNamespace.NS, localName, type, displaySize, precision, scale,
				signed);
	}

	public MetaColumn( final String localName, final int type,
			final javax.persistence.Column columnDef )
	{
		this(localName, type, 0, columnDef.precision(), columnDef.scale(), true);
		setNullable(columnDef.nullable() ? DatabaseMetaData.columnNullable
				: DatabaseMetaData.columnNoNulls);
	}

	@Override
	public Catalog getCatalog()
	{
		return table.getSchema().getCatalog();
	}

	@Override
	public String getDBName()
	{
		return getTable().getDBName() + "." + getLocalName();
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

	void setTable( final Table table )
	{
		this.table = table;
	}

}
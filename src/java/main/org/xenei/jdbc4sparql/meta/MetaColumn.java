package org.xenei.jdbc4sparql.meta;

import java.sql.DatabaseMetaData;
import java.sql.Types;

import org.xenei.jdbc4sparql.ColumnDefImpl;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.iface.meta.ColumnsTableRow;

public class MetaColumn extends ColumnDefImpl implements Column
{
	
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

	public MetaColumn( String localName, int type, javax.persistence.Column columnDef )
	{
		this( localName, type, 0, columnDef.precision(), columnDef.scale(), true );
		setNullable(columnDef.nullable()?DatabaseMetaData.columnNullable:DatabaseMetaData.columnNoNulls);
	}
	
	public MetaColumn( String localName, int type, int displaySize,  int precision, int scale, boolean signed )
	{
		super( MetaNamespace.NS, localName, type, displaySize, precision, scale, signed );
	}
	

	void setTable( Table table )
	{
		this.table=table;
	}
	
	@Override
	public Catalog getCatalog()
	{
		return table.getSchema().getCatalog();
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
}
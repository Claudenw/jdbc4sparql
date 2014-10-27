package org.xenei.jdbc4sparql.impl.virtual;

import java.util.ArrayList;
import java.util.List;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.iface.TableName;
import org.xenei.jdbc4sparql.impl.AbstractTable;

public class VirtualTable extends AbstractTable<Column>
{
	private final Schema schema;
	private final TableName tableName;
	private final List<Column> columns;
	private TableDef tableDef;

	public VirtualTable( final Schema schema )
	{
		this(schema, "");
	}

	public VirtualTable( final Schema schema, final String name )
	{
		this.tableName = schema.getName().getTableName(name);
		this.columns = new ArrayList<Column>();
		this.schema = schema;
	}

	@Override
	public void delete()
	{
		// does nothing
	}

	@Override
	public List<Column> getColumnList()
	{
		return columns;
	}

	@Override
	public TableName getName()
	{
		return tableName;
	}

	@Override
	public String getQuerySegmentFmt()
	{
		return null;
	}

	@Override
	public String getRemarks()
	{
		return "";
	}

	@Override
	public Schema getSchema()
	{
		return schema;
	}

	@Override
	public Table<Column> getSuperTable()
	{
		return null;
	}

	@Override
	public TableDef getTableDef()
	{
		return tableDef;
	}

	@Override
	public String getType()
	{
		return "Virtual";
	}

	@Override
	public boolean hasQuerySegments()
	{
		return false;
	}

}

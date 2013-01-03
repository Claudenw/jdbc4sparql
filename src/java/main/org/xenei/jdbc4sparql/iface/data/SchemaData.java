package org.xenei.jdbc4sparql.iface.data;

import java.util.Collection;

import org.xenei.jdbc4sparql.iface.meta.ColumnsTableRow;

public interface SchemaData
{

	Collection<ColumnsTableRow> getColumnsTableData( String tablePattern );

	String getName();

	TableData getTableData( String tableName );

	Collection<String> getTableNames();
}

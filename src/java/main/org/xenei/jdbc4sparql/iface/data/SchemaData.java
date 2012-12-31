package org.xenei.jdbc4sparql.iface.data;

import java.util.Collection;

import org.xenei.jdbc4sparql.iface.meta.ColumnsTableRow;

public interface SchemaData
{

	String getName();
	
	Collection<ColumnsTableRow> getColumnsTableData( String tablePattern );
	
	Collection<String> getTableNames();
	
	TableData getTableData( String tableName );
}

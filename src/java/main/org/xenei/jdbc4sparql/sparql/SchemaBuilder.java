package org.xenei.jdbc4sparql.sparql;

import java.util.Set;

import org.xenei.jdbc4sparql.iface.TableDef;

public interface SchemaBuilder
{
	Set<TableDef> getTableDefs();

}

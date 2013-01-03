package org.xenei.jdbc4sparql.sparql.builders;

import java.util.Set;

import org.xenei.jdbc4sparql.iface.TableDef;

public interface SchemaBuilder
{
	Set<TableDef> getTableDefs();

}

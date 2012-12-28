package org.xenei.jdbc4sparql.iface;

import java.util.List;

public interface Table extends NamespacedObject
{
	/**
	 * The schema the table belongs in.
	 * @return
	 */
	Schema getSchema();
	
	Catalog getCatalog();
	
	TableDef getTableDef();
	
	/**
	 * Get the type of table.
	 * Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", 
	 * "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
	 */
	String getType();  
	
	boolean isEmpty();
}

package org.xenei.jdbc4sparql.iface;

import java.util.Collection;
import java.util.Set;

public interface Catalog extends NamespacedObject
{
	
	/**
	 * 
	 * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the database; 
	 * "" retrieves those without a schema; null means that all schemas should be returned
	 * @return
	 */
	Set<Schema> getSchemas();
	
	/**
	 * Get the schema
	 * @param schema
	 * @return
	 */
	Schema getSchema(String schemaName);
	
	/**
	 * Return the list of schemas that have names matching the pattern
	 * if name pattern == null return all the schemas
	 * if name pattern == "" return only unamed schemas.
	 * if name is anything else match the string.
	 * @param schemaNamePattern
	 * @return
	 */
	NameFilter<Schema> findSchemas( String schemaNamePattern );
	
}

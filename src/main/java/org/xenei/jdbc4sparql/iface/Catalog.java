/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.jdbc4sparql.iface;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QuerySolution;

import java.util.List;
import java.util.Set;

public interface Catalog extends NamedObject<CatalogName>
{
	/**
	 * Close release all associated resources.
	 */
	public void close();

	/**
	 * Execute the query against the local Model.
	 * 
	 * This is used to execute queries built by the query builder.
	 * 
	 * @param query
	 * @return The list of QuerySolutions.
	 */
	public List<QuerySolution> executeLocalQuery( final Query query );

	/**
	 * Return the list of schemas that have names matching the pattern
	 * if name pattern == null return all the schemas
	 * if name pattern == "" return only unamed schemas.
	 * if name is anything else match the string.
	 * 
	 * @param schemaNamePattern
	 * @return
	 */
	NameFilter<? extends Schema> findSchemas( String schemaNamePattern );

	/**
	 * Get the schema
	 * 
	 * @param schema
	 * @return
	 */
	Schema getSchema( String schemaName );

	/**
	 * 
	 * @param schemaPattern
	 *            a schema name pattern; must match the schema name as it is
	 *            stored in the database;
	 *            "" retrieves those without a schema; null means that all
	 *            schemas should be returned
	 * @return
	 */
	Set<? extends Schema> getSchemas();

}

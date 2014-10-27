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

public interface Column extends NamedObject<ColumnName>
{
	/**
	 * Get teh catalog this table is in.
	 *
	 * @return the Catalog this table is in.
	 */
	public Catalog getCatalog();

	/**
	 * Get the column definition for this column.
	 *
	 * @return the ColumnDef
	 */
	public ColumnDef getColumnDef();

	/**
	 * A string used to format the column name with respect to the table so that
	 * the SPARQL query will retrieve the proper data. For example
	 * "%1$s <http://example.com/jdbc4sparql#NullableIntCol> %2$s"
	 *
	 * %1$s is the table name
	 * %2$s is the column name
	 *
	 * @return Format string for query segments in SPARQL query
	 */
	public String getQuerySegmentFmt();

	/**
	 * Get the remarks for the column.
	 *
	 * @return the remarks
	 */
	public String getRemarks();

	/**
	 * Get the Schema for this column.
	 *
	 * @return The schema this table is in
	 */
	public Schema getSchema();

	/**
	 * Get the SPARQL name for this column.
	 *
	 * @return The name formatted for SPARQL
	 */
	public String getSPARQLName();

	/**
	 * get the SQL name for this column.
	 *
	 * @return the name formatted for SQL
	 */
	public String getSQLName();

	/**
	 * Get the table this column is in.
	 *
	 * @return The table this column is in.
	 */
	public Table getTable();

	/**
	 * Return true if this column has querySegments.
	 * Most columns do, however, some function columns do not.
	 *
	 * @return
	 */
	public boolean hasQuerySegments();

	/**
	 * True if this column is optional
	 *
	 * @return
	 */
	public boolean isOptional();

}

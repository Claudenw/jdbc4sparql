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

import java.util.Iterator;
import java.util.List;

import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.sparql.items.NamedObject;

public interface Table extends NamedObject<TableName> {

	/**
	 * delete the table. Removes the table from the schema.
	 */
	void delete();

	/**
	 * Find all columns with the columnNamePattern.
	 *
	 * If columnNamePattern is null all columns are matched.
	 *
	 * @param columnNamePattern
	 *            The pattern to match or null.
	 * @return
	 */
	NameFilter<Column> findColumns(String columnNamePattern);

	/**
	 *
	 * @return Get the catalog this table is in.
	 */
	Catalog getCatalog();

	/**
	 * Get the column by index.
	 *
	 * @param idx
	 *            The index of the column to retrieve.
	 * @return the column.
	 * @thows IndexOutOfBoundsException
	 */
	Column getColumn(int idx);

	/**
	 * Get the column by short name
	 *
	 * @param shortName
	 *            the short name of the column to retrieve
	 * @return the column or null if name not found.
	 */
	Column getColumn(String shortName);

	int getColumnCount();

	public int getColumnIndex(Column column);

	/**
	 * Get the index (zero based) for the column name.
	 *
	 * @param columnName
	 *            The column name to search for
	 * @return index for column name or -1 if not found.
	 */
	public int getColumnIndex(String columnName);

	List<Column> getColumnList();

	/**
	 * Get an iterator over all the columns in order.
	 *
	 * @return The column iterator.
	 */
	Iterator<Column> getColumns();

	/**
	 * A string used to format the column name with respect to the table so that
	 * the SPARQL query will retrieve the proper data. For example
	 * "%1$s <http://example.com/jdbc4sparql#NullableIntCol> %2$s"
	 *
	 * %1$s is the table name %2$s is the column name
	 *
	 * @return Format string for query segments in SPARQL query
	 */
	public String getQuerySegmentFmt();

	String getRemarks();

	/**
	 * @return The schema the table belongs in.
	 */
	Schema getSchema();

	/**
	 *
	 * @return the SPARQL formatted table name.
	 */
	String getSPARQLName();

	/**
	 * @return the SQL formatted table name
	 */
	String getSQLName();

	/**
	 * Get the supertable for this table.
	 *
	 * @return The super table or null.
	 */
	Table getSuperTable();

	public TableDef getTableDef();

	/**
	 * Get the type of table. Typical types are "TABLE", "VIEW", "SYSTEM TABLE",
	 * "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
	 *
	 * @return The table type
	 */
	String getType();

	/**
	 * Return true if this column has querySegments. Most columns do, however,
	 * some function columns do not.
	 *
	 * @return
	 */
	public boolean hasQuerySegments();

}

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

public interface ColumnDef
{
	String getColumnClassName();

	int getDisplaySize();

	String getLabel();

	/**
	 * Indicates the nullability of values in the designated column.
	 * Possible return values are ResultSetMetaData.columnNullable,
	 * ResultSetMetaData.columnNoNulls,  ResultSetMetaData.columnNullableUnknown
	 * @return the nullability status of the given column.
	 */
	int getNullable();

	int getPrecision();

	int getScale();

	int getType();

	String getTypeName();

	boolean isAutoIncrement();

	boolean isCaseSensitive();

	boolean isCurrency();

	boolean isDefinitelyWritable();

	boolean isReadOnly();

	boolean isSearchable();

	boolean isSigned();

	boolean isWritable();
}

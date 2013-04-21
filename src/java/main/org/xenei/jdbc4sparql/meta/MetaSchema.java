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
package org.xenei.jdbc4sparql.meta;

import java.sql.DatabaseMetaData;
import java.sql.Types;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.impl.ColumnDefImpl;
import org.xenei.jdbc4sparql.impl.SchemaImpl;
import org.xenei.jdbc4sparql.impl.TableDefImpl;

public class MetaSchema extends SchemaImpl
{
	public static final String LOCAL_NAME = "Schema";

	public static final String CATALOGS_TABLE = "Catalogs";
	public static final String COLUMNS_TABLE = "Columns";
	public static final String COLUMN_PRIVILIGES_TABLE = "ColumnPriviliges";
	public static final String EXPORTED_KEYS_TABLE = "ExportedKeys";
	public static final String IMPORTED_KEYS_TABLE = "ImportedKeys";
	public static final String XREF_TABLE = "XrefKeys";
	public static final String TYPEINFO_TABLE = "TypeInfo";
	public static final String INDEXINFO_TABLE = "IndexInfo";
	public static final String UDT_TABLES = "UDTs";
	public static final String SUPER_TYPES_TABLE = "SuperTypes";
	public static final String SUPER_TABLES_TABLE = "SuperTables";
	public static final String ATTRIBUTES_TABLE = "Attributes";
	public static final String SCHEMAS_TABLE = "Schemas";
	public static final String CLIENT_INFO_TABLE = "ClientInfo";
	public static final String FUNCTIONS_TABLE = "Functions";
	public static final String FUNCTION_COLUMNS_TABLE = "FunctionColumns";
	public static final String PRIMARY_KEY_TABLE = "PrimaryKeys";
	public static final String PROCEDURE_COLUMNS_TABLE = "ProcedureColumns";
	public static final String PROCEDURES_TABLE = "Procedures";
	public static final String VERSION_COLUMNS_TABLE = "Version";
	public static final String TABLE_PRIVILEGES_TABLE = "TablePriv";
	public static final String TABLE_TYPES_TABLE = "TableTypes";
	public static final String TABLES_TABLE = "Tables";
	public static final String BEST_ROW_TABLE = "BestRow";

	public MetaSchema( final Catalog catalog )
	{
		super(catalog, MetaSchema.LOCAL_NAME);
		init();
	}

	private void init()
	{
		TableDefImpl tableDef = null;

		tableDef = new TableDefImpl(MetaNamespace.NS, MetaSchema.CATALOGS_TABLE);
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"TABLE_CAT").build());
		tableDef.addKey("TABLE_CAT");
		tableDef.setUnique();
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS, MetaSchema.COLUMNS_TABLE);

		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TABLE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TABLE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"TABLE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"COLUMN_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"DATA_TYPE").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TYPE_NAME")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"COLUMN_SIZE").build());
		tableDef.add(ColumnDefImpl.Builder
				.getIntegerBuilder(MetaNamespace.NS, "BUFFER_LENGTH")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getIntegerBuilder(MetaNamespace.NS, "DECIMAL_DIGITS")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getIntegerBuilder(MetaNamespace.NS, "NUM_PREC_RADIX")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"NULLABLE").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "REMARKS")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "COLUMN_DEF")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getIntegerBuilder(MetaNamespace.NS, "SQL_DATA_TYPE")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getIntegerBuilder(MetaNamespace.NS, "SQL_DATETIME_SUB")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getIntegerBuilder(MetaNamespace.NS, "CHAR_OCTET_LENGTH")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"ORDINAL_POSITION").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"IS_NULLABLE").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "SCOPE_CATLOG")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "SCOPE_SCHEMA")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "SCOPE_TABLE")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(new ColumnDefImpl.Builder().setNamespace(MetaNamespace.NS)
				.setLocalName("SOURCE_DATA_TYPE").setType(Types.SMALLINT)
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"IS_AUTOINCREMENT").build());
		tableDef.addKey("TABLE_CAT");
		tableDef.addKey("TABLE_SCHEM");
		tableDef.addKey("TABLE_NAME");
		tableDef.addKey("ORDINAL_POSITION");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS, MetaSchema.SCHEMAS_TABLE);
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"TABLE_SCHEM").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TABLE_CATALOG")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.addKey("TABLE_CATALOG");
		tableDef.addKey("TABLE_SCHEM");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.CLIENT_INFO_TABLE);
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"MAX_LEN").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"DEFAULT_VALUE").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"DESCRIPTION").build());
		tableDef.addKey("NAME");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.FUNCTIONS_TABLE);
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "FUNCTION_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "FUNCTION_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"FUNCTION_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"REMARKS").build());
		tableDef.add(new ColumnDefImpl.Builder().setNamespace(MetaNamespace.NS)
				.setLocalName("FUNCTION_TYPE").setType(Types.SMALLINT).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"SPECIFIC_NAME").build());
		tableDef.addKey("FUNCTION_CAT");
		tableDef.addKey("FUNCTION_SCHEM");
		tableDef.addKey("FUNCTION_NAME");
		tableDef.addKey("SPECIFIC_NAME");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.TABLE_PRIVILEGES_TABLE);
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TABLE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TABLE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"TABLE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "GRANTOR")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"GRANTEE").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"PRIVILEGE").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"IS_GRANTABLE").build());
		tableDef.addKey("TABLE_CAT");
		tableDef.addKey("TABLE_SCHEM");
		tableDef.addKey("TABLE_NAME");
		tableDef.addKey("PRIVILEGE");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS, MetaSchema.BEST_ROW_TABLE);
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"SCOPE").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"COLUMN_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"DATA_TYPE").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TYPE_NAME")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"COLUMN_SIZE").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"BUFFER_LENGTH").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"DECIMAL_DIGITS").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"PSEUDO_COLUMN").build());
		tableDef.addKey("SCOPE");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.PROCEDURES_TABLE);
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "PROCEDURE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "PROCEDURE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"PROCEDURE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "FUTURE1")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "FUTURE2")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "FUTURE3")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"REMARKS").build());
		tableDef.add(new ColumnDefImpl.Builder().setNamespace(MetaNamespace.NS)
				.setLocalName("PROCEDURE_TYPE").setType(Types.SMALLINT).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"SPECIFIC_NAME").build());
		tableDef.addKey("PROCEDURE_CAT");
		tableDef.addKey("PROCEDURE_SCHEM");
		tableDef.addKey("PROCEDURE_NAME");
		tableDef.addKey("SPECIFIC_NAME");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.PROCEDURE_COLUMNS_TABLE);
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "PROCEDURE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "PROCEDURE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"PROCEDURE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"COLUMN_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"COLUMN_TYPE").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"DATA_TYPE").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"TYPE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"PRECISION").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"LENGTH").build());
		tableDef.add(ColumnDefImpl.Builder
				.getSmallIntBuilder(MetaNamespace.NS, "SCALE")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"RADIX").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"NULLABLE").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"REMARKS").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "COLUMN_DEF")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getIntegerBuilder(MetaNamespace.NS, "SQL_DATA_TYPE")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getIntegerBuilder(MetaNamespace.NS, "SQL_DATETIME_SUB")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getIntegerBuilder(MetaNamespace.NS, "CHAR_OCTET_LENGTH")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"ORDINAL_POSITION").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"IS_NULLABLE").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"SPECIFIC_NAME").build());
		tableDef.addKey("PROCEDURE_CAT");
		tableDef.addKey("PROCEDURE_SCHEM");
		tableDef.addKey("PROCEDURE_NAME");
		tableDef.addKey("SPECIFIC_NAME");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS, MetaSchema.TABLES_TABLE);
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TABLE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TABLE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"TABLE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"TABLE_TYPE").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"REMARKS").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TYPE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TYPE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TYPE_NAME")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "SELF_REFERENCING_COL_NAME")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "REF_GENERATION")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.addKey("TABLE_TYPE");
		tableDef.addKey("TABLE_CAT");
		tableDef.addKey("TABLE_SCHEM");
		tableDef.addKey("TABLE_NAME");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.TABLE_TYPES_TABLE);
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"TABLE_TYPE").build());
		tableDef.addKey("TABLE_TYPE");
		tableDef.setUnique();
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.COLUMN_PRIVILIGES_TABLE);
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TABLE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TABLE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"TABLE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"COLUMN_NAME").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "GRANTOR")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"GRANTEE").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"PRIVILEGE").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"IS_GRANTABLE").build());
		tableDef.addKey("COLUMN_NAME");
		tableDef.addKey("PRIVILEGE");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.VERSION_COLUMNS_TABLE);
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"SCOPE").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"COLUMN_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"DATA_TYPE").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"TYPE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"COLUMN_SIZE").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"BUFFER_LENGTH").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"DECIMAL_DIGITS").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"PSEUDO_COLUMN").build());
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.PRIMARY_KEY_TABLE);
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TABLE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TABLE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"TABLE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"COLUMN_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"KEY_SEQ").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "PK_NAME")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.addKey("COLUMN_NAME");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.IMPORTED_KEYS_TABLE);
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "PKTABLE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "PKTABLE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"PKTABLE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"PKCOLUMN_NAME").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "FKTABLE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "FKTABLE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"FKTABLE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"FKCOLUMN_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"KEY_SEQ").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"UPDATE_RULE").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"DELETE_RULE").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "FK_NAME")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "PK_NAME")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"DEFERRABILITY").build());
		tableDef.addKey("PKTABLE_CAT");
		tableDef.addKey("PKTABLE_SCHEM");
		tableDef.addKey("PKTABLE_NAME");
		tableDef.addKey("KEY_SEQ");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.EXPORTED_KEYS_TABLE);
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "PKTABLE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "PKTABLE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"PKTABLE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"PKCOLUMN_NAME").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "FKTABLE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "FKTABLE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"FKTABLE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"FKCOLUMN_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"KEY_SEQ").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"UPDATE_RULE").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"DELETE_RULE").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "FK_NAME")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "PK_NAME")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"DEFERRABILITY").build());
		tableDef.addKey("FKTABLE_CAT");
		tableDef.addKey("FKTABLE_SCHEM");
		tableDef.addKey("FKTABLE_NAME");
		tableDef.addKey("KEY_SEQ");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS, MetaSchema.XREF_TABLE);
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "PKTABLE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "PKTABLE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"PKTABLE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"PKCOLUMN_NAME").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "FKTABLE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "FKTABLE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"FKTABLE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"FKCOLUMN_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"KEY_SEQ").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"UPDATE_RULE").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"DELETE_RULE").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "FK_NAME")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "PK_NAME")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"DEFERRABILITY").build());
		tableDef.addKey("FKTABLE_CAT");
		tableDef.addKey("FKTABLE_SCHEM");
		tableDef.addKey("FKTABLE_NAME");
		tableDef.addKey("KEY_SEQ");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS, MetaSchema.TYPEINFO_TABLE);
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"TYPE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"DATA_TYPE").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"PRECISION").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "LITERAL_PREFIX")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "LITERAL_SUFFIX")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "CREATE_PARAMS")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"NULLABLE").build());
		tableDef.add(new ColumnDefImpl.Builder().setNamespace(MetaNamespace.NS)
				.setLocalName("CASE_SENSITIVE").setType(Types.BOOLEAN).build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"SEARCHABLE").build());
		tableDef.add(new ColumnDefImpl.Builder().setNamespace(MetaNamespace.NS)
				.setLocalName("UNSIGNED_ATTRIBUTE").setType(Types.BOOLEAN)
				.build());
		tableDef.add(new ColumnDefImpl.Builder().setNamespace(MetaNamespace.NS)
				.setLocalName("FIXED_PREC_SCALE").setType(Types.BOOLEAN)
				.build());
		tableDef.add(new ColumnDefImpl.Builder().setNamespace(MetaNamespace.NS)
				.setLocalName("AUTO_INCREMENT").setType(Types.BOOLEAN).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "LOCAL_TYPE_NAME")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"MINIMUM_SCALE").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"MAXIMUM_SCALE").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"SQL_DATA_TYPE").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"SQL_DATETIME_SUB").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"NUM_PREC_RADIX").build());
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.INDEXINFO_TABLE);
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TABLE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TABLE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"TABLE_NAME").build());
		tableDef.add(new ColumnDefImpl.Builder().setNamespace(MetaNamespace.NS)
				.setLocalName("NON_UNIQUE").setType(Types.BOOLEAN).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "INDEX_QUALIFIER")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "INDEX_NAME")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"TYPE").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"ORDINAL_POSITION").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "COLUMN_NAME")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "ASC_OR_DESC")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"CARDINALITY").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"PAGES").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "FILTER_CONDITION")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.addKey("NON_UNIQUE");
		tableDef.addKey("TYPE");
		tableDef.addKey("INDEX_NAME");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS, MetaSchema.UDT_TABLES);
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TYPE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TYPE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"TYPE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"CLASS_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"DATA_TYPE").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"REMARKS").build());
		tableDef.add(ColumnDefImpl.Builder
				.getSmallIntBuilder(MetaNamespace.NS, "BASE_TYPE")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.addKey("DATA_TYPE");
		tableDef.addKey("TYPE_CAT");
		tableDef.addKey("TYPE_SCHEM");
		tableDef.addKey("TYPE_NAME");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.SUPER_TYPES_TABLE);
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TYPE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TYPE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"TYPE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "SUPERTYPE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "SUPERTYPE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"SUPERTYPE_NAME").build());
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.SUPER_TABLES_TABLE);
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TABLE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TABLE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"TABLE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"SUPERTABLE_NAME").build());
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.ATTRIBUTES_TABLE);
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TYPE_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "TYPE_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"TYPE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"ATTR_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"DATA_TYPE").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"ATTR_TYPE_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"ATTR_SIZE").build());
		tableDef.add(ColumnDefImpl.Builder
				.getIntegerBuilder(MetaNamespace.NS, "DECIMAL_DIGITS")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"NUM_PREC_RADIX").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"NULLABLE").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "REMARKS")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "ATTR_DEF")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getIntegerBuilder(MetaNamespace.NS, "SQL_DATA_TYPE")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getIntegerBuilder(MetaNamespace.NS, "SQL_DATETIME_SUB")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"CHAR_OCTET_LENGTH").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"ORDINAL_POSITION").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"IS_NULLABLE").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "SCOPE_CATALOG")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "SCOPE_SCHEMA")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "SCOPE_TABLE")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getSmallIntBuilder(MetaNamespace.NS, "SOURCE_DATA_TYPE")
				.setNullable(DatabaseMetaData.columnNullable).build());
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.FUNCTION_COLUMNS_TABLE);
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "FUNCTION_CAT")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "FUNCTION_SCHEM")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"FUNCTION_NAME").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"COLUMN_NAME").build());
		;
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"COLUMN_TYPE").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"DATA_TYPE").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"TYPE_NAME").build());
		;
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"PRECISION").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"LENGTH").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"SCALE").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"RADIX").build());
		tableDef.add(ColumnDefImpl.Builder.getSmallIntBuilder(MetaNamespace.NS,
				"NULLABLE").build());
		tableDef.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "REMARKS")
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"CHAR_OCTET_LENGTH").build());
		tableDef.add(ColumnDefImpl.Builder.getIntegerBuilder(MetaNamespace.NS,
				"ORDINAL_POSITION").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"IS_NULLABLE").build());
		tableDef.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"SPECIFIC_NAME").build());
		tableDef.addKey("FUNCTION_CAT");
		tableDef.addKey("FUNCTION_SCHEM");
		tableDef.addKey("FUNCTION_NAME");
		tableDef.addKey("SPECIFIC_NAME");
		addTableDef(tableDef);
	}
}

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
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_CAT"));
		tableDef.addKey("TABLE_CAT");
		tableDef.setUnique();
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS, MetaSchema.COLUMNS_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"COLUMN_NAME"));
		tableDef.add(ColumnDefImpl
				.getIntInstance(MetaNamespace.NS, "DATA_TYPE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TYPE_NAME").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"COLUMN_SIZE"));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"BUFFER_LENGTH").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"DECIMAL_DIGITS").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"NUM_PREC_RADIX").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS, "NULLABLE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"REMARKS").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"COLUMN_DEF").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"SQL_DATA_TYPE").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"SQL_DATETIME_SUB")
				.setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"CHAR_OCTET_LENGTH").setNullable(
				DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"ORDINAL_POSITION"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"IS_NULLABLE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"SCOPE_CATLOG").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"SCOPE_SCHEMA").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"SCOPE_TABLE").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(new ColumnDefImpl(MetaNamespace.NS, "SOURCE_DATA_TYPE",
				Types.SMALLINT).setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"IS_AUTOINCREMENT"));
		tableDef.addKey("TABLE_CAT");
		tableDef.addKey("TABLE_SCHEM");
		tableDef.addKey("TABLE_NAME");
		tableDef.addKey("ORDINAL_POSITION");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS, MetaSchema.SCHEMAS_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_SCHEM"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_CATALOG").setNullable(DatabaseMetaData.columnNullable));
		tableDef.addKey("TABLE_CATALOG");
		tableDef.addKey("TABLE_SCHEM");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.CLIENT_INFO_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS, "NAME"));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS, "MAX_LEN"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"DEFAULT_VALUE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"DESCRIPTION"));
		tableDef.addKey("NAME");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.FUNCTIONS_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FUNCTION_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FUNCTION_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FUNCTION_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"REMARKS"));
		tableDef.add(new MetaColumn("FUNCTION_TYPE", Types.SMALLINT));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"SPECIFIC_NAME"));
		tableDef.addKey("FUNCTION_CAT");
		tableDef.addKey("FUNCTION_SCHEM");
		tableDef.addKey("FUNCTION_NAME");
		tableDef.addKey("SPECIFIC_NAME");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.TABLE_PRIVILEGES_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"GRANTOR").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"GRANTEE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PRIVILEGE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"IS_GRANTABLE"));
		tableDef.addKey("TABLE_CAT");
		tableDef.addKey("TABLE_SCHEM");
		tableDef.addKey("TABLE_NAME");
		tableDef.addKey("PRIVILEGE");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS, MetaSchema.BEST_ROW_TABLE);
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS, "SCOPE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"COLUMN_NAME"));
		tableDef.add(ColumnDefImpl
				.getIntInstance(MetaNamespace.NS, "DATA_TYPE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TYPE_NAME").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"COLUMN_SIZE"));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"BUFFER_LENGTH"));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"DECIMAL_DIGITS"));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"PSEUDO_COLUMN"));
		tableDef.addKey("SCOPE");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.PROCEDURES_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PROCEDURE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PROCEDURE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PROCEDURE_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FUTURE1").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FUTURE2").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FUTURE3").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"REMARKS"));
		tableDef.add(new MetaColumn("PROCEDURE_TYPE", Types.SMALLINT));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"SPECIFIC_NAME"));
		tableDef.addKey("PROCEDURE_CAT");
		tableDef.addKey("PROCEDURE_SCHEM");
		tableDef.addKey("PROCEDURE_NAME");
		tableDef.addKey("SPECIFIC_NAME");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.PROCEDURE_COLUMNS_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PROCEDURE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PROCEDURE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PROCEDURE_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"COLUMN_NAME"));
		tableDef.add(new MetaColumn("COLUMN_TYPE", Types.SMALLINT));
		tableDef.add(ColumnDefImpl
				.getIntInstance(MetaNamespace.NS, "DATA_TYPE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TYPE_NAME"));
		tableDef.add(ColumnDefImpl
				.getIntInstance(MetaNamespace.NS, "PRECISION"));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS, "LENGTH"));
		tableDef.add(new MetaColumn("SCALE", Types.SMALLINT)
				.setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(new MetaColumn("RADIX", Types.SMALLINT));
		tableDef.add(new MetaColumn("NULLABLE", Types.SMALLINT));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"REMARKS"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"COLUMN_DEF").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"SQL_DATA_TYPE").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"SQL_DATETIME_SUB")
				.setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"CHAR_OCTET_LENGTH").setNullable(
				DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"ORDINAL_POSITION"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"IS_NULLABLE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"SPECIFIC_NAME"));
		tableDef.addKey("PROCEDURE_CAT");
		tableDef.addKey("PROCEDURE_SCHEM");
		tableDef.addKey("PROCEDURE_NAME");
		tableDef.addKey("SPECIFIC_NAME");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS, MetaSchema.TABLES_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_TYPE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"REMARKS"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TYPE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TYPE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TYPE_NAME").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"SELF_REFERENCING_COL_NAME").setNullable(
				DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"REF_GENERATION").setNullable(DatabaseMetaData.columnNullable));
		tableDef.addKey("TABLE_TYPE");
		tableDef.addKey("TABLE_CAT");
		tableDef.addKey("TABLE_SCHEM");
		tableDef.addKey("TABLE_NAME");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.TABLE_TYPES_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_TYPE"));
		tableDef.addKey("TABLE_TYPE");
		tableDef.setUnique();
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.COLUMN_PRIVILIGES_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"COLUMN_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"GRANTOR").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"GRANTEE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PRIVILEGE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"IS_GRANTABLE"));
		tableDef.addKey("COLUMN_NAME");
		tableDef.addKey("PRIVILEGE");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.VERSION_COLUMNS_TABLE);
		tableDef.add(new MetaColumn("SCOPE", Types.SMALLINT));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"COLUMN_NAME"));
		tableDef.add(ColumnDefImpl
				.getIntInstance(MetaNamespace.NS, "DATA_TYPE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TYPE_NAME"));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"COLUMN_SIZE"));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"BUFFER_LENGTH"));
		tableDef.add(new MetaColumn("DECIMAL_DIGITS", Types.SMALLINT));
		tableDef.add(new MetaColumn("PSEUDO_COLUMN", Types.SMALLINT));
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.PRIMARY_KEY_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"COLUMN_NAME"));
		tableDef.add(new MetaColumn("KEY_SEQ", Types.SMALLINT));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PK_NAME").setNullable(DatabaseMetaData.columnNullable));
		tableDef.addKey("COLUMN_NAME");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.IMPORTED_KEYS_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PKTABLE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PKTABLE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PKTABLE_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PKCOLUMN_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FKTABLE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FKTABLE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FKTABLE_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FKCOLUMN_NAME"));
		tableDef.add(new MetaColumn("KEY_SEQ", Types.SMALLINT));
		tableDef.add(new MetaColumn("UPDATE_RULE", Types.SMALLINT));
		tableDef.add(new MetaColumn("DELETE_RULE", Types.SMALLINT));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FK_NAME").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PK_NAME").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(new MetaColumn("DEFERRABILITY", Types.SMALLINT));
		tableDef.addKey("PKTABLE_CAT");
		tableDef.addKey("PKTABLE_SCHEM");
		tableDef.addKey("PKTABLE_NAME");
		tableDef.addKey("KEY_SEQ");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.EXPORTED_KEYS_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PKTABLE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PKTABLE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PKTABLE_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PKCOLUMN_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FKTABLE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FKTABLE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FKTABLE_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FKCOLUMN_NAME"));
		tableDef.add(new MetaColumn("KEY_SEQ", Types.SMALLINT));
		tableDef.add(new MetaColumn("UPDATE_RULE", Types.SMALLINT));
		tableDef.add(new MetaColumn("DELETE_RULE", Types.SMALLINT));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FK_NAME").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PK_NAME").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(new MetaColumn("DEFERRABILITY", Types.SMALLINT));
		tableDef.addKey("FKTABLE_CAT");
		tableDef.addKey("FKTABLE_SCHEM");
		tableDef.addKey("FKTABLE_NAME");
		tableDef.addKey("KEY_SEQ");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS, MetaSchema.XREF_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PKTABLE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PKTABLE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PKTABLE_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PKCOLUMN_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FKTABLE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FKTABLE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FKTABLE_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FKCOLUMN_NAME"));
		tableDef.add(new MetaColumn("KEY_SEQ", Types.SMALLINT));
		tableDef.add(new MetaColumn("UPDATE_RULE", Types.SMALLINT));
		tableDef.add(new MetaColumn("DELETE_RULE", Types.SMALLINT));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FK_NAME").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"PK_NAME").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(new MetaColumn("DEFERRABILITY", Types.SMALLINT));
		tableDef.addKey("FKTABLE_CAT");
		tableDef.addKey("FKTABLE_SCHEM");
		tableDef.addKey("FKTABLE_NAME");
		tableDef.addKey("KEY_SEQ");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS, MetaSchema.TYPEINFO_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TYPE_NAME"));
		tableDef.add(ColumnDefImpl
				.getIntInstance(MetaNamespace.NS, "DATA_TYPE"));
		tableDef.add(ColumnDefImpl
				.getIntInstance(MetaNamespace.NS, "PRECISION"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"LITERAL_PREFIX").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"LITERAL_SUFFIX").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"CREATE_PARAMS").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(new MetaColumn("NULLABLE", Types.SMALLINT));
		tableDef.add(new MetaColumn("CASE_SENSITIVE", Types.BOOLEAN));
		tableDef.add(new MetaColumn("SEARCHABLE", Types.SMALLINT));
		tableDef.add(new MetaColumn("UNSIGNED_ATTRIBUTE", Types.BOOLEAN));
		tableDef.add(new MetaColumn("FIXED_PREC_SCALE", Types.BOOLEAN));
		tableDef.add(new MetaColumn("AUTO_INCREMENT", Types.BOOLEAN));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"LOCAL_TYPE_NAME").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(new MetaColumn("MINIMUM_SCALE", Types.SMALLINT));
		tableDef.add(new MetaColumn("MAXIMUM_SCALE", Types.SMALLINT));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"SQL_DATA_TYPE"));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"SQL_DATETIME_SUB"));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"NUM_PREC_RADIX"));
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.INDEXINFO_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_NAME"));
		tableDef.add(new MetaColumn("NON_UNIQUE", Types.BOOLEAN));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"INDEX_QUALIFIER").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"INDEX_NAME").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(new MetaColumn("TYPE", Types.SMALLINT));
		tableDef.add(new MetaColumn("ORDINAL_POSITION", Types.SMALLINT));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"COLUMN_NAME").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"ASC_OR_DESC").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"CARDINALITY"));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS, "PAGES"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FILTER_CONDITION")
				.setNullable(DatabaseMetaData.columnNullable));
		tableDef.addKey("NON_UNIQUE");
		tableDef.addKey("TYPE");
		tableDef.addKey("INDEX_NAME");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS, MetaSchema.UDT_TABLES);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TYPE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TYPE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TYPE_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"CLASS_NAME"));
		tableDef.add(ColumnDefImpl
				.getIntInstance(MetaNamespace.NS, "DATA_TYPE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"REMARKS"));
		tableDef.add(new MetaColumn("BASE_TYPE", Types.SMALLINT)
				.setNullable(DatabaseMetaData.columnNullable));
		tableDef.addKey("DATA_TYPE");
		tableDef.addKey("TYPE_CAT");
		tableDef.addKey("TYPE_SCHEM");
		tableDef.addKey("TYPE_NAME");
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.SUPER_TYPES_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TYPE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TYPE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TYPE_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"SUPERTYPE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"SUPERTYPE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"SUPERTYPE_NAME"));
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.SUPER_TABLES_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TABLE_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"SUPERTABLE_NAME"));
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.ATTRIBUTES_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TYPE_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TYPE_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TYPE_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"ATTR_NAME"));
		tableDef.add(ColumnDefImpl
				.getIntInstance(MetaNamespace.NS, "DATA_TYPE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"ATTR_TYPE_NAME"));
		tableDef.add(ColumnDefImpl
				.getIntInstance(MetaNamespace.NS, "ATTR_SIZE"));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"DECIMAL_DIGITS").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"NUM_PREC_RADIX"));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS, "NULLABLE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"REMARKS").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"ATTR_DEF").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"SQL_DATA_TYPE").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"SQL_DATETIME_SUB")
				.setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"CHAR_OCTET_LENGTH"));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"ORDINAL_POSITION"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"IS_NULLABLE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"SCOPE_CATALOG").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"SCOPE_SCHEMA").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"SCOPE_TABLE").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(new MetaColumn("SOURCE_DATA_TYPE", Types.SMALLINT)
				.setNullable(DatabaseMetaData.columnNullable));
		addTableDef(tableDef);

		tableDef = new TableDefImpl(MetaNamespace.NS,
				MetaSchema.FUNCTION_COLUMNS_TABLE);
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FUNCTION_CAT").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FUNCTION_SCHEM").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"FUNCTION_NAME"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"COLUMN_NAME"));
		;
		tableDef.add(new MetaColumn("COLUMN_TYPE", Types.SMALLINT));
		tableDef.add(ColumnDefImpl
				.getIntInstance(MetaNamespace.NS, "DATA_TYPE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"TYPE_NAME"));
		;
		tableDef.add(ColumnDefImpl
				.getIntInstance(MetaNamespace.NS, "PRECISION"));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS, "LENGTH"));
		tableDef.add(new MetaColumn("SCALE", Types.SMALLINT));
		tableDef.add(new MetaColumn("RADIX", Types.SMALLINT));
		tableDef.add(new MetaColumn("NULLABLE", Types.SMALLINT));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"REMARKS").setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"CHAR_OCTET_LENGTH"));
		tableDef.add(ColumnDefImpl.getIntInstance(MetaNamespace.NS,
				"ORDINAL_POSITION"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"IS_NULLABLE"));
		tableDef.add(ColumnDefImpl.getStringInstance(MetaNamespace.NS,
				"SPECIFIC_NAME"));
		tableDef.addKey("FUNCTION_CAT");
		tableDef.addKey("FUNCTION_SCHEM");
		tableDef.addKey("FUNCTION_NAME");
		tableDef.addKey("SPECIFIC_NAME");
		addTableDef(tableDef);
	}
}

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
import java.sql.ResultSetMetaData;
import java.sql.Types;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.DatasetProducer;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumn;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumnDef;
import org.xenei.jdbc4sparql.impl.rdf.RdfKey;
import org.xenei.jdbc4sparql.impl.rdf.RdfKeySegment;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef;
import org.xenei.jdbc4sparql.impl.rdf.ResourceBuilder;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

public class MetaCatalogBuilder {
	public static Catalog getInstance(final DatasetProducer dsProducer) {
		final Model model = dsProducer
				.getMetaDataModel(MetaCatalogBuilder.LOCAL_NAME);
		final RdfCatalog cat = new RdfCatalog.Builder()
		.setName(MetaCatalogBuilder.LOCAL_NAME)
		.setLocalModel(dsProducer.getMetaDatasetUnionModel())
		.build(model);

		final RdfSchema schema = new RdfSchema.Builder().setCatalog(cat)
				.setName(MetaCatalogBuilder.SCHEMA_NAME).build(model);
		// populate the catalog
		new MetaCatalogBuilder(schema, model).build();

		return cat;
	}

	public static RdfColumnDef.Builder getNonNullBooleanBuilder() {
		return new RdfColumnDef.Builder().setType(Types.BOOLEAN).setNullable(
				DatabaseMetaData.columnNoNulls);
	}

	public static RdfColumnDef.Builder getNonNullIntBuilder() {
		return RdfColumnDef.Builder.getIntegerBuilder().setNullable(
				DatabaseMetaData.columnNoNulls);
	}

	public static RdfColumnDef.Builder getNonNullShortBuilder() {
		return RdfColumnDef.Builder.getSmallIntBuilder().setNullable(
				DatabaseMetaData.columnNoNulls);
	}

	public static RdfColumnDef.Builder getNonNullStringBuilder() {
		return RdfColumnDef.Builder.getStringBuilder().setNullable(
				DatabaseMetaData.columnNoNulls);
	}

	public static RdfColumnDef.Builder getNullBooleanBuilder() {
		return new RdfColumnDef.Builder().setType(Types.BOOLEAN).setNullable(
				DatabaseMetaData.columnNullable);
	}

	public static RdfColumnDef.Builder getNullIntBuilder() {
		return RdfColumnDef.Builder.getIntegerBuilder().setNullable(
				DatabaseMetaData.columnNullable);
	}

	public static RdfColumnDef.Builder getNullShortBuilder() {
		return RdfColumnDef.Builder.getSmallIntBuilder().setNullable(
				DatabaseMetaData.columnNullable);
	}

	public static RdfColumnDef.Builder getNullStringBuilder() {
		return RdfColumnDef.Builder.getStringBuilder().setNullable(
				DatabaseMetaData.columnNullable);
	}

	public static final String NS = "http://org.xenei.jdbc4sparql/meta#";
	public static final String LOCAL_NAME = "METADATA";
	public static final String SCHEMA_NAME = "Schema";
	public static final String TABLE_TYPE = "SYSTEM TABLE";
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

	public static final String REMARK = "Created by MetaCatalogBuilder";

	private final ColumnDef nonNullString;
	private final ColumnDef nullableString;

	private final ColumnDef nonNullInt;

	private final ColumnDef nullableInt;

	private final ColumnDef nonNullShort;

	private final ColumnDef nullableShort;

	private final ColumnDef nullableBoolean;

	private final ColumnDef nonNullBoolean;

	private final Model model;

	private final RdfSchema schema;

	private final ResourceBuilder resourceBuilder;

	private MetaCatalogBuilder(final RdfSchema schema, final Model model) {
		this.schema = schema;
		this.model = model;
		resourceBuilder = new ResourceBuilder(model);
		nonNullString = MetaCatalogBuilder.getNonNullStringBuilder().build(
				model);
		nullableString = MetaCatalogBuilder.getNullStringBuilder().build(model);
		nonNullInt = MetaCatalogBuilder.getNonNullIntBuilder().build(model);
		nullableInt = MetaCatalogBuilder.getNullIntBuilder().build(model);
		nonNullShort = MetaCatalogBuilder.getNonNullShortBuilder().build(model);
		nullableShort = MetaCatalogBuilder.getNullShortBuilder().build(model);
		nullableBoolean = MetaCatalogBuilder.getNullBooleanBuilder().build(
				model);
		nonNullBoolean = MetaCatalogBuilder.getNonNullBooleanBuilder().build(
				model);
	}

	private void addAttributesTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nonNullString) // TYPE_CAT
		.addColumnDef(nonNullString) // TYPE_SCHEM
		.addColumnDef(nonNullString) // TYPE_NAME
		.addColumnDef(nullableString) // ATTR_NAME
		.addColumnDef(nonNullInt) // DATA_TYPE
		.addColumnDef(nullableString) // ATTR_TYPE_NAME
		.addColumnDef(nullableInt) // ATTR_SIZE
		.addColumnDef(nullableInt) // DECIMAL_DIGITS
		.addColumnDef(nullableInt) // NUM_PREC_RADIX
		.addColumnDef(nonNullInt) // NULLABLE
		.addColumnDef(nullableString) // REMARKS
		.addColumnDef(nullableString) // ATTR_DEF
		.addColumnDef(nonNullInt) // SQL_DATA_TYPE
		.addColumnDef(nullableInt) // SQL_DATETIME_SUB
		.addColumnDef(nonNullInt) // CHAR_OCTET_LENGTH
		.addColumnDef(nonNullInt) // ORDINAL_POSITION
		.addColumnDef(nullableString) // IS_NULLABLE
		.addColumnDef(nullableString) // SCOPE_CATALOG
		.addColumnDef(nullableString) // SCOPE_SCHEMA
		.addColumnDef(nullableString) // SCOPE_TABLE
		.addColumnDef(nullableShort) // SOURCE_DATA_TYPE
		.build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setSchema(schema).setTableDef(tableDef)
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.ATTRIBUTES_TABLE)
		.setRemarks(MetaCatalogBuilder.REMARK).setColumn(0, "TYPE_CAT")
		.setColumn(1, "TYPE_SCHEM").setColumn(2, "TYPE_NAME")
		.setColumn(3, "ATTR_NAME").setColumn(4, "DATA_TYPE")
		.setColumn(5, "ATTR_TYPE_NAME").setColumn(6, "ATTR_SIZE")
		.setColumn(7, "DECIMAL_DIGITS").setColumn(8, "NUM_PREC_RADIX")
		.setColumn(9, "NULLABLE").setColumn(10, "REMARKS")
		.setColumn(11, "ATTR_DEF").setColumn(12, "SQL_DATA_TYPE")
		.setColumn(13, "SQL_DATETIME_SUB")
		.setColumn(14, "CHAR_OCTET_LENGTH")
		.setColumn(15, "ORDINAL_POSITION").setColumn(16, "IS_NULLABLE")
		.setColumn(17, "SCOPE_CATALOG").setColumn(18, "SCOPE_SCHEMA")
		.setColumn(19, "SCOPE_TABLE").setColumn(20, "SOURCE_DATA_TYPE");
		setNull(builder);
		builder.build(model);
	}

	private void addBestRowTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nonNullString)
		// SCOPE
		.addColumnDef(nonNullString)
		// COLUMN_NAME
		.addColumnDef(nonNullInt)
		// DATA_TYPE
		.addColumnDef(nonNullString)
		// TYPE_NAME
		.addColumnDef(nullableInt)
		// COLUMN_SIZE
		.addColumnDef(nullableInt)
		// BUFFER_LENGTH
		.addColumnDef(nullableInt)
		// DECIMAL_DIGITS
		.addColumnDef(nullableInt)
		// PSEUDO_COLUMN
		.setSortKey(
				new RdfKey.Builder().addSegment(
						new RdfKeySegment.Builder().setAscending(true)
						.setIdx(0).build(model)).build(model))
						.build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setSchema(schema).setTableDef(tableDef)
		.setRemarks(MetaCatalogBuilder.REMARK)
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.BEST_ROW_TABLE)
		.setColumn(0, "SCOPE").setColumn(1, "COLUMN_NAME")
		.setColumn(2, "DATA_TYPE").setColumn(3, "TYPE_NAME")
		.setColumn(4, "COLUMN_SIZE").setColumn(5, "BUFFER_LENGTH")
		.setColumn(6, "DECIMAL_DIGITS").setColumn(7, "PSEUDO_COLUMN");
		setNull(builder).build(model);
	}

	private void addCatalogsTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nonNullString)
		.setSortKey(
				new RdfKey.Builder()
				.addSegment(
						new RdfKeySegment.Builder()
						.setAscending(true).setIdx(0)
						.build(model)).setUnique(true)
						.build(model)).build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setSchema(schema)
		.setTableDef(tableDef)
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.CATALOGS_TABLE)
		.setRemarks(MetaCatalogBuilder.REMARK)
		.addQuerySegment(
				makeQS("%1$s", RDF.type, makeNode(ResourceBuilder
						.getFQName(RdfCatalog.class))))
						.setColumn(0, "TABLE_CAT");
		builder.getColumn(0)
		.addQuerySegment(makeQS("%1$s", RDFS.label, "%2$s"))
		.setRemarks(MetaCatalogBuilder.REMARK);
		builder.build(model);
	}

	private void addClientInfoTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nonNullString) // NAME
		.addColumnDef(nullableInt) // MAX_LEN
		.addColumnDef(nullableString) // DEFAULT_VALUE
		.addColumnDef(nullableString) // DESCRIPTION
		.setSortKey(
				new RdfKey.Builder()
				.addSegment(
						new RdfKeySegment.Builder()
						.setAscending(true).setIdx(0)
						.build(model)).setUnique(true)
						.build(model)).build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setSchema(schema).setTableDef(tableDef)
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.CLIENT_INFO_TABLE)
		.setRemarks(MetaCatalogBuilder.REMARK).setColumn(0, "NAME")
		.setColumn(1, "MAX_LEN").setColumn(2, "DEFAULT_VALUE")
		.setColumn(3, "DESCRIPTION");

		setNull(builder).build(model);
	}

	private void addColumnPriviligesTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nullableString) // TABLE_CAT
		.addColumnDef(nullableString) // TABLE_SCHEM
		.addColumnDef(nonNullString) // TABLE_NAME
		.addColumnDef(nonNullString) // COLUMN_NAME
		.addColumnDef(nullableString) // GRANTOR
		.addColumnDef(nonNullString) // GRANTEE
		.addColumnDef(nonNullString) // PRIVILEGE
		.addColumnDef(nonNullString) // IS_GRANTABLE
		.setSortKey(
				new RdfKey.Builder()
				.addSegment(
						new RdfKeySegment.Builder()
						.setAscending(true).setIdx(3)
						.build(model))
						.addSegment(
								new RdfKeySegment.Builder()
								.setAscending(true).setIdx(6)
								.build(model)).setUnique(false)
								.build(model)).build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.COLUMN_PRIVILIGES_TABLE)
		.setSchema(schema).setTableDef(tableDef)
		.setRemarks(MetaCatalogBuilder.REMARK)
		.setColumn(0, "TABLE_CAT").setColumn(1, "TABLE_SCHEM")
		.setColumn(2, "TABLE_NAME").setColumn(3, "COLUMN_NAME")
		.setColumn(4, "GRANTOR").setColumn(5, "GRANTEE")
		.setColumn(6, "PRIVILEGE").setColumn(7, "IS_GRANTABLE");

		setNull(builder).build(model);

	}

	private void addColumnsTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nonNullString) // TABLE_CAT
		.addColumnDef(nonNullString) // TABLE_SCHEM
		.addColumnDef(nonNullString) // TABLE_NAME
		.addColumnDef(nonNullString) // COLUMN_NAME
		.addColumnDef(nonNullInt) // DATA_TYPE
		.addColumnDef(nullableString) // TYPE_NAME
		.addColumnDef(nonNullInt) // COLUMN_SIZE
		.addColumnDef(nullableInt) // BUFFER_LENGTH
		.addColumnDef(nullableInt) // DECIMAL_DIGITS
		.addColumnDef(nullableInt) // NUM_PREC_RADIX
		.addColumnDef(nonNullInt) // NULLABLE
		.addColumnDef(nullableString) // REMARKS
		.addColumnDef(nullableString) // COLUMN_DEF
		.addColumnDef(nullableInt) // SQL_DATA_TYPE
		.addColumnDef(nullableInt) // SQL_DATETIME_SUB
		.addColumnDef(nullableInt) // CHAR_OCTET_LENGTH
		.addColumnDef(nonNullInt) // ORDINAL_POSITION
		.addColumnDef(nullableString) // IS_NULLABLE
		.addColumnDef(nullableString) // SCOPE_CATLOG
		.addColumnDef(nullableString) // SCOPE_SCHEMA
		.addColumnDef(nullableString) // SCOPE_TABLE
		.addColumnDef(nullableShort) // SOURCE_DATA_TYPE
		.addColumnDef(nonNullString) // IS_AUTOINCREMENT

		.setPrimaryKey(
				new RdfKey.Builder()
				.addSegment(
						new RdfKeySegment.Builder()
						.setAscending(true).setIdx(0)
						.build(model))
						.addSegment(
								new RdfKeySegment.Builder()
								.setAscending(true).setIdx(1)
								.build(model))
								.addSegment(
										new RdfKeySegment.Builder()
										.setAscending(true).setIdx(2)
										.build(model))
										.addSegment(
												new RdfKeySegment.Builder()
												.setAscending(true).setIdx(16)
												.build(model)).setUnique(true)
												.build(model)).build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setSchema(schema).setTableDef(tableDef)
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.COLUMNS_TABLE)
		.setRemarks(MetaCatalogBuilder.REMARK)
		.setColumn(0, "TABLE_CAT").setColumn(1, "TABLE_SCHEM")
		.setColumn(2, "TABLE_NAME").setColumn(3, "COLUMN_NAME")
		.setColumn(4, "DATA_TYPE").setColumn(5, "TYPE_NAME")
		.setColumn(6, "COLUMN_SIZE").setColumn(7, "BUFFER_LENGTH")
		.setColumn(8, "DECIMAL_DIGITS").setColumn(9, "NUM_PREC_RADIX")
		.setColumn(10, "NULLABLE").setColumn(11, "REMARKS")
		.setColumn(12, "COLUMN_DEF").setColumn(13, "SQL_DATA_TYPE")
		.setColumn(14, "SQL_DATETIME_SUB")
		.setColumn(15, "CHAR_OCTET_LENGTH")
		.setColumn(16, "ORDINAL_POSITION").setColumn(17, "IS_NULLABLE")
		.setColumn(18, "SCOPE_CATLOG").setColumn(19, "SCOPE_SCHEMA")
		.setColumn(20, "SCOPE_TABLE").setColumn(21, "SOURCE_DATA_TYPE")
		.setColumn(22, "IS_AUTOINCREMENT");
		final String vTable = "?" + NameUtils.createUUIDName();
		final String vSchema = "?" + NameUtils.createUUIDName();
		final String vCat = "?" + NameUtils.createUUIDName();
		final String vColDef = "?" + NameUtils.createUUIDName();
		final String vInc = "?" + NameUtils.createUUIDName();
		final String vCol = "?" + NameUtils.createUUIDName();
		final String vOrd = "?" + NameUtils.createUUIDName();
		final String vNullable = "?" + NameUtils.createUUIDName();

		builder.addQuerySegment(
				makeQS("%1$s", RDF.type,
						makeNode(ResourceBuilder.getFQName(RdfColumn.class))))
						.addQuerySegment(
								makeQS("%1$s", resourceBuilder.getProperty(
										RdfColumn.class, "table"), vTable))
										.addQuerySegment(
												makeQS(vSchema, resourceBuilder.getProperty(
														RdfSchema.class, "tables"), vTable))
														.addQuerySegment(
																makeQS(vCat, resourceBuilder.getProperty(
																		RdfCatalog.class, "schemas"), vSchema))
																		.addQuerySegment(
																				makeQS("%1$s", resourceBuilder.getProperty(
																						RdfColumn.class, "columnDef"), vColDef))
																						.addQuerySegment(
																								makeQS(vColDef, resourceBuilder.getProperty(
																										RdfColumnDef.class, "autoIncrement"), vInc))
																										.addQuerySegment(
																												makeQS(vTable, resourceBuilder.getProperty(
																														RdfTable.class, "column"), vCol));

		// TABLE_CAT
		builder.getColumn(0).addQuerySegment(makeQS(vCat, RDFS.label, "%2$s"))
		.setRemarks(MetaCatalogBuilder.REMARK);
		// TABLE_SCHEM
		builder.getColumn(1)
		.addQuerySegment(makeQS(vSchema, RDFS.label, "%2$s"))
		.setRemarks(MetaCatalogBuilder.REMARK);
		// TABLE_NAME
		builder.getColumn(2)
		.addQuerySegment(makeQS(vTable, RDFS.label, "%2$s"))
		.setRemarks(MetaCatalogBuilder.REMARK);
		// COLUMN_NAME
		builder.getColumn(3)
		.addQuerySegment(makeQS("%1$s", RDFS.label, "%2$s"))
		.setRemarks(MetaCatalogBuilder.REMARK);
		// DATA_TYPE
		builder.getColumn(4)
		.addQuerySegment(
				makeQS(vColDef, resourceBuilder.getProperty(
						RdfColumnDef.class, "type"), "%2$s"))
						.setRemarks(MetaCatalogBuilder.REMARK);
		// TYPE_NAME
		builder.getColumn(5)
		.addQuerySegment(
				makeQS(vColDef, resourceBuilder.getProperty(
						RdfColumnDef.class, "typeName"), "%2$s"))
						.setRemarks(MetaCatalogBuilder.REMARK);
		// COLUMN_SIZE
		builder.getColumn(6)
		.addQuerySegment(
				makeQS(vColDef, resourceBuilder.getProperty(
						RdfColumnDef.class, "scale"), "%2$s"))
						.setRemarks(MetaCatalogBuilder.REMARK);
		// BUFFER_LENGTH (not used)
		setNull(builder.getColumn(7));
		// DECIMAL_DIGITS
		setNull(builder.getColumn(8));
		// NUM_PREC_RADIX
		setNull(builder.getColumn(9));
		// NULLABLE
		builder.getColumn(10)
		.addQuerySegment(
				makeQS(vColDef, resourceBuilder.getProperty(
						RdfColumnDef.class, "nullable"), "%2$s"))
						.setRemarks(MetaCatalogBuilder.REMARK);
		// REMARKS
		builder.getColumn(11)
		.addQuerySegment(
				makeQS("%1$s", resourceBuilder.getProperty(
						RdfColumn.class, "remarks"), "%2$s"))
						.setRemarks(MetaCatalogBuilder.REMARK);
		// COLUMN_DEF
		setNull(builder.getColumn(12));
		// SQL_DATA_TYPE
		setNull(builder.getColumn(13));
		// SQL_DATETIME_SUB
		setNull(builder.getColumn(14));
		// CHAR_OCTET_LENGTH
		setNull(builder.getColumn(15));
		// ORDINAL_POSITION
		builder.getColumn(16)
		// list list:index (index member)
		.addQuerySegment(
				makeQS(vTable, resourceBuilder.getProperty(
						RdfTable.class, "column"), "_:columns"))
						.addQuerySegment(
								makeQS("_:columns",
										"http://jena.hpl.hp.com/ARQ/list#index", "( "
												+ vOrd + " %1$s )"))
												.addQuerySegment("BIND( ( " + vOrd + " + 1 ) as %2$s ).")
												.setRemarks(MetaCatalogBuilder.REMARK);
		// IS_NULLABLE
		builder.getColumn(17)
		// YES NO or ""
		.addQuerySegment(
				makeQS(vColDef, resourceBuilder.getProperty(
						RdfColumnDef.class, "nullable"), vNullable))
						.addQuerySegment(
								"BIND( if( " + vNullable + " = "
										+ ResultSetMetaData.columnNullable
										+ ", 'YES', if( " + vNullable + " = "
										+ ResultSetMetaData.columnNoNulls
										+ ", 'NO', '')) as %2$s)")
										.setRemarks(MetaCatalogBuilder.REMARK);
		// SCOPE_CATALOG
		setNull(builder.getColumn(18));
		// SCOPE_SCHEMA
		setNull(builder.getColumn(19));
		// SCOPE_TABLE
		setNull(builder.getColumn(20));
		// SOURCE_DATA_TYPE
		builder.getColumn(21)
		.addQuerySegment(
				makeQS(vColDef, resourceBuilder.getProperty(
						RdfColumnDef.class, "type"), "%2$s"))
						.setRemarks(MetaCatalogBuilder.REMARK);

		// IS_AUTOINCREMENT
		builder.getColumn(22)
		.addQuerySegment(
				makeQS(vColDef, resourceBuilder.getProperty(
						RdfColumnDef.class, "autoIncrement"), vInc))
						.addQuerySegment(
								"BIND( if( " + vInc + ", 'YES', 'NO') as %2$s)")
								.setRemarks(MetaCatalogBuilder.REMARK);

		builder.build(model);
	}

	private void addExportedKeysTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nullableString) // PKTABLE_CAT
		.addColumnDef(nullableString) // PKTABLE_SCHEM
		.addColumnDef(nonNullString) // PKTABLE_NAME
		.addColumnDef(nonNullString) // PKCOLUMN_NAME
		.addColumnDef(nullableString) // FKTABLE_CAT
		.addColumnDef(nullableString) // FKTABLE_SCHEM
		.addColumnDef(nonNullString) // FKTABLE_NAME
		.addColumnDef(nonNullString) // FKCOLUMN_NAME
		.addColumnDef(nullableShort) // KEY_SEQ
		.addColumnDef(nullableShort) // UPDATE_RULE
		.addColumnDef(nullableShort) // DELETE_RULE
		.addColumnDef(nullableString) // FK_NAME
		.addColumnDef(nullableString) // PK_NAME
		.addColumnDef(nullableShort) // DEFERRABILITY
		.setSortKey(
				new RdfKey.Builder()
				.addSegment(
						new RdfKeySegment.Builder()
						.setAscending(true).setIdx(0)
						.build(model))
						.addSegment(
								new RdfKeySegment.Builder()
								.setAscending(true).setIdx(1)
								.build(model))
								.addSegment(
										new RdfKeySegment.Builder()
										.setAscending(true).setIdx(2)
										.build(model))
										.addSegment(
												new RdfKeySegment.Builder()
												.setAscending(true).setIdx(8)
												.build(model)).setUnique(true)
												.build(model)).build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.EXPORTED_KEYS_TABLE)
		.setSchema(schema).setTableDef(tableDef)
		.setRemarks(MetaCatalogBuilder.REMARK)
		.setColumn(0, "PKTABLE_CAT").setColumn(1, "PKTABLE_SCHEM")
		.setColumn(2, "PKTABLE_NAME").setColumn(3, "PKCOLUMN_NAME")
		.setColumn(4, "FKTABLE_CAT").setColumn(5, "FKTABLE_SCHEM")
		.setColumn(6, "FKTABLE_NAME").setColumn(7, "FKCOLUMN_NAME")
		.setColumn(8, "KEY_SEQ").setColumn(9, "UPDATE_RULE")
		.setColumn(10, "DELETE_RULE").setColumn(11, "FK_NAME")
		.setColumn(12, "PK_NAME").setColumn(13, "DEFERRABILITY");

		setNull(builder).build(model);
	}

	private void addFunctionColumnsTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nullableString) // FUNCTION_CAT
		.addColumnDef(nullableString) // FUNCTION_SCHEM
		.addColumnDef(nonNullString) // FUNCTION_NAME
		.addColumnDef(nonNullString) // COLUMN_NAME
		.addColumnDef(nullableShort) // COLUMN_TYPE
		.addColumnDef(nullableInt) // DATA_TYPE
		.addColumnDef(nonNullString) // TYPE_NAME
		.addColumnDef(nullableInt) // PRECISION
		.addColumnDef(nullableInt) // LENGTH
		.addColumnDef(nullableShort) // SCALE
		.addColumnDef(nullableShort) // RADIX
		.addColumnDef(nullableShort) // NULLABLE
		.addColumnDef(nullableString) // REMARKS
		.addColumnDef(nullableInt) // CHAR_OCTET_LENGTH
		.addColumnDef(nullableInt) // ORDINAL_POSITION
		.addColumnDef(nonNullString) // IS_NULLABLE
		.addColumnDef(nonNullString) // SPECIFIC_NAME
		.setSortKey(
				new RdfKey.Builder()
				.addSegment(
						new RdfKeySegment.Builder()
						.setAscending(true).setIdx(0)
						.build(model))
						.addSegment(
								new RdfKeySegment.Builder()
								.setAscending(true).setIdx(1)
								.build(model))
								.addSegment(
										new RdfKeySegment.Builder()
										.setAscending(true).setIdx(2)
										.build(model))
										.addSegment(
												new RdfKeySegment.Builder()
												.setAscending(true).setIdx(16)
												.build(model)).setUnique(true)
												.build(model)).build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.FUNCTION_COLUMNS_TABLE)
		.setSchema(schema).setTableDef(tableDef)
		.setRemarks(MetaCatalogBuilder.REMARK)
		.setColumn(0, "FUNCTION_CAT").setColumn(1, "FUNCTION_SCHEM")
		.setColumn(2, "FUNCTION_NAME").setColumn(3, "COLUMN_NAME")
		.setColumn(4, "COLUMN_TYPE").setColumn(5, "DATA_TYPE")
		.setColumn(6, "TYPE_NAME").setColumn(7, "PRECISION")
		.setColumn(8, "LENGTH").setColumn(9, "SCALE")
		.setColumn(10, "RADIX").setColumn(11, "NULLABLE")
		.setColumn(12, "REMARKS").setColumn(13, "CHAR_OCTET_LENGTH")
		.setColumn(14, "ORDINAL_POSITION").setColumn(15, "IS_NULLABLE")
		.setColumn(16, "SPECIFIC_NAME");

		setNull(builder).build(model);

	}

	private void addFunctionsTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nullableString) // FUNCTION_CAT
		.addColumnDef(nullableString) // FUNCTION_SCHEM
		.addColumnDef(nonNullString) // FUNCTION_NAME
		.addColumnDef(nonNullString) // REMARKS
		.addColumnDef(nonNullShort) // FUNCTION_TYPE
		.addColumnDef(nonNullString) // SPECIFIC_NAME
		.setSortKey(
				new RdfKey.Builder()
				.addSegment(
						new RdfKeySegment.Builder()
						.setAscending(true).setIdx(0)
						.build(model))
						.addSegment(
								new RdfKeySegment.Builder()
								.setAscending(true).setIdx(1)
								.build(model))
								.addSegment(
										new RdfKeySegment.Builder()
										.setAscending(true).setIdx(2)
										.build(model))
										.addSegment(
												new RdfKeySegment.Builder()
												.setAscending(true).setIdx(5)
												.build(model)).setUnique(true)
												.build(model)).build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.FUNCTIONS_TABLE).setSchema(schema)
		.setTableDef(tableDef).setRemarks(MetaCatalogBuilder.REMARK)
		.setColumn(0, "FUNCTION_CAT").setColumn(1, "FUNCTION_SCHEM")
		.setColumn(2, "FUNCTION_NAME").setColumn(3, "REMARKS")
		.setColumn(4, "FUNCTION_TYPE").setColumn(5, "SPECIFIC_NAME");

		setNull(builder).build(model);

	}

	private void addImportedKeysTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nullableString) // PKTABLE_CAT
		.addColumnDef(nullableString) // PKTABLE_SCHEM
		.addColumnDef(nonNullString) // PKTABLE_NAME
		.addColumnDef(nonNullString) // PKCOLUMN_NAME
		.addColumnDef(nullableString) // FKTABLE_CAT
		.addColumnDef(nullableString) // FKTABLE_SCHEM
		.addColumnDef(nonNullString) // FKTABLE_NAME
		.addColumnDef(nonNullString) // FKCOLUMN_NAME
		.addColumnDef(nullableShort) // KEY_SEQ
		.addColumnDef(nullableShort) // UPDATE_RULE
		.addColumnDef(nullableShort) // DELETE_RULE
		.addColumnDef(nullableString) // FK_NAME
		.addColumnDef(nullableString) // PK_NAME
		.addColumnDef(nullableShort) // DEFERRABILITY
		.setSortKey(
				new RdfKey.Builder()
				.addSegment(
						new RdfKeySegment.Builder()
						.setAscending(true).setIdx(0)
						.build(model))
						.addSegment(
								new RdfKeySegment.Builder()
								.setAscending(true).setIdx(1)
								.build(model))
								.addSegment(
										new RdfKeySegment.Builder()
										.setAscending(true).setIdx(2)
										.build(model))
										.addSegment(
												new RdfKeySegment.Builder()
												.setAscending(true).setIdx(8)
												.build(model)).setUnique(true)
												.build(model)).build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.IMPORTED_KEYS_TABLE)
		.setSchema(schema).setTableDef(tableDef)
		.setRemarks(MetaCatalogBuilder.REMARK)
		.setColumn(0, "PKTABLE_CAT").setColumn(1, "PKTABLE_SCHEM")
		.setColumn(2, "PKTABLE_NAME").setColumn(3, "PKCOLUMN_NAME")
		.setColumn(4, "FKTABLE_CAT").setColumn(5, "FKTABLE_SCHEM")
		.setColumn(6, "FKTABLE_NAME").setColumn(7, "FKCOLUMN_NAME")
		.setColumn(8, "KEY_SEQ").setColumn(9, "UPDATE_RULE")
		.setColumn(10, "DELETE_RULE").setColumn(11, "FK_NAME")
		.setColumn(12, "PK_NAME").setColumn(13, "DEFERRABILITY");

		setNull(builder).build(model);
	}

	private void addIndexInfoTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nullableString) // TABLE_CAT
		.addColumnDef(nullableString) // TABLE_SCHEM
		.addColumnDef(nonNullString) // TABLE_NAME
		.addColumnDef(nullableBoolean) // NON_UNIQUE
		.addColumnDef(nullableString) // INDEX_QUALIFIER
		.addColumnDef(nullableString) // INDEX_NAME
		.addColumnDef(nullableShort) // TYPE
		.addColumnDef(nullableShort) // ORDINAL_POSITION
		.addColumnDef(nullableString) // COLUMN_NAME
		.addColumnDef(nullableString) // ASC_OR_DESC
		.addColumnDef(nullableInt) // CARDINALITY
		.addColumnDef(nullableInt) // PAGES
		.addColumnDef(nullableString) // FILTER_CONDITION
		.setSortKey(
				new RdfKey.Builder()
				.addSegment(
						new RdfKeySegment.Builder()
						.setAscending(true).setIdx(3)
						.build(model))
						.addSegment(
								new RdfKeySegment.Builder()
								.setAscending(true).setIdx(6)
								.build(model))
								.addSegment(
										new RdfKeySegment.Builder()
										.setAscending(true).setIdx(5)
										.build(model)).setUnique(false)
										.build(model)).build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.INDEXINFO_TABLE).setSchema(schema)
		.setTableDef(tableDef).setRemarks(MetaCatalogBuilder.REMARK)
		.setColumn(0, "TABLE_CAT").setColumn(1, "TABLE_SCHEM")
		.setColumn(2, "TABLE_NAME").setColumn(3, "NON_UNIQUE")
		.setColumn(4, "INDEX_QUALIFIER").setColumn(5, "INDEX_NAME")
		.setColumn(6, "TYPE").setColumn(7, "ORDINAL_POSITION")
		.setColumn(8, "COLUMN_NAME").setColumn(9, "ASC_OR_DESC")
		.setColumn(10, "CARDINALITY").setColumn(11, "PAGES")
		.setColumn(12, "FILTER_CONDITION");

		setNull(builder).build(model);

	}

	private void addPrimaryKeyTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nullableString) // TABLE_CAT
		.addColumnDef(nullableString) // TABLE_SCHEM
		.addColumnDef(nonNullString) // TABLE_NAME
		.addColumnDef(nonNullString) // COLUMN_NAME
		.addColumnDef(nullableShort) // KEY_SEQ
		.addColumnDef(nullableString) // PK_NAME
		.setSortKey(
				new RdfKey.Builder()
				.addSegment(
						new RdfKeySegment.Builder()
						.setAscending(true).setIdx(4)
						.build(model)).setUnique(false)
						.build(model)).build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.PRIMARY_KEY_TABLE)
		.setSchema(schema).setTableDef(tableDef)
		.setRemarks(MetaCatalogBuilder.REMARK)
		.setColumn(0, "TABLE_CAT").setColumn(1, "TABLE_SCHEM")
		.setColumn(2, "TABLE_NAME").setColumn(3, "COLUMN_NAME")
		.setColumn(4, "KEY_SEQ").setColumn(5, "PK_NAME");

		setNull(builder).build(model);
	}

	private void addProcedureColumnsTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nullableString) // PROCEDURE_CAT
		.addColumnDef(nullableString) // PROCEDURE_SCHEM
		.addColumnDef(nonNullString) // PROCEDURE_NAME
		.addColumnDef(nonNullString) // COLUMN_NAME
		.addColumnDef(nullableShort) // COLUMN_TYPE
		.addColumnDef(nullableInt) // DATA_TYPE
		.addColumnDef(nonNullString) // TYPE_NAME
		.addColumnDef(nullableInt) // PRECISION
		.addColumnDef(nullableInt) // LENGTH
		.addColumnDef(nullableShort) // SCALE
		.addColumnDef(nullableShort) // RADIX
		.addColumnDef(nullableShort) // NULLABLE
		.addColumnDef(nonNullString) // REMARKS
		.addColumnDef(nullableString) // COLUMN_DEF
		.addColumnDef(nullableInt) // SQL_DATA_TYPE
		.addColumnDef(nullableInt) // SQL_DATETIME_SUB
		.addColumnDef(nullableInt) // CHAR_OCTET_LENGTH
		.addColumnDef(nullableInt) // ORDINAL_POSITION
		.addColumnDef(nonNullString) // IS_NULLABLE
		.addColumnDef(nonNullString) // SPECIFIC_NAME
		.setSortKey(
				new RdfKey.Builder()
				.addSegment(
						new RdfKeySegment.Builder()
						.setAscending(true).setIdx(0)
						.build(model))
						.addSegment(
								new RdfKeySegment.Builder()
								.setAscending(true).setIdx(1)
								.build(model))
								.addSegment(
										new RdfKeySegment.Builder()
										.setAscending(true).setIdx(2)
										.build(model))
										.addSegment(
												new RdfKeySegment.Builder()
												.setAscending(true).setIdx(19)
												.build(model)).setUnique(false)
												.build(model)).build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.PROCEDURE_COLUMNS_TABLE)
		.setSchema(schema).setTableDef(tableDef)
		.setRemarks(MetaCatalogBuilder.REMARK)
		.setColumn(0, "PROCEDURE_CAT").setColumn(1, "PROCEDURE_SCHEM")
		.setColumn(2, "PROCEDURE_NAME").setColumn(3, "COLUMN_NAME")
		.setColumn(4, "COLUMN_TYPE").setColumn(5, "DATA_TYPE")
		.setColumn(6, "TYPE_NAME").setColumn(7, "PRECISION")
		.setColumn(8, "LENGTH").setColumn(9, "SCALE")
		.setColumn(10, "RADIX").setColumn(11, "NULLABLE")
		.setColumn(12, "REMARKS").setColumn(13, "COLUMN_DEF")
		.setColumn(14, "SQL_DATA_TYPE")
		.setColumn(15, "SQL_DATETIME_SUB")
		.setColumn(16, "CHAR_OCTET_LENGTH")
		.setColumn(17, "ORDINAL_POSITION").setColumn(18, "IS_NULLABLE")
		.setColumn(19, "SPECIFIC_NAME");

		setNull(builder).build(model);

	}

	private void addProceduresTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nullableString) // PROCEDURE_CAT
		.addColumnDef(nullableString) // PROCEDURE_SCHEM
		.addColumnDef(nonNullString) // PROCEDURE_NAME
		.addColumnDef(nullableString) // FUTURE1
		.addColumnDef(nullableString) // FUTURE2
		.addColumnDef(nullableString) // FUTURE3
		.addColumnDef(nonNullString) // REMARKS
		.addColumnDef(nonNullShort) // PROCEDURE_TYPE
		.addColumnDef(nonNullString) // SPECIFIC_NAME
		.setSortKey(
				new RdfKey.Builder()
				.addSegment(
						new RdfKeySegment.Builder()
						.setAscending(true).setIdx(0)
						.build(model))
						.addSegment(
								new RdfKeySegment.Builder()
								.setAscending(true).setIdx(1)
								.build(model))
								.addSegment(
										new RdfKeySegment.Builder()
										.setAscending(true).setIdx(2)
										.build(model))
										.addSegment(
												new RdfKeySegment.Builder()
												.setAscending(true).setIdx(8)
												.build(model)).setUnique(false)
												.build(model)).build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.PROCEDURES_TABLE).setSchema(schema)
		.setTableDef(tableDef).setRemarks(MetaCatalogBuilder.REMARK)
		.setColumn(0, "PROCEDURE_CAT").setColumn(1, "PROCEDURE_SCHEM")
		.setColumn(2, "PROCEDURE_NAME").setColumn(3, "FUTURE1")
		.setColumn(4, "FUTURE2").setColumn(5, "FUTURE3")
		.setColumn(6, "REMARKS").setColumn(7, "PROCEDURE_TYPE")
		.setColumn(8, "SPECIFIC_NAME");

		setNull(builder).build(model);

	}

	private void addSchemasTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nonNullString) // TABLE_SCHEM
		.addColumnDef(nullableString) // TABLE_CATALOG
		.setSortKey(
				new RdfKey.Builder()
				.addSegment(
						new RdfKeySegment.Builder()
						.setAscending(true).setIdx(1)
						.build(model))
						.addSegment(
								new RdfKeySegment.Builder()
								.setAscending(true).setIdx(0)
								.build(model)).setUnique(false)
								.build(model)).build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.SCHEMAS_TABLE).setSchema(schema)
		.setTableDef(tableDef).setRemarks(MetaCatalogBuilder.REMARK)
		.setColumn(0, "TABLE_SCHEM").setColumn(1, "TABLE_CATALOG");

		final String vCat = "?" + NameUtils.createUUIDName();
		builder.addQuerySegment(
				makeQS("%1$s", RDF.type,
						makeNode(ResourceBuilder.getFQName(RdfSchema.class))))
						.addQuerySegment(
								makeQS(vCat, resourceBuilder.getProperty(
										RdfCatalog.class, "schemas"), "%1$s"));

		// TABLE_SCHEM
		builder.getColumn(0)
		.addQuerySegment(makeQS("%1$s", RDFS.label, "%2$s"))
		.setRemarks(MetaCatalogBuilder.REMARK);

		// TABLE_CAT
		builder.getColumn(1).addQuerySegment(makeQS(vCat, RDFS.label, "%2$s"))
		.setRemarks(MetaCatalogBuilder.REMARK);

		builder.build(model);
	}

	private void addSuperTablesTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nullableString) // TABLE_CAT
		.addColumnDef(nullableString) // TABLE_SCHEM
		.addColumnDef(nonNullString) // TABLE_NAME
		.addColumnDef(nonNullString) // SUPERTABLE_NAME
		.build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.SUPER_TABLES_TABLE)
		.setSchema(schema).setTableDef(tableDef)
		.setRemarks(MetaCatalogBuilder.REMARK)
		.setColumn(0, "TABLE_CAT").setColumn(1, "TABLE_SCHEM")
		.setColumn(2, "TABLE_NAME").setColumn(3, "SUPERTABLE_NAME");

		final String vSchema = "?" + NameUtils.createUUIDName();
		final String vCat = "?" + NameUtils.createUUIDName();
		builder.addQuerySegment(
				makeQS("%1$s", RDF.type,
						makeNode(ResourceBuilder.getFQName(RdfTable.class))))
						.addQuerySegment(
								makeQS(vSchema, resourceBuilder.getProperty(
										RdfSchema.class, "tables"), "%1$s"))
										.addQuerySegment(
												makeQS(vCat, resourceBuilder.getProperty(
														RdfCatalog.class, "schemas"), vSchema));

		// TABLE_CAT
		builder.getColumn(0).addQuerySegment(makeQS(vCat, RDFS.label, "%2$s"))
		.setRemarks(MetaCatalogBuilder.REMARK);
		// TABLE_SCHEM
		builder.getColumn(1)
		.addQuerySegment(makeQS(vSchema, RDFS.label, "%2$s"))
		.setRemarks(MetaCatalogBuilder.REMARK);
		// TABLE_NAME
		builder.getColumn(2)
		.addQuerySegment(makeQS("%1$s", RDFS.label, "%2$s"))
		.setRemarks(MetaCatalogBuilder.REMARK);

		setNull(builder.getColumn(3));
		builder.build(model);

	}

	private void addSuperTypesTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nullableString) // TYPE_CAT
		.addColumnDef(nullableString) // TYPE_SCHEM
		.addColumnDef(nonNullString) // TYPE_NAME
		.addColumnDef(nullableString) // SUPERTYPE_CAT
		.addColumnDef(nullableString) // SUPERTYPE_SCHEM
		.addColumnDef(nonNullString) // SUPERTYPE_NAME
		.build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.SUPER_TYPES_TABLE)
		.setSchema(schema).setTableDef(tableDef)
		.setRemarks(MetaCatalogBuilder.REMARK).setColumn(0, "TYPE_CAT")
		.setColumn(1, "TYPE_SCHEM").setColumn(2, "TYPE_NAME")
		.setColumn(3, "SUPERTYPE_CAT").setColumn(4, "SUPERTYPE_SCHEM")
		.setColumn(5, "SUPERTYPE_NAME");

		setNull(builder).build(model);
	}

	private void addTablePrivilegesTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nullableString) // TABLE_CAT
		.addColumnDef(nullableString) // TABLE_SCHEM
		.addColumnDef(nonNullString) // TABLE_NAME
		.addColumnDef(nullableString) // GRANTOR
		.addColumnDef(nonNullString) // GRANTEE
		.addColumnDef(nonNullString) // PRIVILEGE
		.addColumnDef(nonNullString) // IS_GRANTABLE
		.setSortKey(
				new RdfKey.Builder()
				.addSegment(
						new RdfKeySegment.Builder()
						.setAscending(true).setIdx(0)
						.build(model))
						.addSegment(
								new RdfKeySegment.Builder()
								.setAscending(true).setIdx(1)
								.build(model))
								.addSegment(
										new RdfKeySegment.Builder()
										.setAscending(true).setIdx(2)
										.build(model))
										.addSegment(
												new RdfKeySegment.Builder()
												.setAscending(true).setIdx(5)
												.build(model)).setUnique(false)
												.build(model)).build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.TABLE_PRIVILEGES_TABLE)
		.setSchema(schema).setTableDef(tableDef)
		.setRemarks(MetaCatalogBuilder.REMARK)
		.setColumn(0, "TABLE_CAT").setColumn(1, "TABLE_SCHEM")
		.setColumn(2, "TABLE_NAME").setColumn(3, "GRANTOR")
		.setColumn(4, "GRANTEE").setColumn(5, "PRIVILEGE")
		.setColumn(6, "IS_GRANTABLE");

		setNull(builder).build(model);
	}

	private void addTablesTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nullableString) // TABLE_CAT
		.addColumnDef(nullableString) // TABLE_SCHEM
		.addColumnDef(nonNullString) // TABLE_NAME
		.addColumnDef(nonNullString) // TABLE_TYPE
		.addColumnDef(nonNullString) // REMARKS
		.addColumnDef(nullableString) // TYPE_CAT
		.addColumnDef(nullableString) // TYPE_SCHEM
		.addColumnDef(nullableString) // TYPE_NAME
		.addColumnDef(nullableString) // SELF_REFERENCING_COL_NAME
		.addColumnDef(nullableString) // REF_GENERATION
		.setSortKey(
				new RdfKey.Builder()
				.addSegment(
						new RdfKeySegment.Builder()
						.setAscending(true).setIdx(3)
						.build(model))
						.addSegment(
								new RdfKeySegment.Builder()
								.setAscending(true).setIdx(0)
								.build(model))
								.addSegment(
										new RdfKeySegment.Builder()
										.setAscending(true).setIdx(1)
										.build(model))
										.addSegment(
												new RdfKeySegment.Builder()
												.setAscending(true).setIdx(2)
												.build(model)).setUnique(false)
												.build(model)).build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.TABLES_TABLE).setSchema(schema)
		.setTableDef(tableDef).setRemarks(MetaCatalogBuilder.REMARK)
		.setColumn(0, "TABLE_CAT").setColumn(1, "TABLE_SCHEM")
		.setColumn(2, "TABLE_NAME").setColumn(3, "TABLE_TYPE")
		.setColumn(4, "REMARKS").setColumn(5, "TYPE_CAT")
		.setColumn(6, "TYPE_SCHEM").setColumn(7, "TYPE_NAME")
		.setColumn(8, "SELF_REFERENCING_COL_NAME")
		.setColumn(9, "REF_GENERATION");

		final String vSchema = "?" + NameUtils.createUUIDName();
		final String vCat = "?" + NameUtils.createUUIDName();
		builder.addQuerySegment(
				makeQS("%1$s", RDF.type,
						makeNode(ResourceBuilder.getFQName(RdfTable.class))))
						.addQuerySegment(
								makeQS(vSchema, resourceBuilder.getProperty(
										RdfSchema.class, "tables"), "%1$s"))
										.addQuerySegment(
												makeQS(vCat, resourceBuilder.getProperty(
														RdfCatalog.class, "schemas"), vSchema));

		// TABLE_CAT
		builder.getColumn(0).addQuerySegment(makeQS(vCat, RDFS.label, "%2$s"))
		.setRemarks(MetaCatalogBuilder.REMARK);
		// TABLE_SCHEM
		builder.getColumn(1)
		.addQuerySegment(makeQS(vSchema, RDFS.label, "%2$s"))
		.setRemarks(MetaCatalogBuilder.REMARK);
		// TABLE_NAME
		builder.getColumn(2)
		.addQuerySegment(makeQS("%1$s", RDFS.label, "%2$s"))
		.setRemarks(MetaCatalogBuilder.REMARK);
		// TABLE_TYPE
		builder.getColumn(3)
		.addQuerySegment(
				makeQS("%1$s", resourceBuilder.getProperty(
						RdfTable.class, "type"), "%2$s"))
						.setRemarks(MetaCatalogBuilder.REMARK);
		// REMARKS
		builder.getColumn(4)
		.addQuerySegment(
				makeQS("%1$s", resourceBuilder.getProperty(
						RdfTable.class, "remarks"), "%2$s"))
						.setRemarks(MetaCatalogBuilder.REMARK);
		// TYPE_CAT TYPE_SCHEM TYPE_NAME SELF_REFERENCING_COL_NAME
		// REF_GENERATION
		for (int i = 5; i < 10; i++) {
			setNull(builder.getColumn(i));
		}
		builder.build(model);

	}

	private void addTableTypesTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nonNullString) // TABLE_TYPE
		.setPrimaryKey(
				new RdfKey.Builder()
				.addSegment(
						new RdfKeySegment.Builder()
						.setAscending(true).setIdx(0)
						.build(model)).setUnique(true)
						.build(model)).build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.TABLE_TYPES_TABLE)
		.setSchema(schema)
		.setTableDef(tableDef)
		.setRemarks(MetaCatalogBuilder.REMARK)
		.addQuerySegment(
				makeQS("%1$s", RDF.type, makeNode(ResourceBuilder
						.getFQName(RdfTable.class))))
						.setColumn(0, "TABLE_TYPE");
		builder.getColumn(0)
		.addQuerySegment(
				makeQS("%1$s", resourceBuilder.getProperty(
						RdfTable.class, "type"), "%2$s"))
						.setRemarks(MetaCatalogBuilder.REMARK);
		builder.build(model);
	}

	private void addTypeInfoTableTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nonNullString) // TYPE_NAME
		.addColumnDef(nullableInt) // DATA_TYPE
		.addColumnDef(nullableInt) // PRECISION
		.addColumnDef(nullableString) // LITERAL_PREFIX
		.addColumnDef(nullableString) // LITERAL_SUFFIX
		.addColumnDef(nullableString) // CREATE_PARAMS
		.addColumnDef(nullableShort) // NULLABLE
		.addColumnDef(nonNullBoolean) // CASE_SENSITIVE
		.addColumnDef(nullableShort) // SEARCHABLE
		.addColumnDef(nonNullBoolean) // UNSIGNED_ATTRIBUTE
		.addColumnDef(nullableBoolean) // FIXED_PREC_SCALE
		.addColumnDef(nullableBoolean) // AUTO_INCREMENT
		.addColumnDef(nullableString) // LOCAL_TYPE_NAME
		.addColumnDef(nullableShort) // MINIMUM_SCALE
		.addColumnDef(nullableShort) // MAXIMUM_SCALE
		.addColumnDef(nullableInt) // SQL_DATA_TYPE
		.addColumnDef(nullableInt) // SQL_DATETIME_SUB
		.addColumnDef(nullableInt) // NUM_PREC_RADIX
		.build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.TYPEINFO_TABLE).setSchema(schema)
		.setTableDef(tableDef).setRemarks(MetaCatalogBuilder.REMARK)
		.setColumn(0, "TYPE_NAME").setColumn(1, "DATA_TYPE")
		.setColumn(2, "PRECISION").setColumn(3, "LITERAL_PREFIX")
		.setColumn(4, "LITERAL_SUFFIX").setColumn(5, "CREATE_PARAMS")
		.setColumn(6, "NULLABLE").setColumn(7, "CASE_SENSITIVE")
		.setColumn(8, "SEARCHABLE").setColumn(9, "UNSIGNED_ATTRIBUTE")
		.setColumn(10, "FIXED_PREC_SCALE")
		.setColumn(11, "AUTO_INCREMENT")
		.setColumn(12, "LOCAL_TYPE_NAME")
		.setColumn(13, "MINIMUM_SCALE").setColumn(14, "MAXIMUM_SCALE")
		.setColumn(15, "SQL_DATA_TYPE")
		.setColumn(16, "SQL_DATETIME_SUB")
		.setColumn(17, "NUM_PREC_RADIX");

		setNull(builder).build(model);
	}

	private void addUDTTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nullableString) // TYPE_CAT
		.addColumnDef(nullableString) // TYPE_SCHEM
		.addColumnDef(nonNullString) // TYPE_NAME
		.addColumnDef(nonNullString) // CLASS_NAME
		.addColumnDef(nullableInt) // DATA_TYPE
		.addColumnDef(nonNullString) // REMARKS
		.addColumnDef(nullableShort) // BASE_TYPE
		.setSortKey(
				new RdfKey.Builder()
				.addSegment(
						new RdfKeySegment.Builder()
						.setAscending(true).setIdx(4)
						.build(model))
						.addSegment(
								new RdfKeySegment.Builder()
								.setAscending(true).setIdx(0)
								.build(model))
								.addSegment(
										new RdfKeySegment.Builder()
										.setAscending(true).setIdx(1)
										.build(model))
										.addSegment(
												new RdfKeySegment.Builder()
												.setAscending(true).setIdx(2)
												.build(model)).setUnique(false)
												.build(model)).build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.UDT_TABLES).setSchema(schema)
		.setTableDef(tableDef).setRemarks(MetaCatalogBuilder.REMARK)
		.setColumn(0, "TYPE_CAT").setColumn(1, "TYPE_SCHEM")
		.setColumn(2, "TYPE_NAME").setColumn(3, "CLASS_NAME")
		.setColumn(4, "DATA_TYPE").setColumn(5, "REMARKS")
		.setColumn(6, "BASE_TYPE");

		setNull(builder).build(model);

	}

	private void addVersionColumnsTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nullableShort) // SCOPE
		.addColumnDef(nonNullString) // COLUMN_NAME
		.addColumnDef(nullableInt) // DATA_TYPE
		.addColumnDef(nonNullString) // TYPE_NAME
		.addColumnDef(nullableInt) // COLUMN_SIZE
		.addColumnDef(nullableInt) // BUFFER_LENGTH
		.addColumnDef(nullableShort) // DECIMAL_DIGITS
		.addColumnDef(nullableShort) // PSEUDO_COLUMN
		.build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.VERSION_COLUMNS_TABLE)
		.setSchema(schema).setTableDef(tableDef)
		.setRemarks(MetaCatalogBuilder.REMARK).setColumn(0, "SCOPE")
		.setColumn(1, "COLUMN_NAME").setColumn(2, "DATA_TYPE")
		.setColumn(3, "TYPE_NAME").setColumn(4, "COLUMN_SIZE")
		.setColumn(5, "BUFFER_LENGTH").setColumn(6, "DECIMAL_DIGITS")
		.setColumn(7, "PSEUDO_COLUMN");

		setNull(builder).build(model);
	}

	private void addXrefTable() {
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(nullableString) // PKTABLE_CAT
		.addColumnDef(nullableString) // PKTABLE_SCHEM
		.addColumnDef(nonNullString) // PKTABLE_NAME
		.addColumnDef(nonNullString) // PKCOLUMN_NAME
		.addColumnDef(nullableString) // FKTABLE_CAT
		.addColumnDef(nullableString) // FKTABLE_SCHEM
		.addColumnDef(nonNullString) // FKTABLE_NAME
		.addColumnDef(nonNullString) // FKCOLUMN_NAME
		.addColumnDef(nullableShort) // KEY_SEQ
		.addColumnDef(nullableShort) // UPDATE_RULE
		.addColumnDef(nullableShort) // DELETE_RULE
		.addColumnDef(nullableString) // FK_NAME
		.addColumnDef(nullableString) // PK_NAME
		.addColumnDef(nullableShort) // DEFERRABILITY
		.setSortKey(
				new RdfKey.Builder()
				.addSegment(
						new RdfKeySegment.Builder()
						.setAscending(true).setIdx(4)
						.build(model))
						.addSegment(
								new RdfKeySegment.Builder()
								.setAscending(true).setIdx(5)
								.build(model))
								.addSegment(
										new RdfKeySegment.Builder()
										.setAscending(true).setIdx(6)
										.build(model))
										.addSegment(
												new RdfKeySegment.Builder()
												.setAscending(true).setIdx(7)
												.build(model)).setUnique(false)
												.build(model)).build(model);

		final RdfTable.Builder builder = new RdfTable.Builder()
		.setType(MetaCatalogBuilder.TABLE_TYPE)
		.setName(MetaCatalogBuilder.XREF_TABLE).setSchema(schema)
		.setTableDef(tableDef).setRemarks(MetaCatalogBuilder.REMARK)
		.setColumn(0, "PKTABLE_CAT").setColumn(1, "PKTABLE_SCHEM")
		.setColumn(2, "PKTABLE_NAME").setColumn(3, "PKCOLUMN_NAME")
		.setColumn(4, "FKTABLE_CAT").setColumn(5, "FKTABLE_SCHEM")
		.setColumn(6, "FKTABLE_NAME").setColumn(7, "FKCOLUMN_NAME")
		.setColumn(8, "KEY_SEQ").setColumn(9, "UPDATE_RULE")
		.setColumn(10, "DELETE_RULE").setColumn(11, "FK_NAME")
		.setColumn(12, "PK_NAME").setColumn(13, "DEFERRABILITY");

		setNull(builder).build(model);
	}

	public void build() {
		addAttributesTable();
		addBestRowTable();
		addCatalogsTable();
		addClientInfoTable();
		addColumnsTable();
		addExportedKeysTable();
		addFunctionColumnsTable();
		addFunctionsTable();
		addImportedKeysTable();
		addIndexInfoTable();
		addPrimaryKeyTable();
		addProcedureColumnsTable();
		addProceduresTable();
		addSchemasTable();
		addSuperTablesTable();
		addSuperTypesTable();
		addTablePrivilegesTable();
		addTablesTable();
		addTableTypesTable();
		addColumnPriviligesTable();
		addTypeInfoTableTable();
		addUDTTable();
		addVersionColumnsTable();
		addXrefTable();
	}

	private String makeNode(final String s) {
		return String.format("<%s>", s);
	}

	private String makeQS(final String s, final Property p, final String o) {
		return makeQS(s, p.getURI(), o);
	}

	private String makeQS(final String s, final String p, final String o) {
		return String.format("%s <%s> %s .", s, p, o);
	}

	private RdfColumn.Builder setNull(final RdfColumn.Builder colBuilder) {
		colBuilder.addQuerySegment(
				"%1$s <http://org.xenei.jdbc4sparql/entity/Table#null> %2$s .")
				.setRemarks(MetaCatalogBuilder.REMARK);
		return colBuilder;
	}

	private RdfTable.Builder setNull(final RdfTable.Builder tblBuilder) {
		tblBuilder
		.addQuerySegment("%1$s a <http://org.xenei.jdbc4sparql/entity/Table#null> . ");
		for (int i = 0; i < tblBuilder.getColumnCount(); i++) {
			setNull(tblBuilder.getColumn(i));
		}
		return tblBuilder;
	}

}

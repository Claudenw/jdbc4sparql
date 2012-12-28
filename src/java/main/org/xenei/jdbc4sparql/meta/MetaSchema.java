package org.xenei.jdbc4sparql.meta;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;

public class MetaSchema extends MetaNamespace implements Schema 
{
	public static final String CATALOGS_TABLE="Catalogs";
	public static final String COLUMNS_TABLE="Columns";
	public static final String COLUMN_PRIVILIGES_TABLE = "ColumnPriviliges";
	public static final String EXPORTED_KEYS_TABLE ="ExportedKeys";
	public static final String IMPORTED_KEYS_TABLE = "ImportedKeys";
	public static final String XREF_TABLE ="XrefKeys";
	public static final String TYPEINFO_TABLE ="TypeInfo";
	public static final String INDEXINFO_TABLE="IndexInfo";
	public static final String UDT_TABLES="UDTs";
	public static final String SUPER_TYPES_TABLE="SuperTypes";
	public static final String SUPER_TABLES_TABLE="SuperTables";	
	public static final String ATTRIBUTES_TABLE="Attributes";
	public static final String SCHEMAS_TABLE="Schemas";
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


	private Catalog catalog;
	private Map<String,Table> tables;
	private static Map<String,TableDef> tableDefs;

	static {
		init();
	}
	
	MetaSchema( Catalog catalog )
	{
		this.catalog = catalog;
		this.tables = new HashMap<String,Table>();
	}


	@Override
	public String getLocalName()
	{
		return "Jdbc4Sparql_SCHEMA";
	}

	@Override
	public Catalog getCatalog()
	{
		return catalog;
	}
	
	public void addTableDef(TableDef tableDef)
	{
		tableDefs.put( tableDef.getName(),  tableDef );
	}
	
	public MetaTable newTable( String name )
	{
		TableDef tableDef = tableDefs.get(name);
		if (tableDef == null)
		{
			throw new IllegalArgumentException( name+" is not a table in this schema");
		}
		return new MetaTable( this, tableDef );	
	}
	
	public Set<Table> getTables()
	{
		HashSet<Table> retval = new HashSet<Table>(tables.values());
		for (String tableName : tableDefs.keySet())
		{
			if (!tables.containsKey(tableName))
			{
				retval.add( new MetaTable( this, tableDefs.get(tableName)));
			}
		}
		return retval;
	}
	
	public MetaTable getTable( String name ) {
		MetaTable retval = null;
		if (tables.get(name) == null)
		{
			retval = newTable( name );
			tables.put( name,  retval );
		}
		return retval;
	}

	private static void init()
	{
		tableDefs = new HashMap<String,TableDef>();
		MetaTableDef tableDef;
		
		tableDef = new MetaTableDef( CATALOGS_TABLE );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_CAT" ) );
		tableDef.addKey( "TABLE_CAT");
		tableDef.setUnique();
		tableDefs.put( tableDef.getName(), tableDef);
		
		tableDef = new MetaTableDef( COLUMNS_TABLE );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_CAT" ).setNullable( DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_NAME" ));
		tableDef.add( MetaColumn.getStringInstance( "COLUMN_NAME" ));
		tableDef.add( MetaColumn.getIntInstance( "DATA_TYPE" ));
		tableDef.add( MetaColumn.getStringInstance( "TYPE_NAME" ).setNullable(DatabaseMetaData.columnNullable));
		tableDef.add( MetaColumn.getIntInstance( "COLUMN_SIZE" ));
		tableDef.add( MetaColumn.getIntInstance( "BUFFER_LENGTH" ).setNullable(DatabaseMetaData.columnNullable));
		tableDef.add( MetaColumn.getIntInstance( "DECIMAL_DIGITS" ).setNullable(DatabaseMetaData.columnNullable));
		tableDef.add( MetaColumn.getIntInstance( "NUM_PREC_RADIX" ).setNullable(DatabaseMetaData.columnNullable));
		tableDef.add( MetaColumn.getIntInstance( "NULLABLE" ));
		tableDef.add( MetaColumn.getStringInstance( "REMARKS" ).setNullable(DatabaseMetaData.columnNullable));
		tableDef.add( MetaColumn.getStringInstance( "COLUMN_DEF" ).setNullable(DatabaseMetaData.columnNullable));
		tableDef.add( MetaColumn.getIntInstance( "SQL_DATA_TYPE" ).setNullable(DatabaseMetaData.columnNullable));
		tableDef.add( MetaColumn.getIntInstance( "SQL_DATETIME_SUB" ).setNullable(DatabaseMetaData.columnNullable));
		tableDef.add( MetaColumn.getIntInstance( "CHAR_OCTET_LENGTH" ).setNullable(DatabaseMetaData.columnNullable));
		tableDef.add( MetaColumn.getIntInstance( "ORDINAL_POSITION" ));
		tableDef.add( MetaColumn.getStringInstance( "IS_NULLABLE" ));
		tableDef.add( MetaColumn.getStringInstance( "SCOPE_CATLOG" ).setNullable(DatabaseMetaData.columnNullable));
		tableDef.add( MetaColumn.getStringInstance( "SCOPE_SCHEMA" ).setNullable(DatabaseMetaData.columnNullable));
		tableDef.add( new MetaColumn( "SOURCE_DATA_TYPE", Types.SMALLINT ).setNullable(DatabaseMetaData.columnNullable));
		tableDef.add( MetaColumn.getStringInstance( "IS_AUTOINCREMENT" ));
		tableDef.addKey( "TABLE_CAT");
		tableDef.addKey( "TABLE_SCHEM");
		tableDef.addKey( "TABLE_NAME");
		tableDef.addKey( "ORDINAL_POSITION");
		tableDefs.put( tableDef.getName(), tableDef);
		
		tableDef = new MetaTableDef( SCHEMAS_TABLE );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_SCHEM" ) );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_CATALOG" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.addKey( "TABLE_CATALOG");
		tableDef.addKey( "TABLE_SCHEM");
		tableDefs.put( tableDef.getName(), tableDef);

		tableDef = new MetaTableDef( CLIENT_INFO_TABLE );
		tableDef.add( MetaColumn.getStringInstance( "NAME" ) );
		tableDef.add( MetaColumn.getIntInstance( "MAX_LEN" ) );
		tableDef.add( MetaColumn.getStringInstance( "DEFAULT_VALUE" ) );
		tableDef.add( MetaColumn.getStringInstance( "DESCRIPTION" ) );
		tableDef.addKey( "NAME");
		tableDefs.put( tableDef.getName(), tableDef);
		
		tableDef = new MetaTableDef( FUNCTIONS_TABLE );
		tableDef.add( MetaColumn.getStringInstance( "FUNCTION_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "FUNCTION_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "FUNCTION_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "REMARKS" ) );
		tableDef.add( new MetaColumn( "FUNCTION_TYPE", Types.SMALLINT ));
		tableDef.add( MetaColumn.getStringInstance( "SPECIFIC_NAME" ) );
		tableDef.addKey( "FUNCTION_CAT");
		tableDef.addKey( "FUNCTION_SCHEM");
		tableDef.addKey( "FUNCTION_NAME");
		tableDef.addKey( "SPECIFIC_NAME");
		tableDefs.put( tableDef.getName(), tableDef);
		
		tableDef = new MetaTableDef( TABLE_PRIVILEGES_TABLE );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "GRANTOR" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "GRANTEE" ) );
		tableDef.add( MetaColumn.getStringInstance( "PRIVILEGE" ) );
		tableDef.add( MetaColumn.getStringInstance( "IS_GRANTABLE" ) );
		tableDef.addKey( "TABLE_CAT");
		tableDef.addKey( "TABLE_SCHEM");
		tableDef.addKey( "TABLE_NAME");
		tableDef.addKey( "PRIVILEGE");
		tableDefs.put( tableDef.getName(), tableDef);
		
		tableDef = new MetaTableDef( BEST_ROW_TABLE );
		tableDef.add( MetaColumn.getIntInstance( "SCOPE" ) );
		tableDef.add( MetaColumn.getStringInstance( "COLUMN_NAME" ) );
		tableDef.add( MetaColumn.getIntInstance( "DATA_TYPE" ) );
		tableDef.add( MetaColumn.getStringInstance( "TYPE_NAME" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getIntInstance( "COLUMN_SIZE" ) );
		tableDef.add( MetaColumn.getIntInstance( "BUFFER_LENGTH" ) );
		tableDef.add( MetaColumn.getIntInstance( "DECIMAL_DIGITS" ) );
		tableDef.add( MetaColumn.getIntInstance( "PSEUDO_COLUMN" ) );
		tableDef.addKey( "SCOPE");
		tableDefs.put( tableDef.getName(), tableDef);

		tableDef = new MetaTableDef( PROCEDURES_TABLE );		
		tableDef.add( MetaColumn.getStringInstance( "PROCEDURE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "PROCEDURE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "PROCEDURE_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "FUTURE1" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "FUTURE2" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "FUTURE3" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "REMARKS" ) );
		tableDef.add( new MetaColumn( "PROCEDURE_TYPE", Types.SMALLINT ) );
		tableDef.add( MetaColumn.getStringInstance( "SPECIFIC_NAME" ) );
		tableDef.addKey( "PROCEDURE_CAT");
		tableDef.addKey( "PROCEDURE_SCHEM");
		tableDef.addKey( "PROCEDURE_NAME");
		tableDef.addKey( "SPECIFIC_NAME");
		tableDefs.put( tableDef.getName(), tableDef);

		tableDef = new MetaTableDef( PROCEDURE_COLUMNS_TABLE );		
		tableDef.add( MetaColumn.getStringInstance( "PROCEDURE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "PROCEDURE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "PROCEDURE_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "COLUMN_NAME" ) );
		tableDef.add( new MetaColumn( "COLUMN_TYPE", Types.SMALLINT ) );
		tableDef.add( MetaColumn.getIntInstance( "DATA_TYPE" ) );
		tableDef.add( MetaColumn.getStringInstance( "TYPE_NAME" ) );
		tableDef.add( MetaColumn.getIntInstance( "PRECISION" ) );
		tableDef.add( MetaColumn.getIntInstance( "LENGTH" ) );
		tableDef.add( new MetaColumn( "SCALE", Types.SMALLINT ).setNullable(DatabaseMetaData.columnNullable)  );
		tableDef.add( new MetaColumn( "RADIX", Types.SMALLINT ) );
		tableDef.add( new MetaColumn( "NULLABLE", Types.SMALLINT ) );
		tableDef.add( MetaColumn.getStringInstance( "REMARKS" ) );
		tableDef.add( MetaColumn.getStringInstance( "COLUMN_DEF" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getIntInstance( "SQL_DATA_TYPE" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getIntInstance( "SQL_DATETYPE_SUB" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getIntInstance( "CHAR_OCTET_LENGTH" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getIntInstance( "ORDINAL_POSITION" ) );
		tableDef.add( MetaColumn.getStringInstance( "IS_NULLABLE" ) );
		tableDef.add( MetaColumn.getStringInstance( "SPECIFIC_NAME" ) );
		tableDef.addKey( "PROCEDURE_CAT");
		tableDef.addKey( "PROCEDURE_SCHEM");
		tableDef.addKey( "PROCEDURE_NAME");
		tableDef.addKey( "SPECIFIC_NAME");
		tableDefs.put( tableDef.getName(), tableDef);
		
		tableDef = new MetaTableDef( TABLES_TABLE );		
		tableDef.add( MetaColumn.getStringInstance( "TABLE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_TYPE" ) );
		tableDef.add( MetaColumn.getStringInstance( "REMARKS" ) );
		tableDef.add( MetaColumn.getStringInstance( "TYPE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TYPE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TYPE_NAME" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "SELF_REFERENCING_COL_NAME" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "REF_GENERATION" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.addKey( "TABLE_TYPE");
		tableDef.addKey( "TABLE_CAT");
		tableDef.addKey( "TABLE_SCHEM");
		tableDef.addKey( "TABLE_NAME");
		tableDefs.put( tableDef.getName(), tableDef);
		
		tableDef = new MetaTableDef( TABLE_TYPES_TABLE );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_TYPE" ) );
		tableDef.addKey( "TABLE_TYPE" );
		tableDef.setUnique();
		tableDefs.put( tableDef.getName(), tableDef);
		
		tableDef = new MetaTableDef( COLUMN_PRIVILIGES_TABLE );		
		tableDef.add( MetaColumn.getStringInstance( "TABLE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "COLUMN_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "GRANTOR" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "GRANTEE" ) );
		tableDef.add( MetaColumn.getStringInstance( "PRIVILEGE" ));
		tableDef.add( MetaColumn.getStringInstance( "IS_GRANTABLE" ));
		tableDef.addKey( "COLUMN_NAME");
		tableDef.addKey( "PRIVILEGE");
		tableDefs.put( tableDef.getName(), tableDef);

		tableDef = new MetaTableDef( VERSION_COLUMNS_TABLE );
		tableDef.add( new MetaColumn( "SCOPE", Types.SMALLINT ));
		tableDef.add( MetaColumn.getStringInstance( "COLUMN_NAME" ));
		tableDef.add( MetaColumn.getIntInstance( "DATA_TYPE" ) );
		tableDef.add( MetaColumn.getStringInstance( "TYPE_NAME" ) );
		tableDef.add( MetaColumn.getIntInstance( "COLUMN_SIZE" ) );
		tableDef.add( MetaColumn.getIntInstance( "BUFFER_LENGTH" ) );
		tableDef.add( new MetaColumn( "DECIMAL_DIGITS", Types.SMALLINT ));
		tableDef.add( new MetaColumn( "PSEUDO_COLUMN", Types.SMALLINT ));
		tableDefs.put( tableDef.getName(), tableDef);
		
		tableDef = new MetaTableDef( PRIMARY_KEY_TABLE );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "COLUMN_NAME" ) );
		tableDef.add( new MetaColumn( "KEY_SEQ", Types.SMALLINT ));
		tableDef.add( MetaColumn.getStringInstance( "PK_NAME" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.addKey( "COLUMN_NAME");
		tableDefs.put( tableDef.getName(), tableDef);
		
		tableDef = new MetaTableDef( IMPORTED_KEYS_TABLE );
		tableDef.add( MetaColumn.getStringInstance( "PKTABLE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "PKTABLE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "PKTABLE_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "PKCOLUMN_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "FKTABLE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "FKTABLE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "FKTABLE_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "FKCOLUMN_NAME" ) );
		tableDef.add( new MetaColumn( "KEY_SEQ", Types.SMALLINT ));
		tableDef.add( new MetaColumn( "UPDATE_RULE", Types.SMALLINT ));
		tableDef.add( new MetaColumn( "DELETE_RULE", Types.SMALLINT ));
		tableDef.add( MetaColumn.getStringInstance( "FK_NAME" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "PK_NAME" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( new MetaColumn( "DEFERRABILITY", Types.SMALLINT ));
		tableDef.addKey( "PKTABLE_CAT");
		tableDef.addKey( "PKTABLE_SCHEM");
		tableDef.addKey( "PKTABLE_NAME");
		tableDef.addKey( "KEY_SEQ");
		tableDefs.put( tableDef.getName(), tableDef);
		
		tableDef = new MetaTableDef( EXPORTED_KEYS_TABLE );
		tableDef.add( MetaColumn.getStringInstance( "PKTABLE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "PKTABLE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "PKTABLE_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "PKCOLUMN_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "FKTABLE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "FKTABLE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "FKTABLE_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "FKCOLUMN_NAME" ) );
		tableDef.add( new MetaColumn( "KEY_SEQ", Types.SMALLINT ));
		tableDef.add( new MetaColumn( "UPDATE_RULE", Types.SMALLINT ));
		tableDef.add( new MetaColumn( "DELETE_RULE", Types.SMALLINT ));
		tableDef.add( MetaColumn.getStringInstance( "FK_NAME" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "PK_NAME" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( new MetaColumn( "DEFERRABILITY", Types.SMALLINT ));
		tableDef.addKey( "FKTABLE_CAT");
		tableDef.addKey( "FKTABLE_SCHEM");
		tableDef.addKey( "FKTABLE_NAME");
		tableDef.addKey( "KEY_SEQ");
		tableDefs.put( tableDef.getName(), tableDef);
		
		tableDef = new MetaTableDef( XREF_TABLE );
		tableDef.add( MetaColumn.getStringInstance( "PKTABLE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "PKTABLE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "PKTABLE_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "PKCOLUMN_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "FKTABLE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "FKTABLE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "FKTABLE_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "FKCOLUMN_NAME" ) );
		tableDef.add( new MetaColumn( "KEY_SEQ", Types.SMALLINT ));
		tableDef.add( new MetaColumn( "UPDATE_RULE", Types.SMALLINT ));
		tableDef.add( new MetaColumn( "DELETE_RULE", Types.SMALLINT ));
		tableDef.add( MetaColumn.getStringInstance( "FK_NAME" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "PK_NAME" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( new MetaColumn( "DEFERRABILITY", Types.SMALLINT ));
		tableDef.addKey( "FKTABLE_CAT");
		tableDef.addKey( "FKTABLE_SCHEM");
		tableDef.addKey( "FKTABLE_NAME");
		tableDef.addKey( "KEY_SEQ");
		tableDefs.put( tableDef.getName(), tableDef);
		
		tableDef = new MetaTableDef( TYPEINFO_TABLE );
		tableDef.add( MetaColumn.getStringInstance( "TYPE_NAME" ) );
		tableDef.add( MetaColumn.getIntInstance( "DATA_TYPE" ) );
		tableDef.add( MetaColumn.getIntInstance( "PRECISION" ) );
		tableDef.add( MetaColumn.getStringInstance( "LITERAL_PREFIX" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "LITERAL_SUFFIX" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "CREATE_PARAMS" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( new MetaColumn( "NULLABLE", Types.SMALLINT ));
		tableDef.add( new MetaColumn( "CASE_SENSITIVE", Types.BOOLEAN ));
		tableDef.add( new MetaColumn( "SEARCHABLE", Types.SMALLINT ));
		tableDef.add( new MetaColumn( "UNSIGNED_ATTRIBUTE", Types.BOOLEAN ));
		tableDef.add( new MetaColumn( "FIXED_PREC_SCALE", Types.BOOLEAN ));
		tableDef.add( new MetaColumn( "AUTO_INCREMENT", Types.BOOLEAN ));
		tableDef.add( MetaColumn.getStringInstance( "LOCAL_TYPE_NAME" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( new MetaColumn( "MINIMUM_SCALE", Types.SMALLINT ));
		tableDef.add( new MetaColumn( "MAXIMUM_SCALE", Types.SMALLINT ));
		tableDef.add( MetaColumn.getIntInstance( "SQL_DATA_TYPE" ) );
		tableDef.add( MetaColumn.getIntInstance( "SQL_DATETIME_SUB" ) );
		tableDef.add( MetaColumn.getIntInstance( "NUM_PREC_RADIX" ) );
		tableDefs.put( tableDef.getName(), tableDef);
		
		tableDef = new MetaTableDef( INDEXINFO_TABLE );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_NAME" ) );
		tableDef.add( new MetaColumn( "NON_UNIQUE", Types.BOOLEAN ));
		tableDef.add( MetaColumn.getStringInstance( "INDEX_QUALIFIER" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "INDEX_NAME" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( new MetaColumn( "TYPE", Types.SMALLINT ));
		tableDef.add( new MetaColumn( "ORDINAL_POSITION", Types.SMALLINT ));
		tableDef.add( MetaColumn.getStringInstance( "COLUMN_NAME" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "ASC_OR_DESC" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getIntInstance( "CARDINALITY" ) );
		tableDef.add( MetaColumn.getIntInstance( "PAGES" ) );
		tableDef.add( MetaColumn.getStringInstance( "FILTER_CONDITION" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.addKey( "NON_UNIQUE");
		tableDef.addKey( "TYPE");
		tableDef.addKey( "INDEX_NAME");
		tableDefs.put( tableDef.getName(), tableDef );	
		
		tableDef = new MetaTableDef( UDT_TABLES );
		tableDef.add( MetaColumn.getStringInstance( "TYPE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TYPE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TYPE_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "CLASS_NAME" ) );
		tableDef.add( MetaColumn.getIntInstance( "DATA_TYPE" ) );
		tableDef.add( MetaColumn.getStringInstance( "REMARKS" ) );
		tableDef.add( new MetaColumn( "BASE_TYPE", Types.SMALLINT ).setNullable(DatabaseMetaData.columnNullable));
		tableDef.addKey( "DATA_TYPE");
		tableDef.addKey( "TYPE_CAT");
		tableDef.addKey( "TYPE_SCHEM");
		tableDef.addKey( "TYPE_NAME");
		tableDefs.put( tableDef.getName(), tableDef );
	
		tableDef = new MetaTableDef( SUPER_TYPES_TABLE );
		tableDef.add( MetaColumn.getStringInstance( "TYPE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TYPE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TYPE_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "SUPERTYPE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "SUPERTYPE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "SUPERTYPE_NAME" ) );
		tableDefs.put( tableDef.getName(), tableDef );
		
		tableDef = new MetaTableDef( SUPER_TABLES_TABLE );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TABLE_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "SUPERTABLE_NAME" ) );
		tableDefs.put( tableDef.getName(), tableDef );
	
		tableDef = new MetaTableDef( ATTRIBUTES_TABLE );
		tableDef.add( MetaColumn.getStringInstance( "TYPE_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TYPE_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "TYPE_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "ATTR_NAME" ) );
		tableDef.add( MetaColumn.getIntInstance( "DATA_TYPE" ) );
		tableDef.add( MetaColumn.getStringInstance( "ATTR_TYPE_NAME" ) );
		tableDef.add( MetaColumn.getIntInstance( "ATTR_SIZE" ) );
		tableDef.add( MetaColumn.getIntInstance( "DECIMAL_DIGITS" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getIntInstance( "NUM_PREC_RADIX" ) );
		tableDef.add( MetaColumn.getIntInstance( "NULLABLE" ) );
		tableDef.add( MetaColumn.getStringInstance( "REMARKS" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "ATTR_DEF" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getIntInstance( "SQL_DATA_TYPE" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getIntInstance( "SQL_DATETIME_SUB" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getIntInstance( "CHAR_OCTET_LENGTH" ) );
		tableDef.add( MetaColumn.getIntInstance( "ORDINAL_POSITION" ) );
		tableDef.add( MetaColumn.getStringInstance( "IS_NULLABLE" ) );
		tableDef.add( MetaColumn.getStringInstance( "SCOPE_CATALOG" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "SCOPE_SCHEMA" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "SCOPE_TABLE" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( new MetaColumn( "SOURCE_DATA_TYPE", Types.SMALLINT ).setNullable(DatabaseMetaData.columnNullable) );
		tableDefs.put( tableDef.getName(), tableDef );
			
		tableDef = new MetaTableDef( FUNCTION_COLUMNS_TABLE );
		tableDef.add( MetaColumn.getStringInstance( "FUNCTION_CAT" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "FUNCTION_SCHEM" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getStringInstance( "FUNCTION_NAME" ) );
		tableDef.add( MetaColumn.getStringInstance( "COLUMN_NAME" ) );;
		tableDef.add( new MetaColumn( "COLUMN_TYPE", Types.SMALLINT ));
		tableDef.add( MetaColumn.getIntInstance( "DATA_TYPE" ) );
		tableDef.add( MetaColumn.getStringInstance( "TYPE_NAME" ) );;
		tableDef.add( MetaColumn.getIntInstance( "PRECISION" ) );
		tableDef.add( MetaColumn.getIntInstance( "LENGTH" ) );
		tableDef.add( new MetaColumn( "SCALE", Types.SMALLINT ));
		tableDef.add( new MetaColumn( "RADIX", Types.SMALLINT ));
		tableDef.add( new MetaColumn( "NULLABLE", Types.SMALLINT ));
		tableDef.add( MetaColumn.getStringInstance( "REMARKS" ).setNullable(DatabaseMetaData.columnNullable) );
		tableDef.add( MetaColumn.getIntInstance( "ORDINAL_POSITION" ) );
		tableDef.add( MetaColumn.getStringInstance( "IS_NULLABLE" ) );
		tableDef.add( MetaColumn.getStringInstance( "SPECIFIC_NAME" ) );
		tableDef.addKey( "FUNCTION_CAT");
		tableDef.addKey( "FUNCTION_SCHEM");
		tableDef.addKey( "FUNCTION_NAME");
		tableDef.addKey( "SPECIFIC_NAME");
		tableDefs.put( tableDef.getName(), tableDef);
	}
}

package org.xenei.jdbc4sparql.impl.virtual;

import java.util.ArrayList;
import java.util.List;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.NameSegments;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.impl.AbstractTable;

/**
 * The Virtual table for virtual columns.
 * 
 *
 */
public class VirtualTable extends AbstractTable {
	/**
	 * The name for the SYSTEM table.
	 */
	public static final String SYSTEM_TABLE = "system";
	/**
	 * The default name for this table.
	 */
	public static final String NAME = "";
	private final Schema schema;
	private final TableName tableName;
	private final List<Column> columns;
	private TableDef tableDef;
	
	/**
	 * @return the default virtual table name.
	 */
	public static TableName getDefaultName() {
		return VirtualSchema.getDefaultName().getTableName(NAME);
	}

	/**
	 * @return the default system table name.
	 */
	public static TableName getDefaultSystemName() {
		return VirtualSchema.getDefaultName().getTableName(SYSTEM_TABLE);
	}
	
	/**
	 * Constructor.
	 * @param schema THe schema to build the table in.
	 */
	public VirtualTable(final Schema schema) {
		this(schema, NAME);
	}

	/**
	 * Constructor.
	 * @param schema THe schema to build the table in.
	 * @param name the name for the table.
	 */
	public VirtualTable(final Schema schema, final String name) {
		this.tableName = schema.getName().getTableName(name);
		this.columns = new ArrayList<Column>();
		this.schema = schema;
	}

	@Override
	public void delete() {
		// does nothing
	}

	@Override
	public List<Column> getColumnList() {
		return columns;
	}

	@Override
	public TableName getName() {
		return tableName;
	}

	@Override
	public String getQuerySegmentFmt() {
		return null;
	}

	@Override
	public String getRemarks() {
		return "";
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public Table getSuperTable() {
		return null;
	}

	@Override
	public TableDef getTableDef() {
		return tableDef;
	}

	@Override
	public String getType() {
		return "Virtual";
	}

	@Override
	public boolean hasQuerySegments() {
		return false;
	}

	@Override
	public NameSegments getColumnSegments() {
		return NameSegments.FFFT;
	}

}

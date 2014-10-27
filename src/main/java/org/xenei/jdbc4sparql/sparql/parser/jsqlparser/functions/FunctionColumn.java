package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import net.sf.jsqlparser.expression.Function;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.ColumnName;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableName;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.sparql.QueryColumnInfo;
import org.xenei.jdbc4sparql.utils.SQLNameUtil;

import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.ExprVar;

public class FunctionColumn implements Column
{
	private Table<?> table;
	private ColumnName name;
	private ColumnDef cd;
	
	public FunctionColumn( final Table<?> table, String name, final int type )
	{
		this.table = table;
		this.name = table.getName().getColumnName( name );
		this.cd = new FunctionColumnDef( type );
	}

	@Override
	public ColumnName getName() {
		return name;
	}

	@Override
	public Catalog getCatalog() {
		return table.getCatalog();
	}

	@Override
	public ColumnDef getColumnDef() {
		return cd;
	}

	@Override
	public String getRemarks() {
		return "Function Column";
	}

	@Override
	public Schema getSchema() {
		return table.getSchema();
	}

	@Override
	public String getSPARQLName() {
		return NameUtils.getSPARQLName(this);
	}

	@Override
	public String getSQLName() {
		return NameUtils.getDBName(this);
	}

	@Override
	public Table getTable() {
		return table;
	}

	@Override
	public String getQuerySegmentFmt() {
		return null;
	}

	@Override
	public boolean hasQuerySegments() {
		return false;
	}

	@Override
	public boolean isOptional() {
		return false;
	}
}
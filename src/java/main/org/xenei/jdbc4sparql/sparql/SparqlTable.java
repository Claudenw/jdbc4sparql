package org.xenei.jdbc4sparql.sparql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.SortKey;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.AbstractTable;
import org.xenei.jdbc4sparql.impl.ColumnImpl;
import org.xenei.jdbc4sparql.impl.NamespaceImpl;

public class SparqlTable extends AbstractTable
{
	protected SparqlTable( SparqlSchema schema, SparqlTableDef tableDef )
	{
		super(schema, tableDef);
	}

	@Override
	public String getType()
	{
		return "SPARQL TABLE";
	}


	@Override
	public ResultSet getResultSet() throws SQLException
	{
		return new SparqlResultSet(this);
	}
	
	@Override
	public SparqlTableDef getTableDef()
	{
		return (SparqlTableDef) super.getTableDef();
	}

	@Override
	public SparqlSchema getSchema()
	{
		return (SparqlSchema) super.getSchema();
	}

	@Override
	public SparqlCatalog getCatalog()
	{
		return (SparqlCatalog) super.getCatalog();
	}

}

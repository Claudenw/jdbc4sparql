package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.UUID;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.AbstractTable;
import org.xenei.jdbc4sparql.impl.SchemaImpl;

public class SparqlView extends SparqlTable
{
	public static final String NAME_SPACE="http://org.xenei.jdbc4sparql/vocab#View";
	
	SparqlQueryBuilder builder;
	public SparqlView(SparqlQueryBuilder builder)
	{
		super( builder.getCatalog().getViewSchema(),
				builder.getTableDef( UUID.randomUUID().toString() ));
		this.builder = builder; 

	}
	
	@Override
	public ResultSet getResultSet() throws SQLException
	{
		return new SparqlResultSet(this);
	}


}

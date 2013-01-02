package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Resource;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.SortKey;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.iface.TypeConverter;
import org.xenei.jdbc4sparql.impl.TableDefImpl;
import org.xenei.jdbc4sparql.meta.MetaColumn;

public class SparqlTableDef extends TableDefImpl
{
	
			
	private Query query;
	
	SparqlTableDef(String name, Query query)
	{
		super( name );
		this.query = query;
	}
	
	Query getQuery()
	{
		return query;
	}
	
}

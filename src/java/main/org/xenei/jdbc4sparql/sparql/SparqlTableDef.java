package org.xenei.jdbc4sparql.sparql;

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
import org.xenei.jdbc4sparql.meta.MetaColumn;

public class SparqlTableDef implements TableDef
{
	private static final String columnInfo="prefix afn: <http://jena.hpl.hp.com/ARQ/function#> . " +
			"SELECT DISTINCT ?col " +
			"WHERE { " +
			"[] rdfs:domain <%s> ; " +
			"<?col> [] ;" +
			" FILTER( afn:namespace(?col) == '%s') }";	
			
	private Resource resource;
	private List<ColumnDef> columns;
	private SparqlSchema schema;

	SparqlTableDef( SparqlSchema schema, Resource resource)
	{
		this.resource = resource;
		this.columns = null;
		this.schema = schema;
	}
	
	@Override
	public String getName()
	{
		return resource.getLocalName();
	}

	@Override
	public List<ColumnDef> getColumnDefs()
	{
		if (columns == null)
		{
			createColumns();
		}
		return columns;
	}

	@Override
	public ColumnDef getColumnDef( int idx )
	{
		List<ColumnDef> cols = getColumnDefs();
		return cols.get(idx);
	}

	@Override
	public ColumnDef getColumnDef( String name )
	{
		for (ColumnDef c : getColumnDefs())
		{
			if (c.getLabel().equals( name ))
			{
				return c;
			}
		}
		throw new IllegalArgumentException( String.format("Column %s can not be found", name ));
	}

	@Override
	public int getColumnCount()
	{
		return getColumnDefs().size();
	}

	@Override
	public SortKey getSortKey()
	{
		return null;
	}

	@Override
	public void verify( Object[] row )
	{
		if (row.length != columns.size())
		{
			throw new IllegalArgumentException( String.format( "Expected %s columns but got %s", columns.size(), row.length ));
		}
		for (int i=0;i<row.length;i++)
		{
			ColumnDef c = columns.get(i);
			
			if (row[i] == null)
			{
				if (c.getNullable() == DatabaseMetaData.columnNoNulls)
				{
					throw new IllegalArgumentException( String.format( "Column %s may not be null", c.getLabel()));
				}
			}
			else
			{
				Class<?> clazz = TypeConverter.getJavaType( c.getType() );
				if (! clazz.isAssignableFrom( row[i].getClass() )) 
				{
					throw new IllegalArgumentException( String.format( "Column %s can not recieve values of class %s", c.getLabel(), row[i].getClass()));
				}
			}
		}
		
	}
	@Override
	public int getColumnIndex( ColumnDef column )
	{
		return getColumnIndex( column.getLabel());
	}

	@Override
	public int getColumnIndex( String columnName )
	{
		List<ColumnDef> cols = getColumnDefs();
		for (int i=0;i<cols.size();i++)
		{
			if (cols.get(i).getLabel().equals(columnName))
			{
				return i;
			}
		}
		throw new IllegalArgumentException( String.format("Column %s can not be found", columnName ));

	}

	private synchronized void createColumns()
	{
		if (columns != null)
		{
			return;		// just in case it was created while we waited.
		}
		
		columns = new ArrayList<ColumnDef>();
		List<QuerySolution> solns = schema.getCatalog().executeQuery( String.format( columnInfo, getName(), resource.getNameSpace() ));
		for (QuerySolution sol : solns)
		{
			columns.add( new SparqlColumn( this, sol.getResource( "col")));
		}
	}
}

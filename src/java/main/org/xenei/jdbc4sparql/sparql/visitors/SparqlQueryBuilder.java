package org.xenei.jdbc4sparql.sparql.visitors;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeVisitor;
import com.hp.hpl.jena.graph.Node_ANY;
import com.hp.hpl.jena.graph.Node_Blank;
import com.hp.hpl.jena.graph.Node_Literal;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Node_Variable;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.impl.LiteralLabel;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;
import com.hp.hpl.jena.vocabulary.RDF;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.TableDefImpl;

public class SparqlQueryBuilder
{
	private Query query;
	private Catalog catalog;
	private Set<Table> tablesInQuery;
	private Map<Var,Column> columnsInQuery;

	public SparqlQueryBuilder( Catalog catalog )
	{
		this.catalog = catalog;
		this.query = new Query();
		this.tablesInQuery = new HashSet<Table>();
		this.columnsInQuery = new HashMap<Var, Column>();
		query.setQuerySelectType();	
	}
	
	public Catalog getCatalog()
	{
		return catalog;
	}
	
	public void setDistinct()
	{
		query.setDistinct(true);
	}
	
	public void setAllColumns()
	{
		query.setQueryResultStar(true);
		for (Table t : tablesInQuery)
		{
			addVars(t.getColumns());
		}
	}
	
	public boolean isAllColumns()
	{
		return query.isQueryResultStar();
	}
	
	public void addVar( String var )
	{
		query.addResultVar(var);
	}
	
	public void addOrderBy( Expr expr, boolean ascending )
	{
		query.addOrderBy(expr, ascending?Query.ORDER_ASCENDING:Query.ORDER_DESCENDING);
	}
	
	public void setLimit( Long limit )
	{
		query.setLimit(limit);
	}
	
	public void setOffset( Long offset)
	{
		query.setOffset(offset);
	}
	
	private ElementGroup getElementGroup()
	{
		ElementGroup retval;
		Element e = query.getQueryPattern();
		if (e == null)
		{
			retval = new ElementGroup();
			query.setQueryPattern(retval);
		}
		else if (e instanceof ElementGroup)
		{
			retval = (ElementGroup) e;
		}
		else
		{
			retval = new ElementGroup();
			retval.addElement( e );
		}
		return retval;
	}
	
	public void addBGP( Node s, Node p, Node o )
	{
		ElementGroup eg = getElementGroup();
		Triple t = new Triple( s, p, o );
		for (Element el : eg.getElements())
		{
			if (el instanceof ElementTriplesBlock)
			{
				if (((ElementTriplesBlock)el).getPattern().getList().contains(t))
				{
					return;
				}
			}
		}
		eg.addTriplePattern(t);
	}
	
	public void addFilter( Expr filter)
	{
		ElementFilter el = new ElementFilter(filter);
		getElementGroup().addElementFilter( el );
	}
	
	private String getTableVar( Table table )
	{
		return String.format( "TABLE_%s_%s_%s", table.getCatalog().getLocalName(),
				table.getSchema().getLocalName(), table.getLocalName() );
	}

	/**
	 * Returns the variable for the table.
	 * @param schemaName The schema name to find (null = any)
	 * @param tableName The table name to find (null = any)
	 * @return
	 * @throws SQLException 
	 */
	public Node addTable( String schemaName, String tableName ) throws SQLException
	{
		Collection<Table> tables = findTables(schemaName, tableName);
		if (tables.size() > 1)
		{
			throw new SQLException( tableName+" is found in multiple schemas");
		}
		if (tables.isEmpty())
		{
			throw new SQLException( tableName+" is not found in any schema");
		}
		Table table = tables.iterator().next();
		tablesInQuery.add( table );
		Node tableVar = Node.createVariable( getTableVar( table ));
		addBGP( tableVar, RDF.type.asNode(), Node.createURI(table.getFQName()));
		return tableVar;
	}
	
	public Node addColumn(net.sf.jsqlparser.schema.Column tableColumn)
	{
		/*
		 * Add bgp triples
		 */
		
		Node tableVar;
		Node columnURI;
		Column column;
		try
		{
			tableVar = addTable( tableColumn.getTable().getSchemaName(), tableColumn.getTable().getName() );
			
			
			Collection<Column> columns = findColumns( tableColumn.getTable().getSchemaName(), 
					tableColumn.getTable().getName(), tableColumn.getColumnName() );
			if (columns.size() > 1)
			{
				throw new SQLException( tableColumn.getColumnName()+" is found in multiple tables");
			}
			if (columns.isEmpty())
			{
				throw new SQLException( tableColumn.getColumnName()+" is not found in any table");
			}
			column = columns.iterator().next();
			columnURI = Node.createURI( column.getFQName() );
		}
		catch (SQLException e)
		{
			throw new RuntimeException ( e );
		} 
		Node columnVar = Node.createVariable( tableColumn.getColumnName() );
		columnsInQuery.put( Var.alloc(columnVar), column);
		// ?tableVar columnURI ?columnVar
		addBGP( tableVar, columnURI, columnVar );
		return columnVar;
	}

	public Query build()
	{
		return query;
	}
	
	public String toString()
	{
		return "QueryBuilder["+query.toString()+"]";
	}

	public void addVars( Iterator<? extends Column> cols )
	{
		ElementGroup eg = getElementGroup();
		while (cols.hasNext())
		{
			Column col = cols.next();
			Element e = new ElementTriplesBlock();
			Node s = Node.createVariable( getTableVar( col.getTable()));
			Node p = Node.createURI( col.getFQName() );
			Node o = Node.createVariable( col.getLocalName());
			((ElementTriplesBlock)e).addTriple(new Triple( s, p, o ));
			if (col.getNullable() == DatabaseMetaData.columnNullable )
			{
				e = new ElementOptional( e );
			}
			eg.addElement(e);
		}
	}
	
	private Collection<Table> findTables( String schemaName, String tableName )
	{
		List<Table> tables = new ArrayList<Table>();
		for (Schema schema : catalog.findSchemas(schemaName))
		{
			for (Table table : schema.findTables(tableName))
			{
				tables.add( table );
			}
		}
		return tables;
	}
	
	private Collection<Schema> findSchemas( String schemaName )
	{
		List<Schema> schemas = new ArrayList<Schema>();
		for (Schema schema : catalog.findSchemas(schemaName))
		{
			schemas.add( schema );
			
		}
		return schemas;
	}
	
	private Collection<Column> findColumns( String schemaName, String tableName, String columnName )
	{
		List<Column> columns = new ArrayList<Column>();
		for (Schema schema : catalog.findSchemas(schemaName))
		{
			for (Table table : schema.findTables(tableName))
			{
				for (Column column : table.findColumns(columnName))
				{
					columns.add( column );
				}
			}
		}
		return columns;
	}
	
	public TableDef getTableDef( String name )
	{
		TableDefImpl tableDef = new TableDefImpl( name );
		for (Var var : query.getProjectVars())
		{
			Column c = columnsInQuery.get( var );
			if (c == null)
			{
				throw new IllegalStateException( var+" was not found in the solution");
			}
			tableDef.add( columnsInQuery.get( var ));
		}
		return tableDef;
	}
}

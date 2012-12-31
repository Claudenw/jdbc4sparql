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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;

public class SparqlQueryBuilder
{
	private Query query;
	/*
	private boolean distinct;
	private Set<String> vars;
	private Set<Triple> bgp;
	private Set<String> filters;
	*/
	private Catalog catalog;

	public SparqlQueryBuilder( Catalog catalog )
	{
		this.catalog = catalog;
		this.query = new Query();
		query.setQuerySelectType();
//		distinct = false;
//		vars = new HashSet<String>();
//		bgp = new HashSet<Triple>();
//		filters = new HashSet<String>();	
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
		getElementGroup().addTriplePattern(new Triple( s, p, o ));
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
	 * @param schemaName
	 * @param tableName
	 * @return
	 */
	public Node addTable( String schemaName, String tableName )
	{
		Schema schema =catalog.getSchema( schemaName );
		Table table = schema.getTable( tableName );
		Node tableVar = Node.createVariable( getTableVar( table ));
		addBGP( tableVar, RDF.type.asNode(), Node.createURI(table.getFQName()));
		return tableVar;
	}
	
	/**
	 * Returns the URI for the column.
	 * @param schemaName
	 * @param tableName
	 * @param columnName
	 * @return
	 */
	public Node getColumnURI(String schemaName, String tableName, String columnName)
	{
		Schema schema =catalog.getSchema( schemaName );
		Table table = schema.getTable( tableName );
		Column column = table.getColumn( columnName );
		return Node.createURI( column.getFQName() );
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
		ElementGroup eg = new ElementGroup();
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
		getElementGroup().addElement(eg);
	}
}

package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import java.sql.SQLException;

import net.sf.jsqlparser.expression.Function;

import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor;

public abstract class AbstractFunctionHandler
{
	protected SparqlQueryBuilder builder;
	protected final SparqlExprVisitor exprVisitor;

	public AbstractFunctionHandler( final SparqlQueryBuilder builder )
	{
		this.builder = builder;
		this.exprVisitor = new SparqlExprVisitor(builder,
				SparqlQueryBuilder.REQUIRED);
	}

	/**
	 * Return true if this Handler handles the function;
	 *
	 * @param func
	 * @return
	 * @throws SQLException
	 */
	abstract public boolean handle( Function func ) throws SQLException;

	// protected Var getVar(Function func, int type )
	// {
	// Column column = new FunctionColumn( getSchema(), func, type );
	// return Var.alloc(builder.addFunction( column ));
	// }

	// protected Schema getSchema()
	// {
	// Catalog cat = builder.getCatalog( MetaCatalogBuilder.LOCAL_NAME );
	// return cat.getSchema( MetaCatalogBuilder.FUNCTION_SCHEMA );
	// }

}

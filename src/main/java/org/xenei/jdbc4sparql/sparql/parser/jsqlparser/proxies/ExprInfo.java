package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.proxies;

import java.util.Set;

import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.sparql.items.NamedObject;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor.ExprColumn;

import org.apache.jena.sparql.expr.Expr;

/**
 * An annotated expression that contains the set of columns contained in the expression.
 *
 * This class is used to create a dynamic proxy to the expression so that it can function as 
 * the expression where ever the expression is needed in the system.
 */
public interface ExprInfo extends NamedObject<ColumnName>, Expr {
	public Set<ExprColumn> getColumns();

	public Expr getExpr();
}

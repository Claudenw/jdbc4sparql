package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.proxies;

import java.util.Set;

import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.sparql.items.NamedObject;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor.ExprColumn;

import com.hp.hpl.jena.sparql.expr.Expr;

public interface ExprInfo extends NamedObject<ColumnName>, Expr {
	public Set<ExprColumn> getColumns();

	public Expr getExpr();
}

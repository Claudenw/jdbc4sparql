package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.proxies;

import java.util.Collection;
import java.util.Collections;

import net.sf.cglib.proxy.Enhancer;

import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor.ExprColumn;

import com.hp.hpl.jena.sparql.expr.Expr;

public class ExprInfoFactory {

	public static Expr getInstance(final Expr expr, final ColumnName name) {
		return getInstance(expr, Collections.<ExprColumn> emptyList(), name);
	}

	public static Expr getInstance(final Expr expr,
			final Collection<ExprColumn> columns, final ColumnName name) {

		final Class<?>[] clazz = new Class[1];
		clazz[0] = ExprInfo.class;

		final Enhancer e = new Enhancer();
		e.setInterfaces(clazz);
		e.setCallback(new ExprInfoHandler(expr, columns, name));
		return (Expr) e.create();

	}

}

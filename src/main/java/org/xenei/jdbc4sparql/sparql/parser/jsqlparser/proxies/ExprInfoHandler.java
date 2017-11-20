package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.proxies;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor.ExprColumn;

import org.apache.jena.sparql.expr.Expr;

public class ExprInfoHandler implements MethodInterceptor {
	private final Expr base;
	private final Set<ExprColumn> columns;
	private final ColumnName name;

	public ExprInfoHandler(final Expr base,
			final Collection<ExprColumn> columns, final ColumnName name) {
		this.base = base;
		this.columns = new HashSet<ExprColumn>();
		this.columns.addAll(columns);
		this.name = name;
	}

	@Override
	public Object intercept(final Object obj, final Method method,
			final Object[] args, final MethodProxy proxy) throws Throwable {
		if (method.getName().equals("getColumns")) {
			return columns;
		}

		if (method.getName().equals("getName")) {
			return name;
		}

		if (method.getName().equals("getExpr")) {
			return base;
		}
		return method.invoke(base, args);
	}

}

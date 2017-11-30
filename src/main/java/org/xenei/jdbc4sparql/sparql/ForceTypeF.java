package org.xenei.jdbc4sparql.sparql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.NameSegments;
import org.xenei.jdbc4sparql.iface.TypeConverter;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;

import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.E_Function;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.ExprEvalException;
import org.apache.jena.sparql.expr.ExprFunction0;
import org.apache.jena.sparql.expr.ExprFunction1;
import org.apache.jena.sparql.expr.ExprFunction2;
import org.apache.jena.sparql.expr.ExprFunction3;
import org.apache.jena.sparql.expr.ExprFunctionN;
import org.apache.jena.sparql.expr.ExprFunctionOp;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.ExprNone;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.ExprVisitor;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.nodevalue.NodeValueBoolean;
import org.apache.jena.sparql.function.FunctionEnv;
import org.apache.jena.sparql.graph.NodeTransform;
import org.apache.jena.sparql.syntax.ElementBind;

/**
 * A local filter that removes any values that are null and not allowed to be
 * null or that can not be converted to the expected column value type.
 */
public class ForceTypeF extends CheckTypeF {
	private static final Logger LOG = LoggerFactory.getLogger(ForceTypeF.class);
	public static final String IRI = "java:"
			+ ForceTypeF.class.getCanonicalName();

	public static E_Function getFunction(final QueryColumnInfo columnInfo) {
		return new E_Function(IRI, getExprList(columnInfo));
	}

	public static ElementBind getBinding(final QueryColumnInfo columnInfo) {		
	    Var replace = Var.alloc( columnInfo.getName().getSPARQLName( NameSegments.COLUMN));
		return new ElementBind(replace, getFunction(columnInfo));
	}

	@Override
	public NodeValue exec(final Binding binding, final ExprList args,
			final String uri, final FunctionEnv env) {
		if (((NodeValueBoolean) super.exec(binding, args, uri, env))
				.getBoolean()) {
			final NodeValue retval = TypeConverter.getNodeValue(super
					.getValue());
			if (LOG.isDebugEnabled()) {
				LOG.debug("ForceTypeF( {} ) is {} ({})", args.get(0), retval, binding.get( ((ExprVar) args.get(0)).asVar()));
			}			
			return retval;
		}
		throw new ExprEvalException(
				"CheckTypeF for ForceTypeF did not return true.");

	}

}
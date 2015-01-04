package org.xenei.jdbc4sparql.sparql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.TypeConverter;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;

import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.expr.E_Function;
import com.hp.hpl.jena.sparql.expr.ExprEvalException;
import com.hp.hpl.jena.sparql.expr.ExprList;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueBoolean;
import com.hp.hpl.jena.sparql.function.FunctionEnv;
import com.hp.hpl.jena.sparql.syntax.ElementBind;

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
		return new ElementBind(columnInfo.getVar(), getFunction(columnInfo));
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
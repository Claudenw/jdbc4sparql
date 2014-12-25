package org.xenei.jdbc4sparql.sparql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.TypeConverter;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;

import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.syntax.ElementBind;

/**
 * A local filter that removes any values that are null and not allowed to be
 * null or that can not be converted to the expected column value type.
 */
public class ForceTypeF extends ExprFunction1 {
	private final CheckTypeF checkFunc;
	private static final Logger LOG = LoggerFactory.getLogger(ForceTypeF.class);

	private static Expr checkCheckTypeF(final CheckTypeF checkTypeF) {
		if (checkTypeF == null) {
			throw new IllegalArgumentException("checkTypeF may not be null");
		}
		return new ExprVar(checkTypeF.getColumnInfo().getColumn().getName()
				.getGUID());
	}

	public ForceTypeF(final CheckTypeF checkFunc) {
		super(checkCheckTypeF(checkFunc), "forceTypeF");
		this.checkFunc = checkFunc;
	}

	@Override
	public Expr copy(final Expr expr) {
		return new ForceTypeF(checkFunc);
	}

	@Override
	public boolean equals(final Object o) {
		if (o instanceof ForceTypeF) {
			final ForceTypeF cf = (ForceTypeF) o;
			return checkFunc.equals(cf.checkFunc)
					&& getArg().asVar().equals(cf.getArg().asVar());
		}
		return false;
	}

	@Override
	public NodeValue eval(final NodeValue v) {
		final Object value = checkFunc.getValue();
		final NodeValue retval = TypeConverter.getNodeValue(value);
		if (LOG.isDebugEnabled()) {
			LOG.debug("{} of {} ({}) is {}", this, v, value, retval);
		}
		return retval;
	}

	public ElementBind getBinding(final QueryColumnInfo columnInfo) {
		return new ElementBind(columnInfo.getVar(), this);
	}

}
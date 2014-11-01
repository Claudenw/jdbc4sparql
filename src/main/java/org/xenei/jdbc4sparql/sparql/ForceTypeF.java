package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.syntax.ElementBind;

import org.xenei.jdbc4sparql.iface.TypeConverter;

/**
 * A local filter that removes any values that are null and not allowed to
 * be null or that can not be converted
 * to the expected column value type.
 */
public class ForceTypeF extends ExprFunction1
{
	private final CheckTypeF checkFunc;

	public ForceTypeF( CheckTypeF checkFunc )
	{
		super(checkFunc==null?null:checkFunc.getArg(), "forceTypeF");
		if (checkFunc == null)
		{
			throw new IllegalArgumentException("checkTypeF may not be null");
		}
		this.checkFunc = checkFunc;
	}

	@Override
	public Expr copy( final Expr expr )
	{
		return new ForceTypeF(checkFunc);
	}

	@Override
	public boolean equals( final Object o )
	{
		if (o instanceof ForceTypeF)
		{
			final ForceTypeF cf = (ForceTypeF) o;

			return checkFunc.equals(cf.checkFunc)
					&& getArg().asVar().equals(cf.getArg().asVar());
		}
		return false;
	}

	@Override
	public NodeValue eval( final NodeValue v )
	{
		return TypeConverter.getNodeValue( checkFunc.getValue() );
	}
	
	public ElementBind getBinding() {
		return new ElementBind( checkFunc.getColumnInfo().getVar(), this );
	}

}
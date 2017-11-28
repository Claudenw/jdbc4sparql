package org.xenei.jdbc4sparql.iface.name;

import org.apache.jena.sparql.core.Var;

/**
 * Identifies an object with a GUID.
 *
 */
public interface GUIDObject {
	
	public static Var asVar( GUIDObject obj ) {
		return Var.alloc(asVarName( obj ));
	}
	
	public static String asVarName( GUIDObject obj ) {
		return "v_"+obj.getGUID().replaceAll( "-", "_");
	}
	
	/**
	 * Return the GUID as a string.
	 *
	 * @return the GUID string.
	 */
	public String getGUID();
	
	
}

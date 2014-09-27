package org.xenei.jdbc4sparql.utils;

public class SQLNameUtil
{

	private SQLNameUtil()
	{
		
	}
	
	/**
	 * Clean up a name to only include A-Z, a-z, 0-9 and _
	 * @param name
	 * @return
	 */
	public static String clean(String name)
	{
		return name.replaceAll("[^A-Za-z0-9]+", "_");
	}

}

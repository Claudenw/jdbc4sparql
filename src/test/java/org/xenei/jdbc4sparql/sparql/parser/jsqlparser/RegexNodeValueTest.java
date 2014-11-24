package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import static org.junit.Assert.*;

import org.junit.Test;

public class RegexNodeValueTest {
	private RegexNodeValue rnv;
	private static final String SLASH ="\\"; 
	
	@Test
	public void testLeftSq()
	{
		rnv = new RegexNodeValue( "x[x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"[x$", rnv.asString() );	
	}
	
	@Test
	public void testLeftRight()
	{
		rnv = new RegexNodeValue( "x]x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"]x$", rnv.asString() );	
	}
	
	@Test
	public void testCarat()
	{
		rnv = new RegexNodeValue( "x^x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"^x$", rnv.asString() );	
	}
	
	@Test
	public void testDot()
	{
		rnv = new RegexNodeValue( "x.x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+".x$", rnv.asString() );
	}

	@Test
	public void testBackSlash()
	{
		rnv = new RegexNodeValue( "x"+SLASH+"x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+SLASH+"x$", rnv.asString() );
	}
	
	@Test
	public void testQuestion()
	{
		rnv = new RegexNodeValue( "x?x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"?x$", rnv.asString() );
	}
	
	@Test
	public void testAsterisk()
	{
		rnv = new RegexNodeValue( "x*x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"*x$", rnv.asString() );
	}
	
	@Test
	public void testPlus()
	{
		rnv = new RegexNodeValue( "x+x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"+x$", rnv.asString() );
	}
	
	@Test
	public void testLeftCurley()
	{
		rnv = new RegexNodeValue( "x{x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"{x$", rnv.asString() );
	}
	
	@Test
	public void testRightCurley()
	{
		rnv = new RegexNodeValue( "x}x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"}x$", rnv.asString() );
	}
	
	@Test
	public void testLeftParen()
	{
		rnv = new RegexNodeValue( "x(x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"(x$", rnv.asString() );
	}
	
	@Test
	public void testRightParen()
	{
		rnv = new RegexNodeValue( "x)x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+")x$", rnv.asString() );
	}
	
	@Test
	public void testBar()
	{
		rnv = new RegexNodeValue( "x|x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"|x$", rnv.asString() );
	}
	
	@Test
	public void testDollar()
	{
		rnv = new RegexNodeValue( "x$x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"$x$", rnv.asString() );
	}
	
	@Test
	public void testUnderbar()
	{
		rnv = new RegexNodeValue( "x_x" );
		assertTrue( rnv.isWildcard() );
		assertEquals( "^x.x$", rnv.asString() );
	}
	
	@Test
	public void testPercent()
	{
		rnv = new RegexNodeValue( "x%x" );
		assertTrue( rnv.isWildcard() );
		assertEquals( "^x(.+)x$", rnv.asString() );
	}

	@Test
	public void testEscUnderbar()
	{
		rnv = new RegexNodeValue( "x"+SLASH+"_x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x_x$", rnv.asString() );
	}
	
	@Test
	public void testEscPercent()
	{
		rnv = new RegexNodeValue( "x"+SLASH+"%x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x%x$", rnv.asString() );
	}
	
	
	@Test
	public void testDblEscPercent()
	{
		rnv = new RegexNodeValue( "x"+SLASH+SLASH+"%x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+SLASH+SLASH+SLASH+"(.+)x$", rnv.asString() );
	}
	
	@Test
	public void testDblEscCarat()
	{
		rnv = new RegexNodeValue( "x"+SLASH+SLASH+"^x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+SLASH+SLASH+SLASH+SLASH+"^x$", rnv.asString() );
	}
}

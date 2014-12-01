package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import static org.junit.Assert.*;

import org.junit.Test;

public class RegexNodeValueTest {
	private RegexNodeValue rnv;
	private static final String SLASH ="\\"; 
	
	@Test
	public void testLeftSq()
	{
		rnv = RegexNodeValue.create( "x[x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"[x$", rnv.asString() );	
	}
	
	@Test
	public void testLeftRight()
	{
		rnv = RegexNodeValue.create( "x]x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"]x$", rnv.asString() );	
	}
	
	@Test
	public void testCarat()
	{
		rnv = RegexNodeValue.create( "x^x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"^x$", rnv.asString() );	
	}
	
	@Test
	public void testDot()
	{
		rnv = RegexNodeValue.create( "x.x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+".x$", rnv.asString() );
	}

	@Test
	public void testBackSlash()
	{
		rnv = RegexNodeValue.create( "x"+SLASH+"x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+SLASH+"x$", rnv.asString() );
	}
	
	@Test
	public void testQuestion()
	{
		rnv = RegexNodeValue.create( "x?x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"?x$", rnv.asString() );
	}
	
	@Test
	public void testAsterisk()
	{
		rnv = RegexNodeValue.create( "x*x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"*x$", rnv.asString() );
	}
	
	@Test
	public void testPlus()
	{
		rnv = RegexNodeValue.create( "x+x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"+x$", rnv.asString() );
	}
	
	@Test
	public void testLeftCurley()
	{
		rnv = RegexNodeValue.create( "x{x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"{x$", rnv.asString() );
	}
	
	@Test
	public void testRightCurley()
	{
		rnv = RegexNodeValue.create( "x}x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"}x$", rnv.asString() );
	}
	
	@Test
	public void testLeftParen()
	{
		rnv = RegexNodeValue.create( "x(x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"(x$", rnv.asString() );
	}
	
	@Test
	public void testRightParen()
	{
		rnv = RegexNodeValue.create( "x)x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+")x$", rnv.asString() );
	}
	
	@Test
	public void testBar()
	{
		rnv = RegexNodeValue.create( "x|x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"|x$", rnv.asString() );
	}
	
	@Test
	public void testDollar()
	{
		rnv = RegexNodeValue.create( "x$x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+"$x$", rnv.asString() );
	}
	
	@Test
	public void testUnderbar()
	{
		rnv = RegexNodeValue.create( "x_x" );
		assertTrue( rnv.isWildcard() );
		assertEquals( "^x.x$", rnv.asString() );
	}
	
	@Test
	public void testPercent()
	{
		rnv = RegexNodeValue.create( "x%x" );
		assertTrue( rnv.isWildcard() );
		assertEquals( "^x(.+)x$", rnv.asString() );
	}

	@Test
	public void testEscUnderbar()
	{
		rnv = RegexNodeValue.create( "x"+SLASH+"_x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "x_x", rnv.asString() );
	}
	
	@Test
	public void testEscPercent()
	{
		rnv = RegexNodeValue.create( "x"+SLASH+"%x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "x%x", rnv.asString() );
	}
	
	
	@Test
	public void testDblEscPercent()
	{
		rnv = RegexNodeValue.create( "x"+SLASH+SLASH+"%x" );
		assertTrue( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+SLASH+SLASH+SLASH+"(.+)x$", rnv.asString() );
	}
	
	@Test
	public void testDblEscCarat()
	{
		rnv = RegexNodeValue.create( "x"+SLASH+SLASH+"^x" );
		assertFalse( rnv.isWildcard() );
		assertEquals( "^x"+SLASH+SLASH+SLASH+SLASH+SLASH+"^x$", rnv.asString() );
	}
}

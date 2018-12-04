/**
 * Copyright (c) 2008 Greg Whalin
 * All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the BSD license
 *
 * This library is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.
 *
 * You should have received a copy of the BSD License along with this
 * library.
 *
 * @author Kevin Burton
 * @author greg whalin <greg@meetup.com>
 */
package edu.usc.cs550.rejig.client.test;

import edu.usc.cs550.rejig.client.*;
import edu.usc.cs550.rejig.interfaces.Fragment;
import edu.usc.cs550.rejig.interfaces.RejigConfig;

import java.util.*;
import java.io.Serializable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

public class UnitTests {

	// logger
	private static Logger log =
		Logger.getLogger( UnitTests.class.getName() );

	public static MemcachedClient mc  = null;

	public static void test1() {
			mc.set( "foo", Boolean.TRUE );
			Boolean b = (Boolean)mc.get( "foo" );
			assertion(
				b.booleanValue(),
				"+ store/retrieve Boolean type test failed"
			);
	}

	public static void test2() {
			mc.set( "foo", new Integer( Integer.MAX_VALUE ) );
			Integer i = (Integer)mc.get( "foo" );
			assertion(
				i.intValue() == Integer.MAX_VALUE,
				"+ store/retrieve Integer type test failed"
			);
	}

	public static void test3() {
			String input = "test of string encoding";
			mc.set( "foo", input );
			String s = (String)mc.get( "foo" );
			assertion(
				s.equals( input ),
				"+ store/retrieve String type test failed"
			);
	}

	public static void test4() {
			mc.set( "foo", new Character( 'z' ) );
			Character c = (Character)mc.get( "foo" );
			assertion(
				c.charValue() == 'z',
				"+ store/retrieve Character type test failed"
			);
	}

	public static void test5() {
			mc.set( "foo", new Byte( (byte)127 ) );
			Byte b = (Byte)mc.get( "foo" );
			assertion(
				b.byteValue() == 127,
				"+ store/retrieve Byte type test failed"
			);
	}

	public static void test6() {
			mc.set( "foo", new StringBuffer( "hello" ) );
			StringBuffer o = (StringBuffer)mc.get( "foo" );
			assertion(
				o.toString().equals( "hello" ),
				"+ store/retrieve StringBuffer type test failed"
			);
	}

	public static void test7() {
			mc.set( "foo", new Short( (short)100 ) );
			Short o = (Short)mc.get( "foo" );
			assertion(
				o.shortValue() == 100,
				"+ store/retrieve Short type test failed"
			);
	}

	public static void test8() {
			mc.set( "foo", new Long( Long.MAX_VALUE ) );
			Long o = (Long)mc.get( "foo" );
			assertion(
				o.longValue() == Long.MAX_VALUE,
				"+ store/retrieve Long type test failed"
			);
	}

	public static void test9() {
			mc.set( "foo", new Double( 1.1 ) );
			Double o = (Double)mc.get( "foo" );
			assertion(
				o.doubleValue() == 1.1,
				"+ store/retrieve Double type test failed"
			);
	}

	public static void test10() {
			mc.set( "foo", new Float( 1.1f ) );
			Float o = (Float)mc.get( "foo" );
			assertion(
				o.floatValue() == 1.1f,
				"+ store/retrieve Float type test failed"
			);
	}

	public static void test11() {
			mc.set( "foo", new Integer( 100 ), new Date( System.currentTimeMillis() ));
			try { Thread.sleep( 1000 ); } catch ( Exception ex ) { }
			assertion(
				mc.get( "foo" ) == null,
				"+ store/retrieve w/ expiration test failed"
			);
	}

	public static void test12() {
		long i = 0;
		mc.storeCounter("foo", i);
		mc.incr("foo"); // foo now == 1
		mc.incr("foo", (long)5); // foo now == 6
		long j = mc.decr("foo", (long)2); // foo now == 4
		assertion(
			j == 4 && j == mc.getCounter( "foo" ),
			"+ incr/decr test failed"
		);
	}

	public static void test13() {
		Date d1 = new Date();
		mc.set("foo", d1);
		Date d2 = (Date) mc.get("foo");
		assertion(
			d1.equals( d2 ),
			"+ store/retrieve Date type test failed"
		);
	}

	public static void test14() {
		assertion(
			!mc.keyExists( "foobar123" ),
			"+ store/retrieve test failed"
		);
		mc.set( "foobar123", new Integer( 100000) );
		assertion(
			mc.keyExists( "foobar123" ),
			"+ store/retrieve test failed"
		);

		assertion(
			!mc.keyExists( "counterTest123" ),
			"+ store/retrieve test failed"
		);
		mc.storeCounter( "counterTest123", 0 );
		assertion(
			mc.keyExists( "counterTest123" ),
			"+ counter store test failed"
		);
	}

	public static void test15() {

		Map stats = mc.statsItems();
		assertion(stats != null, "+ stats test failed");

		stats = mc.statsSlabs();
		assertion(stats != null, "+ stats test failed");
	}

	public static void test16() {
		assertion(
			!mc.set( "foo", null ),
			"+ invalid data store [null] test failed"
		);
	}

	public static void test17() {
		mc.set( "foo bar", Boolean.TRUE );
		Boolean b = (Boolean)mc.get( "foo bar" );
		assertion(
			b.booleanValue(),
			"+ store/retrieve Boolean type test failed"
		);
	}

	public static void test18() {
		long i = 0;
		mc.addOrIncr( "foo" ); // foo now == 0
		mc.incr( "foo" ); // foo now == 1
		mc.incr( "foo", (long)5 ); // foo now == 6

		mc.addOrIncr( "foo" ); // foo now 7

		long j = mc.decr( "foo", (long)3 ); // foo now == 4
		assertion(
			j == 4 && j == mc.getCounter( "foo" ),
			"+ incr/decr test failed"
		);
	}

	public static void test19() {
		int max = 100;
		String[] keys = new String[ max ];
		for ( int i=0; i<max; i++ ) {
			keys[i] = Integer.toString(i);
			mc.set( keys[i], "value"+i );
		}

		Map<String,Object> results = mc.getMulti( keys );
		for ( int i=0; i<max; i++ ) {
			assertion(
				results.get( keys[i]).equals( "value"+i ),
				"+ getMulti test failed"
			);
		}
	}

	public static void test20( int max, int skip, int start ) {
		log.warn( String.format( "test 20 starting with start=%5d skip=%5d max=%7d", start, skip, max ) );
		int numEntries = max/skip+1;
		String[] keys = new String[ numEntries ];
		byte[][] vals = new byte[ numEntries ][];

		int size = start;
		for ( int i=0; i<numEntries; i++ ) {
			keys[i] = Integer.toString( size );
			vals[i] = new byte[size + 1];
			for ( int j=0; j<size + 1; j++ )
				vals[i][j] = (byte)j;

			mc.set( keys[i], vals[i] );
			size += skip;
		}

		Map<String,Object> results = mc.getMulti( keys );
		for ( int i=0; i<numEntries; i++ ) {
			assertion(
				Arrays.equals( (byte[])results.get( keys[i]), vals[i] ),
				"test 20 failed"
			);
		}

		log.warn( String.format( "test 20 finished with start=%5d skip=%5d max=%7d", start, skip, max ) );
	}

	public static void test21() {
		mc.set( "foo", new StringBuilder( "hello" ) );
		StringBuilder o = (StringBuilder)mc.get( "foo" );
		assertion(
			o.toString().equals( "hello" ),
			"+ store/retrieve StringBuilder type test failed"
		);
	}

	public static void test22() {
		byte[] b = new byte[10];
		for ( int i = 0; i < 10; i++ )
			b[i] = (byte)i;

				mc.set( "foo", b );
		assertion(
			Arrays.equals( (byte[])mc.get( "foo" ), b ),
			"+ store/retrieve byte[] type test failed"
		);
	}

	public static void test23() {
		TestClass tc = new TestClass( "foo", "bar", new Integer( 32 ) );
				mc.set( "foo", tc );
		assertion(
			tc.equals( (TestClass)mc.get( "foo" ) ),
			"+ store/retrieve serialized object test failed"
		);
	}

	public static void test24() {

		String[] allKeys = { "key1", "key2", "key3", "key4", "key5", "key6", "key7" };
		String[] setKeys = { "key1", "key3", "key5", "key7" };

		for ( String key : setKeys ) {
			mc.set( key, key );
		}

		Map<String,Object> results = mc.getMulti( allKeys );

		assertion(
			allKeys.length == results.size(),
			"+ getMulti w/ keys that don't exist test failed"
		);
		for ( String key : setKeys ) {
			String val = (String)results.get( key );
			assertion(
				key.equals( val ),
				"+ getMulti w/ keys that don't exist test failed"
			);
		}
	}

	// Sets the config object into the client and grant a
	// lease to all fragments for 10 mins.
	public static void setup(RejigConfig config) {
		boolean b = mc.setConfig( config, null );
		assertion( b, "+ setting config failed" );

		Date expiry = new Date(System.currentTimeMillis() + 10*60*1000);
		int fragmentNum = 1;
		for ( Fragment f : config.getFragmentList() ) {
			boolean g = mc.grantLease( fragmentNum, expiry, f.getAddress() );
			assertion( g, "+ granting lease failed: Fragment num: " + fragmentNum );
			fragmentNum++;
		}
	}

	// Revoke all leases granted.
	private static void cleanup(RejigConfig config) {
		int fragmentNum = 1;
		for ( Fragment f : config.getFragmentList() ) {
			boolean g = mc.revokeLease( fragmentNum, f.getAddress() );
			assertion( g, "+ revoking lease failed. Fragment num: " + fragmentNum );
			fragmentNum++;
		}
	}

	private static void assertion(boolean condition, String errorMessage) {
		if (!condition) {
			throw new AssertionError(errorMessage);
		}
	}

	public static void runAlTests( MemcachedClient mc, boolean run14 ) {
		if (run14) {
			test14();
		}
		for ( int t = 0; t < 2; t++ ) {
			mc.setCompressEnable( ( t&1 ) == 1 );

			test1();
			test2();
			test3();
			test4();
			test5();
			test6();
			test7();
			test8();
			test9();
			test10();
			test11();
			test12();
			test13();
			test15();
			test16();
			test17();
			test21();
			test22();
			test23();
			test24();

			for ( int i = 0; i < 3; i++ )
				test19();

			test20( 8191, 1, 0 );
			test20( 8192, 1, 0 );
			test20( 8193, 1, 0 );

			test20( 16384, 100, 0 );
			test20( 17000, 128, 0 );

			test20( 128*1024, 1023, 0 );
			test20( 128*1024, 1023, 1 );
			test20( 128*1024, 1024, 0 );
			test20( 128*1024, 1024, 1 );

			test20( 128*1024, 1023, 0 );
			test20( 128*1024, 1023, 1 );
			test20( 128*1024, 1024, 0 );
			test20( 128*1024, 1024, 1 );

			test20( 900*1024, 32*1024, 0 );
			test20( 900*1024, 32*1024, 1 );
		}

	}

	/**
	 * This runs through some simple tests of the MemcacheClient.
	 */
	public static void main(String[] args) {

		BasicConfigurator.configure();
		org.apache.log4j.Logger.getRootLogger().setLevel( Level.WARN );

		String[] serverlist = {
			"localhost:11210",
			"localhost:11211",
			"localhost:11212",
			"localhost:11213",
			"localhost:11214",
			"localhost:11215",
			"localhost:11216",
			"localhost:11217",
			"localhost:11218",
			"localhost:11219"
		};

		// initialize the pool options
		SockIOPool.SockIOPoolOptions options = new SockIOPool.SockIOPoolOptions();
		options.maxConn = 250;
		options.maintSleep = 30;
		options.nagle = false;
		options.hashingAlg = SockIOPool.FragmentHashingAlgo.NATIVE_HASH;

		// set RejigConfig
		RejigConfig config = createConfig1(serverlist);
		MockRejigConfigReader configReader = new MockRejigConfigReader();
		configReader.setConfig(config);

		// get client instance
		mc = new MemcachedClient(null, new MockErrorHandler(),
			"test", configReader, options);

		// run tests.
		mc.flushAll();
		System.out.println("Running tests.");
		setup(config);
		runAlTests( mc, true );

		// change the config.
		System.out.println("Re-running with config change.");
		setConfig2(config, configReader, mc);

		// run tests again
		runAlTests( mc, false );

		cleanup(configReader.getConfig());
		mc.flushAll();
		// shutdown
		mc.shutDown();
	}

	private static RejigConfig createConfig1(String[] serverlist) {
		Integer[] weights = { 1, 1, 1, 1, 10, 5, 1, 1, 1, 3 };
		RejigConfig.Builder builder = RejigConfig.newBuilder()
			.setId(1);
		for (int i = 0; i < weights.length; i++) {
			int weight = weights[i];
			for (int j = 0; j < weight; j++) {
				builder.addFragment(Fragment.newBuilder()
					.setId(1)
					.setAddress(serverlist[i])
					.build()
				);
			}
		}
		return builder.build();
	}

	private static void setConfig2(RejigConfig curr, MockRejigConfigReader configReader, MemcachedClient mc) {
		RejigConfig.Builder builder = curr.toBuilder().setId(2);
		for (int i = 9; i < 14; i++) {
			builder.setFragment(i, Fragment.newBuilder()
				.setId(2)
				.setAddress("localhost:11215")
				.build()
			);
			mc.revokeLease(i + 1, "localhost:11214");
			mc.grantLease(i + 1, new Date(System.currentTimeMillis() + 10*60*1000), "localhost:11215");
		}
		RejigConfig config = builder.build();
		configReader.setConfig(config);
		// Update impacted server.
		mc.setConfig(config, null, "localhost:11214");
	}

	/**
	 * Class for testing serializing of objects.
	 *
	 * @author $Author: $
	 * @version $Revision: $ $Date: $
	 */
	public static final class TestClass implements Serializable {

		private String field1;
		private String field2;
		private Integer field3;

		public TestClass( String field1, String field2, Integer field3 ) {
			this.field1 = field1;
			this.field2 = field2;
			this.field3 = field3;
		}

		public String getField1() { return this.field1; }
		public String getField2() { return this.field2; }
		public Integer getField3() { return this.field3; }

		public boolean equals( Object o ) {
			if ( this == o ) return true;
			if ( !( o instanceof TestClass ) ) return false;

			TestClass obj = (TestClass)o;

			return ( ( this.field1 == obj.getField1() || ( this.field1 != null && this.field1.equals( obj.getField1() ) ) )
					&& ( this.field2 == obj.getField2() || ( this.field2 != null && this.field2.equals( obj.getField2() ) ) )
					&& ( this.field3 == obj.getField3() || ( this.field3 != null && this.field3.equals( obj.getField3() ) ) ) );
		}
	}
}

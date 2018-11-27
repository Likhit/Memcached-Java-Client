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
 * @author greg whalin <greg@meetup.com>
 */
package edu.usc.cs550.rejig.client.test;

import edu.usc.cs550.rejig.client.*;
import edu.usc.cs550.rejig.interfaces.Fragment;
import edu.usc.cs550.rejig.interfaces.RejigConfig;

import org.apache.log4j.*;

public class TestMemcached  {
	public static void main(String[] args) {
		      // memcached should be running on port 11211 but NOT on 11212

		BasicConfigurator.configure();
		String[] servers = { "localhost:11211", "localhost:11212" };
    // initialize the pool options
		SockIOPool.SockIOPoolOptions options = new SockIOPool.SockIOPoolOptions();
    options.failover = true;
		options.initConn = 10;
		options.minConn = 5;
		options.maxConn = 250;
		options.maintSleep = 30;
		options.nagle = false;
    options.socketTO = 3000;
    options.aliveCheck = true;

    // set RejigConfig
		MockRejigConfigReader configReader = new MockRejigConfigReader();
		configReader.setConfig(RejigConfig.newBuilder()
			.setId(1)
			.addFragment(Fragment.newBuilder()
				.setId(1)
				.setAddress(servers[0])
				.build()
			).addFragment(Fragment.newBuilder()
				.setId(1)
				.setAddress(servers[1])
				.build()
			).build());

		// get client instance
		MemcachedClient mcc = new MemcachedClient(
			null, new MockErrorHandler(),
			"test", configReader, options);

		// turn off most memcached client logging:
		edu.usc.cs550.rejig.client.Logger.getLogger( MemcachedClient.class.getName() ).setLevel( edu.usc.cs550.rejig.client.Logger.LEVEL_WARN );

		for ( int i = 0; i < 10; i++ ) {
			boolean success = mcc.set( "" + i, "Hello!" );
			String result = (String)mcc.get( "" + i );
			System.out.println( String.format( "set( %d ): %s", i, success ) );
			System.out.println( String.format( "get( %d ): %s", i, result ) );
		}

		System.out.println( "\n\t -- sleeping --\n" );
		try { Thread.sleep( 10000 ); } catch ( Exception ex ) { }

		for ( int i = 0; i < 10; i++ ) {
			boolean success = mcc.set( "" + i, "Hello!" );
			String result = (String)mcc.get( "" + i );
			System.out.println( String.format( "set( %d ): %s", i, success ) );
			System.out.println( String.format( "get( %d ): %s", i, result ) );
		}
	}
}

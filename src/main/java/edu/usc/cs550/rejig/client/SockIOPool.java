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
package edu.usc.cs550.rejig.client;
import edu.usc.cs550.rejig.interfaces.Fragment;
import edu.usc.cs550.rejig.interfaces.RejigConfig;


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

// java.util
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Date;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;

import java.util.zip.*;
import java.net.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;

/**
 * This class is a connection pool for maintaning a pool of persistent connections<br/>
 * to memcached servers.
 *
 * The pool must be initialized prior to use. This should typically be early on<br/>
 * in the lifecycle of the JVM instance.<br/>
 * <br/>
 * <h3>An example of initializing using defaults:</h3>
 * <pre>
 *
 *	static {
 *		String[] serverlist = { "cache0.server.com:12345", "cache1.server.com:12345" };
 *
 *		SockIOPool pool = SockIOPool.getInstance();
 *		pool.setServers(serverlist);
 *		pool.initialize();
 *	}
 * </pre>
 * <h3>An example of initializing using defaults and providing weights for servers:</h3>
 *  <pre>
 *	static {
 *		String[] serverlist = { "cache0.server.com:12345", "cache1.server.com:12345" };
 *		Integer[] weights   = { new Integer(5), new Integer(2) };
 *
 *		SockIOPool pool = SockIOPool.getInstance();
 *		pool.setServers(serverlist);
 *		pool.setWeights(weights);
 *		pool.initialize();
 *	}
 *  </pre>
 * <h3>An example of initializing overriding defaults:</h3>
 *  <pre>
 *	static {
 *		String[] serverlist     = { "cache0.server.com:12345", "cache1.server.com:12345" };
 *		Integer[] weights       = { new Integer(5), new Integer(2) };
 *		int initialConnections  = 10;
 *		int minSpareConnections = 5;
 *		int maxSpareConnections = 50;
 *		long maxIdleTime        = 1000 * 60 * 30;	// 30 minutes
 *		long maxBusyTime        = 1000 * 60 * 5;	// 5 minutes
 *		long maintThreadSleep   = 1000 * 5;			// 5 seconds
 *		int	socketTimeOut       = 1000 * 3;			// 3 seconds to block on reads
 *		int	socketConnectTO     = 1000 * 3;			// 3 seconds to block on initial connections.  If 0, then will use blocking connect (default)
 *		boolean failover        = false;			// turn off auto-failover in event of server down
 *		boolean nagleAlg        = false;			// turn off Nagle's algorithm on all sockets in pool
 *		boolean aliveCheck      = false;			// disable health check of socket on checkout
 *
 *		SockIOPool pool = SockIOPool.getInstance();
 *		pool.setServers( serverlist );
 *		pool.setWeights( weights );
 *		pool.setInitConn( initialConnections );
 *		pool.setMinConn( minSpareConnections );
 *		pool.setMaxConn( maxSpareConnections );
 *		pool.setMaxIdle( maxIdleTime );
 *		pool.setMaxBusyTime( maxBusyTime );
 *		pool.setMaintSleep( maintThreadSleep );
 *		pool.setSocketTO( socketTimeOut );
 *		pool.setNagle( nagleAlg );
 *		pool.setHashingAlg( SockIOPool.NEW_COMPAT_HASH );
 *		pool.setAliveCheck( true );
 *		pool.initialize();
 *	}
 *  </pre>
 * The easiest manner in which to initialize the pool is to set the servers and rely on defaults as in the first example.<br/>
 * After pool is initialized, a client will request a SockIO object by calling getSock with the cache key<br/>
 * The client must always close the SockIO object when finished, which will return the connection back to the pool.<br/>
 * <h3>An example of retrieving a SockIO object:</h3>
 * <pre>
 *		SockIOPool.SockIO sock = SockIOPool.getInstance().getSock( key );
 *		try {
 *			sock.write( "version\r\n" );
 *			sock.flush();
 *			System.out.println( "Version: " + sock.readLine() );
 *		}
 *		catch (IOException ioe) { System.out.println( "io exception thrown" ) };
 *
 *		sock.close();
 * </pre>
 *
 * @author greg whalin <greg@whalin.com>
 * @author Likhit Dharmapuri <ldharmap@usc.edu>
 * @version 1.5
 */
public class SockIOPool {

	// logger
	private static Logger log =
		Logger.getLogger( SockIOPool.class.getName() );

	// store instances of pools
	private static Map<String,SockIOPool> pools =
		new HashMap<String,SockIOPool>();

	// Constants
	private static final Integer ZERO       = new Integer( 0 );
	public static final long MAX_RETRY_DELAY = 10 * 60 * 1000;  // max of 10 minute delay for fall off

	// Hashing algorithm to use to select the fragment to assign a key to.
	public static enum FragmentHashingAlgo {
		NATIVE_HASH,			// native String.hashCode();
		OLD_COMPAT_HASH,	// original compatibility hashing algorithm (works with other clients)
		NEW_COMPAT_HASH		// new CRC32 based compatibility hashing algorithm (works with other clients)
	}

	// Pool data
	private MaintThread maintThread;
	private boolean initialized        = false;
	private int maxCreate              = 1;					// this will be initialized by pool when the pool is initialized

	/**
	 * All the configurable parameters used by
	 * the SockIOPool instance.
	 */
	public static class SockIOPoolOptions {
		/** The initial number of connections per server in the available pool. */
		public int initConn = 10;
		/** The min number of connections per server in the available pool. */
		public int minConn = 5;
		/** The max number of connections per server in the available pool. */
		public int maxConn = 100;
		/** The max idle time for avail sockets */
		public long maxIdle = 1000 * 60 * 5;
		/** The max idle time for avail sockets */
		public long maxBusyTime = 1000 * 30;
		/** Maintenance thread sleep time */
		public long maintSleep = 1000 * 30;
		/** Default timeout of socket reads */
		public int socketTO = 1000 * 3;
		/** Default timeout of socket connections */
		public int socketConnectTO = 1000 * 3;
		/** Default to not check each connection for being alive */
		public boolean aliveCheck = false;
		/** Default to failover in event of cache server dead */
		public boolean failover = true;
		/** Only used if failover is also set ... controls putting a dead server back into rotation */
		public boolean failback = true;
		/** Enable/disable Nagle's algorithm */
		public boolean nagle = false;
		/** Default to using the native hash as it is the fastest */
		public FragmentHashingAlgo hashingAlg = FragmentHashingAlgo.NATIVE_HASH;

		public SockIOPoolOptions copy() {
			SockIOPoolOptions copy = new SockIOPoolOptions();
			copy.initConn = initConn;
			copy.minConn = minConn;
			copy.maxConn = maxConn;
			copy.maxIdle = maxIdle;
			copy.maxBusyTime = maxBusyTime;
			copy.maintSleep = maintSleep;
			copy.socketTO = socketTO;
			copy.socketConnectTO = socketConnectTO;
			copy.aliveCheck = aliveCheck;
			copy.failover = failover;
			copy.failback = failback;
			copy.nagle = nagle;
			copy.hashingAlg = hashingAlg;
			return copy;
		}
	}

	private SockIOPoolOptions options;

	private int poolMultiplier = 3;


	// locks
	private final ReentrantLock hostDeadLock = new ReentrantLock();

	// list of all servers
	private RejigConfig config;

	// dead server map
	private Map<String,Date> hostDead;
	private Map<String,Long> hostDeadDur;

	// map to hold all available sockets
	// map to hold busy sockets
	// set to hold sockets to close
	private Map<String,Map<SockIO,Long>> availPool;
	private Map<String,Map<SockIO,Long>> busyPool;
	private Map<SockIO,Integer> deadPool;

	// empty constructor
	protected SockIOPool() { }

	/**
	 * Factory to create/retrieve new pools given a unique poolName.
	 *
	 * @param poolName unique name of the pool
	 * @return instance of SockIOPool
	 */
	public static synchronized SockIOPool getInstance( String poolName ) {
		if ( pools.containsKey( poolName ) )
			return pools.get( poolName );

		SockIOPool pool = new SockIOPool();
		pools.put( poolName, pool );

		return pool;
	}

	/**
	 * Sets the RejigConfig containing the list of all servers.
	 *
	 * @param config RejigConfig object
	 */
	public SockIOPool setRejigConfig( RejigConfig config ) {
		this.config = config;
		return this;
	}

	/**
	 * Returns the current RejigConfig containing the list of all servers.
	 */
	public RejigConfig getRejigConfig() { return this.config; }

	/**
	 * Sets the SockIOPoolOptions that the object.
	 */
	public SockIOPool setPoolOptions(SockIOPoolOptions options) {
		this.options = options;
		return this;
	}

	/**
	 * Returns a copy of the SockIOPoolOptions that the object.
	 */
	public SockIOPoolOptions getPoolOptions() {
		return this.options.copy();
	}

	/**
	 * Internal private hashing method.
	 *
	 * This is the original hashing algorithm from other clients.
	 * Found to be slow and have poor distribution.
	 *
	 * @param key String to hash
	 * @return hashCode for this string using our own hashing algorithm
	 */
	private static long origCompatHashingAlg( String key ) {
		long hash   = 0;
		char[] cArr = key.toCharArray();

		for ( int i = 0; i < cArr.length; ++i ) {
			hash = (hash * 33) + cArr[i];
		}

		return hash;
	}

	/**
	 * Internal private hashing method.
	 *
	 * This is the new hashing algorithm from other clients.
	 * Found to be fast and have very good distribution.
	 *
	 * UPDATE: This is dog slow under java
	 *
	 * @param key
	 * @return
	 */
	private static long newCompatHashingAlg( String key ) {
		CRC32 checksum = new CRC32();
		checksum.update( key.getBytes() );
		long crc = checksum.getValue();
		return (crc >> 16) & 0x7fff;
	}

	/**
	 * Returns a bucket to check for a given key.
	 *
	 * @param key String key cache is stored under
	 * @return int bucket
	 */
	private long getHash( String key, Integer hashCode ) {

		if ( hashCode != null ) {
				return hashCode.longValue();
		}
		else {
			switch ( options.hashingAlg ) {
				case NATIVE_HASH:
					return (long)key.hashCode();
				case OLD_COMPAT_HASH:
					return origCompatHashingAlg( key );
				case NEW_COMPAT_HASH:
					return newCompatHashingAlg( key );
				default:
					// use the native hash as a default
					options.hashingAlg = FragmentHashingAlgo.NATIVE_HASH;
					return (long)key.hashCode();
			}
		}
	}

	private long getBucket( String key, Integer hashCode ) {
		long hc = getHash( key, hashCode );

		long bucket = hc % config.getFragmentCount();
		if ( bucket < 0 ) bucket *= -1;
		return bucket;
	}

	/**
	 * Initializes the pool.
	 */
	public SockIOPool initialize() {

		synchronized( this ) {

			// check to see if already initialized
			if ( initialized
					&& ( availPool != null )
					&& ( busyPool != null ) ) {
						log.error( "++++ trying to initialize an already initialized pool" );
				return this;
			}

			// pools
			availPool   = new HashMap<String,Map<SockIO,Long>>( config.getFragmentCount() * options.initConn );
			busyPool    = new HashMap<String,Map<SockIO,Long>>( config.getFragmentCount() * options.initConn );
			deadPool    = new IdentityHashMap<SockIO,Integer>();

			hostDeadDur = new HashMap<String,Long>();
			hostDead    = new HashMap<String,Date>();
			maxCreate   = (poolMultiplier > options.minConn) ? options.minConn : options.minConn / poolMultiplier;		// only create up to maxCreate connections at once

			if ( log.isDebugEnabled() ) {
				log.debug( "++++ initializing pool with following settings:" );
				log.debug( "++++ initial size: " + options.initConn );
				log.debug( "++++ min spare   : " + options.minConn );
				log.debug( "++++ max spare   : " + options.maxConn );
			}

			// if servers is not set, or it empty, then
			// throw a runtime exception
			if ( config == null || config.getFragmentCount() <= 0 ) {
				log.error( "++++ trying to initialize with no servers" );
				throw new IllegalStateException( "++++ trying to initialize with no servers" );
			}

			// initalize our internal hashing structures
			populateBuckets();

			// mark pool as initialized
			this.initialized = true;

			// start maint thread
			if ( this.options.maintSleep > 0 )
				this.startMaintThread();
		}

		return this;
	}

	private void populateBuckets() {
		if ( log.isDebugEnabled() )
			log.debug( "++++ initializing internal hashing structure for consistent hashing" );

		for ( int i = 0; i < config.getFragmentCount(); i++ ) {
			String server = config.getFragment(i).getAddress();
			// create initial connections
			if ( log.isDebugEnabled() )
				log.debug( "+++ creating initial connections (" + options.initConn + ") for host: " + server);

			for ( int j = 0; j < options.initConn; j++ ) {
				SockIO socket = createSocket( server );
				if ( socket == null ) {
					log.error( "++++ failed to create connection to: " + server + " -- only " + j + " created." );
					break;
				}

				addSocketToPool( availPool, server, socket );
				if ( log.isDebugEnabled() )
					log.debug( "++++ created and added socket: " + socket.toString() + " for host " + server );
			}
		}
	}

	/**
	 * Returns state of pool.
	 *
	 * @return <CODE>true</CODE> if initialized.
	 */
	public boolean isInitialized() {
		return initialized;
	}

	/**
	 * Creates a new SockIO obj for the given server.
	 *
	 * If server fails to connect, then return null and do not try<br/>
	 * again until a duration has passed.  This duration will grow<br/>
	 * by doubling after each failed attempt to connect.
	 *
	 * @param host host:port to connect to
	 * @return SockIO obj or null if failed to create
	 */
	protected SockIO createSocket( String host ) {

		SockIO socket = null;

		// if host is dead, then we don't need to try again
		// until the dead status has expired
		// we do not try to put back in if failback is off
		hostDeadLock.lock();
		try {
			if ( options.failover && options.failback && hostDead.containsKey( host ) && hostDeadDur.containsKey( host ) ) {

				Date store  = hostDead.get( host );
				long expire = hostDeadDur.get( host ).longValue();

				if ( (store.getTime() + expire) > System.currentTimeMillis() )
					return null;
			}
		}
		finally {
			hostDeadLock.unlock();
		}

		try {
			socket = new SockIO( this, host, this.options.socketTO, this.options.socketConnectTO, this.options.nagle );

			if ( !socket.isConnected() ) {
				log.error( "++++ failed to get SockIO obj for: " + host + " -- new socket is not connected" );
				deadPool.put( socket, ZERO );
				socket = null;
			}
		}
		catch ( Exception ex ) {
			log.error( "++++ failed to get SockIO obj for: " + host );
			log.error( ex.getMessage(), ex );
			socket = null;
		}

		// if we failed to get socket, then mark
		// host dead for a duration which falls off
		hostDeadLock.lock();
		try {
			if ( socket == null ) {
				Date now = new Date();
				hostDead.put( host, now );

				long expire = ( hostDeadDur.containsKey( host ) ) ? (((Long)hostDeadDur.get( host )).longValue() * 2) : 1000;

				if ( expire > MAX_RETRY_DELAY )
					expire = MAX_RETRY_DELAY;

				hostDeadDur.put( host, new Long( expire ) );
				if ( log.isDebugEnabled() )
					log.debug( "++++ ignoring dead host: " + host + " for " + expire + " ms" );

				// also clear all entries for this host from availPool
				clearHostFromPool( availPool, host );
			}
			else {
				if ( log.isDebugEnabled() )
					log.debug( "++++ created socket (" + socket.toString() + ") for host: " + host );
				if ( hostDead.containsKey( host ) || hostDeadDur.containsKey( host ) ) {
					hostDead.remove( host );
					hostDeadDur.remove( host );
				}
			}
		}
		finally {
			hostDeadLock.unlock();
		}

		return socket;
	}

 	/**
	 * @param key
	 * @return
	 */
	public String getHost( String key ) {
		return getHost( key, null );
	}

	/**
	 * Gets the host that a particular key / hashcode resides on.
	 *
	 * @param key
	 * @param hashcode
	 * @return
	 */
	public String getHost( String key, Integer hashcode ) {
		SockIO socket = getSockAndFragmentId( key, hashcode ).sock();
		String host = socket.getHost();
		socket.close();
		return host;
	}

	public SockAndFragmentId getHostSockAndFragmentId( String host ) {
		int i = 0;
		for (Fragment f: config.getFragmentList()) {
			if (f.getAddress().equals(host)) {
				return getSockAndFragmentId( null, i );
			}
			i++;
		}
		return null;
	}

	/**
	 * Returns appropriate SockIO object given
	 * string cache key.
	 *
	 * @param key hashcode for cache key
	 * @return SockIO obj connected to server
	 */
	public SockAndFragmentId getSockAndFragmentId( String key ) {
		return getSockAndFragmentId( key, null );
	}

	/**
	 * Returns appropriate SockAndFragmentId object
	 * containing the SockIO object given string cache
	 * key and optional hashcode, and the fragment id
	 * of the fragment containing the host.
	 *
	 * Trys to get SockIO from pool.  Fails over
	 * to additional pools in event of server failure.
	 *
	 * @param key hashcode for cache key
	 * @param hashCode if not null, then the int hashcode to use
	 * @return SockAndFragmentId obj connected to server
	 */
	public SockAndFragmentId getSockAndFragmentId( String key, Integer hashCode ) {

		if ( log.isDebugEnabled() )
			log.debug( "cache socket pick " + key + " " + hashCode );

		if ( !this.initialized ) {
			log.error( "attempting to get SockIO from uninitialized pool!" );
			return null;
		}

		// if no servers return null
		if ( config != null && config.getFragmentCount() == 0 )
			return null;

		// if only one server, return it
		if ( config != null && config.getFragmentCount() == 1 ) {

			SockIO sock = getConnection( config.getFragment( 0 ).getAddress() );

			if ( sock != null && sock.isConnected() ) {
				if ( options.aliveCheck ) {
					if ( !sock.isAlive() ) {
						sock.close();
						try { sock.trueClose(); } catch ( IOException ioe ) { log.error( "failed to close dead socket" ); }
						sock = null;
					}
				}
			}
			else {
				if ( sock != null ) {
					deadPool.put( sock, ZERO );
					sock = null;
				}
			}

			return new SockAndFragmentId(
				sock, config.getFragment(0).getId(), 1);
		}

		// from here on, we are working w/ multiple servers
		// keep trying different servers until we find one
		// making sure we only try each server one time
		Set<Fragment> tryServers = new HashSet<Fragment>( config.getFragmentList() );

		// get initial bucket
		long bucket = getBucket( key, hashCode );
		Fragment fragment = config.getFragment( (int)bucket );
		int fragmentNum = ((int)bucket) + 1;

		while ( !tryServers.isEmpty() ) {

			// try to get socket from bucket
			String server = fragment.getAddress();
			SockIO sock = getConnection( server );

			if ( log.isDebugEnabled() )
				log.debug( "cache choose " + server + " for " + key );

			if ( sock != null && sock.isConnected() ) {
				if ( options.aliveCheck ) {
					if ( sock.isAlive() ) {
						return new SockAndFragmentId(sock, fragment.getId(), fragmentNum);
					}
					else {
						sock.close();
						try { sock.trueClose(); } catch ( IOException ioe ) { log.error( "failed to close dead socket" ); }
						sock = null;
					}
				}
				else {
					return new SockAndFragmentId(sock, fragment.getId(), fragmentNum);
				}
			}
			else {
				if ( sock != null ) {
					deadPool.put( sock, ZERO );
					sock = null;
				}
			}

			// if we do not want to failover, then bail here
			if ( !options.failover )
				return null;

			// log that we tried
			tryServers.remove( fragment );

			if ( tryServers.isEmpty() )
				break;

			// if we failed to get a socket from this server
			// then we try again by adding an incrementer to the
			// current key and then rehashing
			int rehashTries = 0;
			while ( !tryServers.contains( fragment ) ) {

				String newKey = String.format( "%s%s", rehashTries, key );
				if ( log.isDebugEnabled() )
					log.debug( "rehashing with: " + newKey );

				bucket = getBucket( newKey, null );
				fragment = config.getFragment( (int)bucket );

				rehashTries++;
			}
		}

		return null;
	}

	/**
	 * Returns a SockIO object from the pool for the passed in host.
	 *
	 * Meant to be called from a more intelligent method<br/>
	 * which handles choosing appropriate server<br/>
	 * and failover.
	 *
	 * @param host host from which to retrieve object
	 * @return SockIO object or null if fail to retrieve one
	 */
	public SockIO getConnection( String host ) {

		if ( !this.initialized ) {
			log.error( "attempting to get SockIO from uninitialized pool!" );
			return null;
		}

		if ( host == null )
			return null;

		synchronized( this ) {

			// if we have items in the pool
			// then we can return it
			if ( availPool != null && !availPool.isEmpty() ) {

				// take first connected socket
				Map<SockIO,Long> aSockets = availPool.get( host );

				if ( aSockets != null && !aSockets.isEmpty() ) {

					for ( Iterator<SockIO> i = aSockets.keySet().iterator(); i.hasNext(); ) {
						SockIO socket = i.next();

						if ( socket.isConnected() ) {
							if ( log.isDebugEnabled() )
								log.debug( "++++ moving socket for host (" + host + ") to busy pool ... socket: " + socket );

							// remove from avail pool
							i.remove();

							// add to busy pool
							addSocketToPool( busyPool, host, socket );

							// return socket
							return socket;
						}
						else {
							// add to deadpool for later reaping
							deadPool.put( socket, ZERO );

							// remove from avail pool
							i.remove();
						}
					}
				}
			}
		}

		// create one socket -- let the maint thread take care of creating more
		SockIO socket = createSocket( host );
		if ( socket != null ) {
			synchronized( this ) {
				addSocketToPool( busyPool, host, socket );
			}
		}

		return socket;
	}

	/**
	 * Adds a socket to a given pool for the given host.
	 * THIS METHOD IS NOT THREADSAFE, SO BE CAREFUL WHEN USING!
	 *
	 * Internal utility method.
	 *
	 * @param pool pool to add to
	 * @param host host this socket is connected to
	 * @param socket socket to add
	 */
	protected void addSocketToPool( Map<String,Map<SockIO,Long>> pool, String host, SockIO socket ) {

		if ( pool.containsKey( host ) ) {
			Map<SockIO,Long> sockets = pool.get( host );

			if ( sockets != null ) {
				sockets.put( socket, new Long( System.currentTimeMillis() ) );
				return;
			}
		}

		Map<SockIO,Long> sockets =
			new IdentityHashMap<SockIO,Long>();

		sockets.put( socket, new Long( System.currentTimeMillis() ) );
		pool.put( host, sockets );
	}

	/**
	 * Removes a socket from specified pool for host.
	 * THIS METHOD IS NOT THREADSAFE, SO BE CAREFUL WHEN USING!
	 *
	 * Internal utility method.
	 *
	 * @param pool pool to remove from
	 * @param host host pool
	 * @param socket socket to remove
	 */
	protected void removeSocketFromPool( Map<String,Map<SockIO,Long>> pool, String host, SockIO socket ) {
		if ( pool.containsKey( host ) ) {
			Map<SockIO,Long> sockets = pool.get( host );
			if ( sockets != null )
				sockets.remove( socket );
		}
	}

	/**
	 * Closes and removes all sockets from specified pool for host.
	 * THIS METHOD IS NOT THREADSAFE, SO BE CAREFUL WHEN USING!
	 *
	 * Internal utility method.
	 *
	 * @param pool pool to clear
	 * @param host host to clear
	 */
	protected void clearHostFromPool( Map<String,Map<SockIO,Long>> pool, String host ) {

		if ( pool.containsKey( host ) ) {
			Map<SockIO,Long> sockets = pool.get( host );

			if ( sockets != null && sockets.size() > 0 ) {
				for ( Iterator<SockIO> i = sockets.keySet().iterator(); i.hasNext(); ) {
					SockIO socket = i.next();
					try {
						socket.trueClose();
					}
					catch ( IOException ioe ) {
						log.error( "++++ failed to close socket: " + ioe.getMessage() );
					}

					i.remove();
					socket = null;
				}
			}
		}
	}

	/**
	 * Checks a SockIO object in with the pool.
	 *
	 * This will remove SocketIO from busy pool, and optionally<br/>
	 * add to avail pool.
	 *
	 * @param socket socket to return
	 * @param addToAvail add to avail pool if true
	 */
	private void checkIn( SockIO socket, boolean addToAvail ) {

		String host = socket.getHost();
		if ( log.isDebugEnabled() )
			log.debug( "++++ calling check-in on socket: " + socket.toString() + " for host: " + host );

		synchronized( this ) {
			// remove from the busy pool
			if ( log.isDebugEnabled() )
				log.debug( "++++ removing socket (" + socket.toString() + ") from busy pool for host: " + host );
			removeSocketFromPool( busyPool, host, socket );

			if ( socket.isConnected() && addToAvail ) {
				// add to avail pool
				if ( log.isDebugEnabled() )
					log.debug( "++++ returning socket (" + socket.toString() + " to avail pool for host: " + host );
				addSocketToPool( availPool, host, socket );
			}
			else {
				deadPool.put( socket, ZERO );
				socket = null;
			}
		}
	}

	/**
	 * Returns a socket to the avail pool.
	 *
	 * This is called from SockIO.close().  Calling this method<br/>
	 * directly without closing the SockIO object first<br/>
	 * will cause an IOException to be thrown.
	 *
	 * @param socket socket to return
	 */
	private void checkIn( SockIO socket ) {
		checkIn( socket, true );
	}

	/**
	 * Closes all sockets in the passed in pool.
	 *
	 * Internal utility method.
	 *
	 * @param pool pool to close
	 */
	protected void closePool( Map<String,Map<SockIO,Long>> pool ) {
		 for ( Iterator<String> i = pool.keySet().iterator(); i.hasNext(); ) {
			 String host = i.next();
			 Map<SockIO,Long> sockets = pool.get( host );

			 for ( Iterator<SockIO> j = sockets.keySet().iterator(); j.hasNext(); ) {
				 SockIO socket = j.next();

				 try {
					 socket.trueClose();
				 }
				 catch ( IOException ioe ) {
					 log.error( "++++ failed to trueClose socket: " + socket.toString() + " for host: " + host );
				 }

				 j.remove();
				 socket = null;
			 }
		 }
	}

	/**
	 * Shuts down the pool.
	 *
	 * Cleanly closes all sockets.<br/>
	 * Stops the maint thread.<br/>
	 * Nulls out all internal maps<br/>
	 */
	public void shutDown() {
		synchronized( this ) {
			if ( log.isDebugEnabled() )
				log.debug( "++++ SockIOPool shutting down..." );

			if ( maintThread != null && maintThread.isRunning() ) {
				// stop the main thread
				stopMaintThread();

				// wait for the thread to finish
				while ( maintThread.isRunning() ) {
					if ( log.isDebugEnabled() )
						log.debug( "++++ waiting for main thread to finish run +++" );
					try { Thread.sleep( 500 ); } catch ( Exception ex ) { }
				}
			}

			if ( log.isDebugEnabled() )
				log.debug( "++++ closing all internal pools." );
			closePool( availPool );
			closePool( busyPool );
			availPool         = null;
			busyPool          = null;
			hostDeadDur       = null;
			hostDead          = null;
			maintThread       = null;
			initialized       = false;
			if ( log.isDebugEnabled() )
				log.debug( "++++ SockIOPool finished shutting down." );
		}
	}

	/**
	 * Starts the maintenance thread.
	 *
	 * This thread will manage the size of the active pool<br/>
	 * as well as move any closed, but not checked in sockets<br/>
	 * back to the available pool.
	 */
	protected void startMaintThread() {

		if ( maintThread != null ) {

			if ( maintThread.isRunning() ) {
				log.error( "main thread already running" );
			}
			else {
				maintThread.start();
			}
		}
		else {
			maintThread = new MaintThread( this );
			maintThread.setInterval( this.options.maintSleep );
			maintThread.start();
		}
	}

	/**
	 * Stops the maintenance thread.
	 */
	protected void stopMaintThread() {
		if ( maintThread != null && maintThread.isRunning() )
			maintThread.stopThread();
	}

	/**
	 * Runs self maintenance on all internal pools.
	 *
	 * This is typically called by the maintenance thread to manage pool size.
	 */
	protected void selfMaint() {
		if ( log.isDebugEnabled() )
			log.debug( "++++ Starting self maintenance...." );

		// go through avail sockets and create sockets
		// as needed to maintain pool settings
		Map<String,Integer> needSockets =
			new HashMap<String,Integer>();

		synchronized( this ) {
			// find out how many to create
			for ( Iterator<String> i = availPool.keySet().iterator(); i.hasNext(); ) {
				String host              = i.next();
				Map<SockIO,Long> sockets = availPool.get( host );

				if ( log.isDebugEnabled() )
					log.debug( "++++ Size of avail pool for host (" + host + ") = " + sockets.size() );

				// if pool is too small (n < minSpare)
				if ( sockets.size() < options.minConn ) {
					// need to create new sockets
					int need = options.minConn - sockets.size();
					needSockets.put( host, need );
				}
			}
		}

		// now create
		Map<String,Set<SockIO>> newSockets =
			new HashMap<String,Set<SockIO>>();

		for ( String host : needSockets.keySet() ) {
			Integer need = needSockets.get( host );

			if ( log.isDebugEnabled() )
				log.debug( "++++ Need to create " + need + " new sockets for pool for host: " + host );

			Set<SockIO> newSock = new HashSet<SockIO>( need );
			for ( int j = 0; j < need; j++ ) {
				SockIO socket = createSocket( host );

				if ( socket == null )
					break;

				newSock.add( socket );
			}

			newSockets.put( host, newSock );
		}

		// synchronize to add and remove to/from avail pool
		// as well as clean up the busy pool (no point in releasing
		// lock here as should be quick to pool adjust and no
		// blocking ops here)
		synchronized( this ) {
			for ( String host : newSockets.keySet() ) {
				Set<SockIO> sockets = newSockets.get( host );
				for ( SockIO socket : sockets )
					addSocketToPool( availPool, host, socket );
			}

			for ( Iterator<String> i = availPool.keySet().iterator(); i.hasNext(); ) {
				String host              = i.next();
				Map<SockIO,Long> sockets = availPool.get( host );
				if ( log.isDebugEnabled() )
					log.debug( "++++ Size of avail pool for host (" + host + ") = " + sockets.size() );

				if ( sockets.size() > options.maxConn ) {
					// need to close down some sockets
					int diff        = sockets.size() - options.maxConn;
					int needToClose = (diff <= poolMultiplier)
						? diff
						: (diff) / poolMultiplier;

					if ( log.isDebugEnabled() )
						log.debug( "++++ need to remove " + needToClose + " spare sockets for pool for host: " + host );

					for ( Iterator<SockIO> j = sockets.keySet().iterator(); j.hasNext(); ) {
						if ( needToClose <= 0 )
							break;

						// remove stale entries
						SockIO socket = j.next();
						long expire   = sockets.get( socket ).longValue();

						// if past idle time
						// then close socket
						// and remove from pool
						if ( (expire + options.maxIdle) < System.currentTimeMillis() ) {
							if ( log.isDebugEnabled() )
								log.debug( "+++ removing stale entry from pool as it is past its idle timeout and pool is over max spare" );

							// remove from the availPool
							deadPool.put( socket, ZERO );
							j.remove();
							needToClose--;
						}
					}
				}
			}

			// go through busy sockets and destroy sockets
			// as needed to maintain pool settings
			for ( Iterator<String> i = busyPool.keySet().iterator(); i.hasNext(); ) {

				String host              = i.next();
				Map<SockIO,Long> sockets = busyPool.get( host );

				if ( log.isDebugEnabled() )
					log.debug( "++++ Size of busy pool for host (" + host + ")  = " + sockets.size() );

				// loop through all connections and check to see if we have any hung connections
				for ( Iterator<SockIO> j = sockets.keySet().iterator(); j.hasNext(); ) {
					// remove stale entries
					SockIO socket = j.next();
					long hungTime = sockets.get( socket ).longValue();

					// if past max busy time
					// then close socket
					// and remove from pool
					if ( (hungTime + options.maxBusyTime) < System.currentTimeMillis() ) {
						log.error( "+++ removing potentially hung connection from busy pool ... socket in pool for " + (System.currentTimeMillis() - hungTime) + "ms" );

						// remove from the busy pool
						deadPool.put( socket, ZERO );
						j.remove();
					}
				}
			}
		}

		// finally clean out the deadPool
		Set<SockIO> toClose;
		synchronized( deadPool ) {
			toClose  = deadPool.keySet();
			deadPool = new IdentityHashMap<SockIO,Integer>();
		}

		for ( SockIO socket : toClose ) {
			try {
				socket.trueClose( false );
			}
			catch ( Exception ex ) {
				log.error( "++++ failed to close SockIO obj from deadPool" );
				log.error( ex.getMessage(), ex );
			}

			socket = null;
		}

		if ( log.isDebugEnabled() )
			log.debug( "+++ ending self maintenance." );
	}

	/**
	 * Class which extends thread and handles maintenance of the pool.
	 *
	 * @author greg whalin <greg@meetup.com>
	 * @version 1.5
	 */
	protected static class MaintThread extends Thread {

		// logger
		private static Logger log =
			Logger.getLogger( MaintThread.class.getName() );

		private SockIOPool pool;
		private long interval      = 1000 * 3; // every 3 seconds
		private boolean stopThread = false;
		private boolean running;

		protected MaintThread( SockIOPool pool ) {
			this.pool = pool;
			this.setDaemon( true );
			this.setName( "MaintThread" );
		}

		public void setInterval( long interval ) { this.interval = interval; }

		public boolean isRunning() {
			return this.running;
		}

		/**
		 * sets stop variable
		 * and interupts any wait
		 */
		public void stopThread() {
			this.stopThread = true;
			this.interrupt();
		}

		/**
		 * Start the thread.
		 */
		public void run() {
			this.running = true;

			while ( !this.stopThread ) {
				try {
					Thread.sleep( interval );

					// if pool is initialized, then
					// run the maintenance method on itself
					if ( pool.isInitialized() )
						pool.selfMaint();

				}
				catch ( Exception e ) {
					break;
				}
			}

			this.running = false;
		}
	}

	/**
	 * MemCached client for Java, utility class for Socket IO.
	 *
	 * This class is a wrapper around a Socket and its streams.
	 *
	 * @author greg whalin <greg@meetup.com>
	 * @author Richard 'toast' Russo <russor@msoe.edu>
	 * @version 1.5
	 */
	public static class SockIO implements LineInputStream {

		// logger
		private static Logger log =
			Logger.getLogger( SockIO.class.getName() );

		// pool
		private SockIOPool pool;

		// data
		private String host;
		private Socket sock;

		private DataInputStream in;
		private BufferedOutputStream out;

		/**
		 * creates a new SockIO object wrapping a socket
		 * connection to host:port, and its input and output streams
		 *
		 * @param pool Pool this object is tied to
		 * @param host host to connect to
		 * @param port port to connect to
		 * @param timeout int ms to block on data for read
		 * @param connectTimeout timeout (in ms) for initial connection
		 * @param noDelay TCP NODELAY option?
		 * @throws IOException if an io error occurrs when creating socket
		 * @throws UnknownHostException if hostname is invalid
		 */
		public SockIO( SockIOPool pool, String host, int port, int timeout, int connectTimeout, boolean noDelay ) throws IOException, UnknownHostException {

			this.pool = pool;

			// get a socket channel
			sock = getSocket( host, port, connectTimeout );

			if ( timeout >= 0 )
				sock.setSoTimeout( timeout );

			// testing only
			sock.setTcpNoDelay( noDelay );

			// wrap streams
			in  = new DataInputStream( new BufferedInputStream( sock.getInputStream() ) );
			out = new BufferedOutputStream( sock.getOutputStream() );

			this.host = host + ":" + port;
		}

		/**
		 * creates a new SockIO object wrapping a socket
		 * connection to host:port, and its input and output streams
		 *
		 * @param host hostname:port
		 * @param timeout read timeout value for connected socket
		 * @param connectTimeout timeout for initial connections
		 * @param noDelay TCP NODELAY option?
		 * @throws IOException if an io error occurrs when creating socket
		 * @throws UnknownHostException if hostname is invalid
		 */
		public SockIO( SockIOPool pool, String host, int timeout, int connectTimeout, boolean noDelay ) throws IOException, UnknownHostException {

			this.pool = pool;

			String[] ip = host.split(":");

			// get socket: default is to use non-blocking connect
			sock = getSocket( ip[ 0 ], Integer.parseInt( ip[ 1 ] ), connectTimeout );

			if ( timeout >= 0 )
				this.sock.setSoTimeout( timeout );

			// testing only
			sock.setTcpNoDelay( noDelay );

			// wrap streams
			in   = new DataInputStream( new BufferedInputStream( sock.getInputStream() ) );
			out  = new BufferedOutputStream( sock.getOutputStream() );

			this.host = host;
		}

		/**
		 * Method which gets a connection from SocketChannel.
		 *
		 * @param host host to establish connection to
		 * @param port port on that host
		 * @param timeout connection timeout in ms
		 *
		 * @return connected socket
		 * @throws IOException if errors connecting or if connection times out
		 */
		protected static Socket getSocket( String host, int port, int timeout ) throws IOException {
			SocketChannel sock = SocketChannel.open();
			sock.socket().connect( new InetSocketAddress( host, port ), timeout );
			return sock.socket();
		}

		/**
		 * Lets caller get access to underlying channel.
		 *
		 * @return the backing SocketChannel
		 */
		public SocketChannel getChannel() { return sock.getChannel(); }

		/**
		 * returns the host this socket is connected to
		 *
		 * @return String representation of host (hostname:port)
		 */
		public String getHost() { return this.host; }

		/**
		 * closes socket and all streams connected to it
		 *
		 * @throws IOException if fails to close streams or socket
		 */
		public void trueClose() throws IOException {
			trueClose( true );
		}

		/**
		 * closes socket and all streams connected to it
		 *
		 * @throws IOException if fails to close streams or socket
		 */
		public void trueClose( boolean addToDeadPool ) throws IOException {
			if ( log.isDebugEnabled() )
				log.debug( "++++ Closing socket for real: " + toString() );

			boolean err = false;
			StringBuilder errMsg = new StringBuilder();

			if ( in != null ) {
				try {
					in.close();
				}
				catch( IOException ioe ) {
					log.error( "++++ error closing input stream for socket: " + toString() + " for host: " + getHost() );
					log.error( ioe.getMessage(), ioe );
					errMsg.append( "++++ error closing input stream for socket: " + toString() + " for host: " + getHost() + "\n" );
					errMsg.append( ioe.getMessage() );
					err = true;
				}
			}

			if ( out != null ) {
				try {
					out.close();
				}
				catch ( IOException ioe ) {
					log.error( "++++ error closing output stream for socket: " + toString() + " for host: " + getHost() );
					log.error( ioe.getMessage(), ioe );
					errMsg.append( "++++ error closing output stream for socket: " + toString() + " for host: " + getHost() + "\n" );
					errMsg.append( ioe.getMessage() );
					err = true;
				}
			}

			if ( sock != null ) {
				try {
					sock.close();
				}
				catch ( IOException ioe ) {
					log.error( "++++ error closing socket: " + toString() + " for host: " + getHost() );
					log.error( ioe.getMessage(), ioe );
					errMsg.append( "++++ error closing socket: " + toString() + " for host: " + getHost() + "\n" );
					errMsg.append( ioe.getMessage() );
					err = true;
				}
			}

			// check in to pool
			if ( addToDeadPool && sock != null )
				pool.checkIn( this, false );

			in = null;
			out = null;
			sock = null;

			if ( err )
				throw new IOException( errMsg.toString() );
		}

		/**
		 * sets closed flag and checks in to connection pool
		 * but does not close connections
		 */
		void close() {
			// check in to pool
			if ( log.isDebugEnabled() )
				log.debug("++++ marking socket (" + this.toString() + ") as closed and available to return to avail pool");
			pool.checkIn( this );
		}

		/**
		 * checks if the connection is open
		 *
		 * @return true if connected
		 */
		boolean isConnected() {
			return ( sock != null && sock.isConnected() );
		}

		/*
		 * checks to see that the connection is still working
		 *
		 * @return true if still alive
		 */
		boolean isAlive() {

			if ( !isConnected() )
				return false;

			// try to talk to the server w/ a dumb query to ask its version
			try {
				this.write( "version\r\n".getBytes() );
				this.flush();
				String response = this.readLine();
			}
			catch ( IOException ex ) {
				return false;
			}

			return true;
		}

		/**
		 * reads a line
		 * intentionally not using the deprecated readLine method from DataInputStream
		 *
		 * @return String that was read in
		 * @throws IOException if io problems during read
		 */
		public String readLine() throws IOException {
			if ( sock == null || !sock.isConnected() ) {
				log.error( "++++ attempting to read from closed socket" );
				throw new IOException( "++++ attempting to read from closed socket" );
			}

			byte[] b = new byte[1];
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			boolean eol = false;

			while ( in.read( b, 0, 1 ) != -1 ) {

				if ( b[0] == 13 ) {
					eol = true;
				}
				else {
					if ( eol ) {
						if ( b[0] == 10 )
							break;

						eol = false;
					}
				}

				// cast byte into char array
				bos.write( b, 0, 1 );
			}

			if ( bos == null || bos.size() <= 0 ) {
				throw new IOException( "++++ Stream appears to be dead, so closing it down" );
			}

			// else return the string
			return bos.toString().trim();
		}

		/**
		 * reads up to end of line and returns nothing
		 *
		 * @throws IOException if io problems during read
		 */
		public void clearEOL() throws IOException {
			if ( sock == null || !sock.isConnected() ) {
				log.error( "++++ attempting to read from closed socket" );
				throw new IOException( "++++ attempting to read from closed socket" );
			}

			byte[] b = new byte[1];
			boolean eol = false;
			while ( in.read( b, 0, 1 ) != -1 ) {

				// only stop when we see
				// \r (13) followed by \n (10)
				if ( b[0] == 13 ) {
					eol = true;
					continue;
				}

				if ( eol ) {
					if ( b[0] == 10 )
						break;

					eol = false;
				}
			}
		}

		/**
		 * reads length bytes into the passed in byte array from dtream
		 *
		 * @param b byte array
		 * @throws IOException if io problems during read
		 */
		public int read( byte[] b ) throws IOException {
			if ( sock == null || !sock.isConnected() ) {
				log.error( "++++ attempting to read from closed socket" );
				throw new IOException( "++++ attempting to read from closed socket" );
			}

			int count = 0;
			while ( count < b.length ) {
				int cnt = in.read( b, count, (b.length - count) );
				count += cnt;
			}

			return count;
		}

		/**
		 * flushes output stream
		 *
		 * @throws IOException if io problems during read
		 */
		void flush() throws IOException {
			if ( sock == null || !sock.isConnected() ) {
				log.error( "++++ attempting to write to closed socket" );
				throw new IOException( "++++ attempting to write to closed socket" );
			}
			out.flush();
		}

		/**
		 * writes a byte array to the output stream
		 *
		 * @param b byte array to write
		 * @throws IOException if an io error happens
		 */
		void write( byte[] b ) throws IOException {
			if ( sock == null || !sock.isConnected() ) {
				log.error( "++++ attempting to write to closed socket" );
				throw new IOException( "++++ attempting to write to closed socket" );
			}
			out.write( b );
		}

		/**
		 * use the sockets hashcode for this object
		 * so we can key off of SockIOs
		 *
		 * @return int hashcode
		 */
		public int hashCode() {
			return ( sock == null ) ? 0 : sock.hashCode();
		}

		/**
		 * returns the string representation of this socket
		 *
		 * @return string
		 */
		public String toString() {
			return ( sock == null ) ? "" : sock.toString();
		}

		/**
		 * Hack to reap any leaking children.
		 */
		protected void finalize() throws Throwable {
			try {
				if ( sock != null ) {
					log.error( "++++ closing potentially leaked socket in finalize" );
					sock.close();
					sock = null;
				}
			}
			catch ( Throwable t ) {
				log.error( t.getMessage(), t );
			}
			finally {
				super.finalize();
			}
		}
	}

	public static class SockAndFragmentId {
		// The socket.
		private SockIO sock;
		// The fragment id of the fragment that the socket belongs to.
		private int fragmentId;
		// The fragment number (index in the fragments array + 1).
		private int fragmentNum;

		SockAndFragmentId(SockIO sock, int id, int fragmentNum) {
			this.sock = sock;
			this.fragmentId = id;
			this.fragmentNum = fragmentNum;
		}

		public SockIO sock() {
			return sock;
		}

		public int fragmentId() {
			return fragmentId;
		}

		public int fragmentNum() {
			return fragmentNum;
		}
	}
}

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
 * @author Greg Whalin <greg@meetup.com>
 */
package edu.usc.cs550.rejig.client.test;

import edu.usc.cs550.rejig.client.*;
import edu.usc.cs550.rejig.interfaces.Fragment;
import edu.usc.cs550.rejig.interfaces.RejigConfig;

import java.util.*;

public class MemcachedTest {

	// store results from threads
	private static Hashtable<Integer,StringBuilder> threadInfo =
		new Hashtable<Integer,StringBuilder>();

	/**
	 * This runs through some simple tests of the MemcacheClient.
	 *
	 * Command line args:
	 * args[0] = number of threads to spawn
	 * args[1] = number of runs per thread
	 * args[2] = size of object to store
	 *
	 * @param args the command line arguments
	 */
	public static void main(String[] args) {

		String[] servers = { "localhost:11211", "localhost:11212" };

		// initialize the pool options
		SockIOPool.SockIOPoolOptions options = new SockIOPool.SockIOPoolOptions();
		options.initConn = 5;
		options.minConn = 5;
		options.maxConn = 50;
		options.maintSleep = 30;
		options.nagle = false;

		int threads = Integer.parseInt(args[0]);
		int runs = Integer.parseInt(args[1]);
		int size = 1024 * Integer.parseInt(args[2]);	// how many kilobytes

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
		MemcachedClient mc = new MemcachedClient(
			null, new MockErrorHandler(),
			"test", configReader, options);
		mc.setCompressEnable( false );
		mc.setCompressThreshold(0);
		// get object to store
		int[] obj = new int[size];
		for (int i = 0; i < size; i++) {
			obj[i] = i;
		}

		String[] keys = new String[runs];
		for (int i = 0; i < runs; i++) {
			keys[i] = "test_key" + i;
		}

		for (int i = 0; i < threads; i++) {
			bench b = new bench(mc, runs, i, obj, keys);
			b.start();
		}

		int i = 0;
		while (i < threads) {
			if (threadInfo.containsKey(new Integer(i))) {
				System.out.println( threadInfo.get( new Integer( i ) ) );
				i++;
			}
			else {
				try {
					Thread.sleep(1000);
				}
				catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		mc.shutDown();
	}

	/**
	 * Test code per thread.
	 */
	private static class bench extends Thread {
		private int runs;
		private int threadNum;
		private int[] object;
		private String[] keys;
		private int size;
		private MemcachedClient mc;

		public bench(MemcachedClient client, int runs, int threadNum, int[] object, String[] keys) {
			this.runs = runs;
			this.threadNum = threadNum;
			this.object = object;
			this.keys = keys;
			this.size = object.length;
			this.mc = client;
		}

		public void run() {

			StringBuilder result = new StringBuilder();

			// time deletes
			long start = System.currentTimeMillis();
			for (int i = 0; i < runs; i++) {
				mc.delete(keys[i]);
			}
			long elapse = System.currentTimeMillis() - start;
			float avg = (float) elapse / runs;
			result.append("\nthread " + threadNum + ": runs: " + runs + " deletes of obj " + (size/1024) + "KB -- avg time per req " + avg + " ms (total: " + elapse + " ms)");

			// time stores
			start = System.currentTimeMillis();
			for (int i = 0; i < runs; i++) {
				mc.set(keys[i], object);
			}
			elapse = System.currentTimeMillis() - start;
			avg = (float) elapse / runs;
			result.append("\nthread " + threadNum + ": runs: " + runs + " stores of obj " + (size/1024) + "KB -- avg time per req " + avg + " ms (total: " + elapse + " ms)");

			start = System.currentTimeMillis();
			for (int i = 0; i < runs; i++) {
				mc.get(keys[i]);
			}
			elapse = System.currentTimeMillis() - start;
			avg = (float) elapse / runs;
			result.append("\nthread " + threadNum + ": runs: " + runs + " gets of obj " + (size/1024) + "KB -- avg time per req " + avg + " ms (total: " + elapse + " ms)");

			threadInfo.put(new Integer(threadNum), result);
		}
	}
}

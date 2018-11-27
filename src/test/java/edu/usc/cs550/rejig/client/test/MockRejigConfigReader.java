package edu.usc.cs550.rejig.client.test;

import edu.usc.cs550.rejig.client.configreader.RejigConfigReader;
import edu.usc.cs550.rejig.interfaces.RejigConfig;

class MockRejigConfigReader implements RejigConfigReader {
	private RejigConfig config;

	public RejigConfig getConfig() {
		return this.config;
	}

	public MockRejigConfigReader setConfig(RejigConfig config) {
		this.config = config;
		return this;
	}
}
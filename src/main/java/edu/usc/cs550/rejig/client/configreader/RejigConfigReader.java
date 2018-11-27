package edu.usc.cs550.rejig.client.configreader;

import edu.usc.cs550.rejig.interfaces.RejigConfig;

/**
 * Implement this interface to manage reading the RegjigConfig
 * object from a local, or remote store.
 */
public interface RejigConfigReader {
  RejigConfig getConfig();
}
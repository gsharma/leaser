package com.github.leaser;

/**
 * A simple interface to tie together core lifecycle operations applicable to stateful objects.
 */
public interface Lifecycle {

    // Get the identity of this instance
    String getIdentity();

    // Start this instance
    void start() throws Exception;

    // Stop this instance
    void stop() throws Exception;

    // Check if this instance is running
    boolean isRunning();

}

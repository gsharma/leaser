package com.github.leaser;

import java.util.UUID;

/**
 * A simple interface to tie together core lifecycle operations applicable to stateful objects.
 */
public interface Lifecycle {

    // Get the identity of this instance
    default UUID getIdentity() {
        return UUID.randomUUID();
    }

    // Start this instance
    void start() throws Exception;

    // Stop this instance
    void stop() throws Exception;

    // Check if this instance is running
    boolean isRunning();

}

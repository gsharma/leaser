package com.github.leaser;

import java.util.UUID;

/**
 * Simple resource object on which to acquire a lease.
 */
public class Resource {
    private final String id = UUID.randomUUID().toString();

    public String getId() {
        return id;
    }
}

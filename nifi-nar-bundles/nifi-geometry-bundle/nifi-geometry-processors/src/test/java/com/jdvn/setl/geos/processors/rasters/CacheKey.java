package com.jdvn.setl.geos.processors.rasters;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Key for the cache. Necessary to override the default String.equals() to utilize MessageDigest.isEquals() to prevent timing attacks.
 */
public class CacheKey {
    final String key;

    public CacheKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final CacheKey otherCacheKey = (CacheKey) o;
        return MessageDigest.isEqual(key.getBytes(StandardCharsets.UTF_8), otherCacheKey.key.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return "CacheKey{token ending in '..." + key.substring(key.length() - 6) + "'}";
    }
}
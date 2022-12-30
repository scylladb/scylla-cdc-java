package com.scylladb.cdc.connector.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author buntykumar
 * @version 1.0
 */

public class UtilityCache{

  private UtilityCache(){ throw new IllegalStateException("Private Util Construct"); }

  public static final Cache<String, Boolean> cache = CacheBuilder.newBuilder().maximumSize(1000)
      .expireAfterWrite(1, TimeUnit.HOURS).build();

    public static void put(String key, boolean value) {  cache.put(key, value);  }

    public static boolean get(String key) throws ExecutionException {  return cache.get(key, () -> false); }
}

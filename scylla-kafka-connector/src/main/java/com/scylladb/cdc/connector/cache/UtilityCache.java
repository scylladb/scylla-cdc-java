package com.scylladb.cdc.connector.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author buntykumar
 * @version 1.0
 */

public class UtilityCache{
    private static LoadingCache<String, Boolean> cache;

    public static void cacheBuild(){
        cache = CacheBuilder.newBuilder()
                .maximumSize(100)
                .expireAfterAccess(30, TimeUnit.HOURS)
                .build(new CacheLoader<>() {
                    @Override
                    public Boolean load(String s) throws Exception {
                        Map<String, Boolean> values = new HashMap<>();
                        values.put("update", false);
                        return false;
                    }
                });
    }
    public void put(String key, boolean value) {
        cache.put(key,value);
    }

    public boolean get(String key) throws ExecutionException {
      return cache.get(key);
    }
}

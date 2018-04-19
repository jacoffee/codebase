package com.jacoffee.codebase.cache;

import com.google.common.primitives.Longs;
import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import redis.clients.jedis.Jedis;
import java.security.MessageDigest;


/**

    BloomFilter based on redis
    1. use Bitmaps as the underlying bit array like what Guava dose
    2. choose the right k/m/n based on the bloom filter theory
    3. put: object ----> byte[] ------> hash_function[] ------> bitmaps
       as the bitmap size limitation: 2 ^ 32 && 512M so try to distribute the keys into multiple keys(block)

       i.e md5(object) -- first 2 character
       suffix = first2(md5(object)) % block_num
       xy_user_monitor_suffix

    4. get(mightContains): object ----> byte[] ------> hash_function[]

        boolean isThere = false
        for (hash_function value : hash_function[]) {
            //  getbit(value) == 1
            isThere |= getbit(value)
        }

    return isThere

    TODO: This is filter is not meant to be serializable especially in the distributed computation scenario
*/
public class RedisBloomFilter<T>  {

  private Jedis redis;
  private String redisKey;

  // M --> 0 ~ 2 ^ 32
  private int bitMapSize;
  private double falsePosistivePercentage;
  private int blockSize;

  // K ---> the following two is calculated
  private int numOfHashFunction;
  // N --->
  private long expectedInsertion;

  public RedisBloomFilter(
      Jedis redis, String redisKey,
      int bitMapSize, double falsePosistivePercentage, int blockSize, int numOfHashFunction, long expectedInsertion
  ) {
    this.redis = redis;
    this.redisKey = redisKey;
    this.bitMapSize = bitMapSize;
    this.falsePosistivePercentage = falsePosistivePercentage;
    this.blockSize = blockSize;
    this.numOfHashFunction = numOfHashFunction;
    this.expectedInsertion = expectedInsertion;
  }

  static int optimalNumOfBits(long expectedInsertion, double falsePosistivePercentage) {
    if (falsePosistivePercentage == 0) {
      falsePosistivePercentage = Double.MIN_VALUE;
    }

    long expectedNumOfBits =
        (long)(-expectedInsertion * Math.log(falsePosistivePercentage) / (Math.log(2) * Math.log(2)));

    return (expectedNumOfBits >= Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int)expectedNumOfBits;
  }


  static int optimalHashFunctionSize(int numOfBits, long expectedInsertion) {
    return (int) (Math.log(2) * (numOfBits / expectedInsertion));
  }

  private String generateRedisKeySuffix(String redisKey, String md5, int blockSize) {
    return String.format("%s_%s", redisKey, md5.hashCode() % blockSize);
  }

  public static String md5(byte[] input) throws Exception {
    byte[] originalBytes = MessageDigest.getInstance("MD5").digest(input);
    StringBuffer buffer = new StringBuffer();
    for (int i = 0; i < originalBytes.length; i++) {
      String str = Integer.toHexString(0xFF & originalBytes[i]);

      if (str.length() < 2) {
        buffer.append("0" + str);
      } else {
        buffer.append(str);
      }
    }

    return buffer.toString();
  }

  public static RedisBloomFilter create(
    Jedis redis, String redisKey, long expectedInsertion, double falsePosistivePercentage, int blockSize
  ) {
    //  128M
    // int bitMapSize = new Double(Math.pow(2, 30)).intValue();
    int bitMapSize = optimalNumOfBits(expectedInsertion, falsePosistivePercentage);
    int hashFunctionSize = optimalHashFunctionSize(bitMapSize, expectedInsertion);

    RedisBloomFilter redisBloomFilter =
      new RedisBloomFilter(
          redis, redisKey, bitMapSize,
          falsePosistivePercentage, blockSize, hashFunctionSize, expectedInsertion
      );

    return redisBloomFilter;
  }

  private long lowerEight(byte[] bytes) {
    return Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
  }

  private long upperEight(byte[] bytes) {
    return Longs.fromBytes(bytes[15], bytes[14], bytes[13], bytes[12], bytes[11], bytes[10], bytes[9], bytes[8]);
  }

  // whether the bitmaps has been changed
  public boolean put(T t, Funnel<T> funnel) throws Exception {
    byte[] bytes = Hashing.murmur3_128().hashObject(t, funnel).asBytes();
    long hash1 = lowerEight(bytes);
    long hash2 = upperEight(bytes);
    boolean bitsChanged = false;
    long combinedHash = hash1;
    for (int i = 0; i < numOfHashFunction; i++) {
      String finalKey = generateRedisKeySuffix(redisKey, md5(bytes), blockSize);
      long index = (combinedHash & Long.MAX_VALUE) % bitMapSize;
      bitsChanged |= (!redis.setbit(finalKey, index, true));
      combinedHash += hash2;
    }

    return bitsChanged;
  }

}

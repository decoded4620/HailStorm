package com.milli.core.distributed;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Enumeration;
import java.util.Optional;

/**
 * Distributed Unique Identifier Generator
 * Props to Twitter snowflake: https://github.com/twitter/snowflake/tree/snowflake-2010 for providing the
 * basic idea behind this.
 */
public class HailStorm {
  private static final int BITS_NODE = 10;
  private static final int BITS_EPOC = 42;
  private static final int BITS_SEQ = 12;

  // total bits that make up the ids
  private static final int BITS_TOTAL = BITS_NODE + BITS_EPOC + BITS_SEQ;

  private static final int MAX_NODE_ID = (int) (Math.pow(2, BITS_NODE) - 1);
  private static final int MAX_SEQ = (int) (Math.pow(2, BITS_SEQ) - 1);

  // Custom Epoch (January 1, 2018 Midnight UTC = 2018-01-01T00:00:00Z)
  private static final long CUSTOM_EPOCH = 1514764800000L;
  private static final String HEX_ENCODE = "%02X";

  // singleton instance of HailStorm
  private static HailStorm INST;

  // this can be configuration driven. You can assign ids to each node through configuration / deployment
  // scripts that should suffice. Max 1024 nodes. Otherwise, let Hailstorm do it using the mac address.
  private final int nodeId;
  private volatile long prevTs = -1L;
  private volatile long seq = 0L;

  /**
   * Node Id can be between 0 and max node id (based on number of bits set for the node id out of BITS_TOTAL
   * As a special case, this can be set to -1 specifically to have Hailstorm generate the ID. This is for compatability with
   * configuration systems in order to test both branches.
   *
   * @param nodeId an int.
   */
  private HailStorm(int nodeId) {
    if (nodeId == -1) {
      this.nodeId = createNodeId();
    } else {
      if (nodeId < 0 || nodeId > MAX_NODE_ID) {
        throw new IllegalArgumentException(String.format("NodeId must be between %d and %d", 0, MAX_NODE_ID));
      }
      this.nodeId = nodeId;
    }
  }

  /**
   * Default Constructor will generate its own id.
   */
  private HailStorm() {
    this.nodeId = createNodeId();
  }

  private static int createNodeId() {
    try {
      StringBuilder sb = new StringBuilder();
      Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
      while (networkInterfaces.hasMoreElements()) {
        NetworkInterface networkInterface = networkInterfaces.nextElement();
        Optional.ofNullable(networkInterface.getHardwareAddress()).ifPresent(mac -> {
          for (byte macPart : mac) {
            sb.append(String.format(HEX_ENCODE, macPart));
          }
        });
      }
      return sb.toString().hashCode() & MAX_NODE_ID;
    } catch (SocketException ex) {
      return (new SecureRandom().nextInt()) & MAX_NODE_ID;
    }
  }

  /**
   * Gets a timestamp adjusted by custom epoch time.
   *
   * @return a long
   */
  private static long ts() {
    return Instant.now().toEpochMilli() - CUSTOM_EPOCH;
  }

  /**
   * Singleton accessor
   *
   * @return instance of HailStorm
   */
  public static HailStorm getInstance() {
    if (INST == null) {
      INST = new HailStorm();
    }
    return INST;
  }

  /**
   * Singleton Accessor with custom id input allowance.
   *
   * @param id an int
   * @return the {@link HailStorm} singleton instance.
   */
  public static HailStorm getInstance(int id) {
    if (INST == null) {
      INST = new HailStorm(id);
    } else {
      throw new RuntimeException("Cannot create two instances of HailStorm, HailStorm with node id: " + INST.nodeId + " already exists");
    }

    return INST;
  }

  /**
   * Generate the next sequence identifier.
   *
   * @return a long value
   */
  public synchronized long generate() {
    long currTs = ts();

    if (currTs < prevTs) {
      throw new IllegalStateException("System Clock!");
    } else if (currTs == prevTs) {
      seq = (seq + 1) & MAX_SEQ;
      if (seq == 0) {
        // Sequence Exhausted, wait till next millisecond.
        currTs = waitOneMs(currTs);
      }
    } else {
      // reset seq to start with zero for the next millisecond
      seq = 0;
    }

    prevTs = currTs;

    // do some bit << shifting and | (or) magic on the bits to generate the id.
    return (currTs << (BITS_TOTAL - BITS_EPOC)) | (nodeId << (BITS_TOTAL - BITS_EPOC - BITS_NODE)) | seq;
  }

  /**
   * wait at least one ms after "prevTs"
   * @param ts a current ts (which may equal the previous)
   * @return the prev ts.
   */
  private long waitOneMs(long ts) {
    // using <= for absolute safety here.
    while (ts <= prevTs) {
      ts = ts();
    }
    return ts;
  }
}

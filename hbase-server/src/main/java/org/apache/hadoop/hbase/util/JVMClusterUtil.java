/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.util;

import java.io.InterruptedIOException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

/**
 * Utility used running a cluster all in the one JVM.
 */
@InterfaceAudience.Private
public class JVMClusterUtil {
  private static final Logger LOG = LoggerFactory.getLogger(JVMClusterUtil.class);

  /**
   * Datastructure to hold RegionServer Thread and RegionServer instance
   */
  public static class RegionServerThread extends Thread {
    private final HRegionServer regionServer;

    public RegionServerThread(final HRegionServer r, final int index) {
      super(r, "RS:" + index + ";" + r.getServerName().toShortString());
      this.regionServer = r;
    }

    /** @return the region server */
    public HRegionServer getRegionServer() {
      return this.regionServer;
    }

    /**
     * Block until the region server has come online, indicating it is ready
     * to be used.
     */
    public void waitForServerOnline() {
      // The server is marked online after the init method completes inside of
      // the HRS#run method.  HRS#init can fail for whatever region.  In those
      // cases, we'll jump out of the run without setting online flag.  Check
      // stopRequested so we don't wait here a flag that will never be flipped.
      regionServer.waitForServerOnline();
    }
  }

  /**
   * Creates a {@link RegionServerThread}.
   * Call 'start' on the returned thread to make it run.
   * @param c Configuration to use.
   * @param hrsc Class to create.
   * @param index Used distinguishing the object returned.
   * @throws IOException
   * @return Region server added.
   */
  public static JVMClusterUtil.RegionServerThread createRegionServerThread(final Configuration c,
      final Class<? extends HRegionServer> hrsc, final int index) throws IOException {
    HRegionServer server;
    try {
      Constructor<? extends HRegionServer> ctor = hrsc.getConstructor(Configuration.class);
      ctor.setAccessible(true);
      server = ctor.newInstance(c);
    } catch (InvocationTargetException ite) {
      Throwable target = ite.getTargetException();
      throw new RuntimeException("Failed construction of RegionServer: " +
        hrsc.toString() + ((target.getCause() != null)?
          target.getCause().getMessage(): ""), target);
    } catch (Exception e) {
      IOException ioe = new IOException();
      ioe.initCause(e);
      throw ioe;
    }
    return new JVMClusterUtil.RegionServerThread(server, index);
  }


  /**
   * Datastructure to hold Master Thread and Master instance
   */
  public static class MasterThread extends Thread {
    private final HMaster master;

    public MasterThread(final HMaster m, final int index) {
      super(m, "M:" + index + ";" + m.getServerName().toShortString());
      this.master = m;
    }

    /** @return the master */
    public HMaster getMaster() {
      return this.master;
    }
  }

  /**
   * Creates a {@link MasterThread}.
   * Call 'start' on the returned thread to make it run.
   * @param c Configuration to use.
   * @param hmc Class to create.
   * @param index Used distinguishing the object returned.
   * @throws IOException
   * @return Master added.
   */
  public static JVMClusterUtil.MasterThread createMasterThread(final Configuration c,
      final Class<? extends HMaster> hmc, final int index) throws IOException {
    HMaster server;
    try {
      server = hmc.getConstructor(Configuration.class).newInstance(c);
    } catch (InvocationTargetException ite) {
      Throwable target = ite.getTargetException();
      throw new RuntimeException("Failed construction of Master: " +
        hmc.toString() + ((target.getCause() != null)?
          target.getCause().getMessage(): ""), target);
    } catch (Exception e) {
      IOException ioe = new IOException();
      ioe.initCause(e);
      throw ioe;
    }
    return new JVMClusterUtil.MasterThread(server, index);
  }

  private static JVMClusterUtil.MasterThread findActiveMaster(
    List<JVMClusterUtil.MasterThread> masters) {
    for (JVMClusterUtil.MasterThread t : masters) {
      if (t.master.isActiveMaster()) {
        return t;
      }
    }

    return null;
  }

  /**
   * Start the cluster.  Waits until there is a primary master initialized
   * 返回的是一个Cluster的地址
   * and returns its address.
   * 这段代码是用于启动HBase集群中HMaster和HRegionServer线程的。它包含了启动过程的多个步骤，
   * 并确保在HMaster完全启动并初始化之后，才启动HRegionServer线程。现在我们来分析这段代码，特别是关于HMaster和HRegionServer线程的启动顺序
   * @param masters
   * @param regionservers
   * @return Address to use contacting primary master.
   */
  public static String startup(final List<JVMClusterUtil.MasterThread> masters,
      final List<JVMClusterUtil.RegionServerThread> regionservers) throws IOException {
    // Implementation note: This method relies on timed sleeps in a loop. It's not great, and
    // should probably be re-written to use actual synchronization objects, but it's ok for now
    // 实现说明：此方法依赖于循环中的定时休眠。它不太好，并且
    // 可能应该重写以使用实际的同步对象，但现在还可以

    Configuration configuration = null;

    // 需要等待一个Master 启动之后，才能启动
    if (masters == null || masters.isEmpty()) {
      return null;
    }

    // 这里的Master是一个集合列表，因为有可能有多个MAster线程
    for (JVMClusterUtil.MasterThread t : masters) {
      configuration = t.getMaster().getConfiguration();
      // Master start
      t.start();
    }

    // Wait for an active master
    //  having an active master before starting the region threads allows
    //  then to succeed on their connection to master
    // 等待活动主服务器
    // 在启动区域线程之前拥有活动主服务器允许
    // 然后成功连接到主服务器
    final int startTimeout = configuration != null ? Integer.parseInt(
        configuration.get("hbase.master.start.timeout.localHBaseCluster", "30000")) : 30000;
    // 等待能够返回Active Master
    waitForEvent(startTimeout, "active", () -> findActiveMaster(masters) != null);

    // 如果没有RegionServer，不跳过，创建regionServer进程
    if (regionservers != null) {
      System.out.println("RegionServer 大小" + regionservers.size());
      for (JVMClusterUtil.RegionServerThread t: regionservers) {
        t.start();
      }
    }

    // 等到master进程初始化后，我们返回cluster已经被初始化了
    // Wait for an active master to be initialized (implies being master)
    //  with this, when we return the cluster is complete
    final int initTimeout = configuration != null ? Integer.parseInt(
        configuration.get("hbase.master.init.timeout.localHBaseCluster", "200000")) : 200000;


    // 等待时间的触发，
    waitForEvent(initTimeout, "initialized", () -> {
      // 查找活跃的HMaster
        JVMClusterUtil.MasterThread t = findActiveMaster(masters);
        // master thread should never be null at this point, but let's keep the check anyway
      // Master 应该是活跃的，并且已经完成了初始化，
        return t != null && t.master.isInitialized();
      }
    );

    return findActiveMaster(masters).master.getServerName().toString();
  }

  /**
   * Utility method to wait some time for an event to occur, and then return control to the caller.
   * @param millis How long to wait, in milliseconds.
   * @param action The action that we are waiting for. Will be used in log message if the event
   *               does not occur.
   * @param check A Supplier that will be checked periodically to produce an updated true/false
   *              result indicating if the expected event has happened or not.
   * @throws InterruptedIOException If we are interrupted while waiting for the event.
   * @throws RuntimeException If we reach the specified timeout while waiting for the event.
   */
  private static void waitForEvent(long millis, String action, Supplier<Boolean> check)
      throws InterruptedIOException {
    long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);

    while (true) {
      if (check.get()) {
        return;
      }

      if (System.nanoTime() > end) {
        String msg = "Master not " + action + " after " + millis + "ms";
        Threads.printThreadInfo(System.out, "Thread dump because: " + msg);
        throw new RuntimeException(msg);
      }

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException().initCause(e);
      }
    }

  }

  /**
   * @param masters
   * @param regionservers
   */
  public static void shutdown(final List<MasterThread> masters,
      final List<RegionServerThread> regionservers) {
    LOG.debug("Shutting down HBase Cluster");
    if (masters != null) {
      // Do backups first.
      JVMClusterUtil.MasterThread activeMaster = null;
      for (JVMClusterUtil.MasterThread t : masters) {
        // Master was killed but could be still considered as active. Check first if it is stopped.
        if (!t.master.isStopped()) {
          if (!t.master.isActiveMaster()) {
            try {
              t.master.stopMaster();
            } catch (IOException e) {
              LOG.error("Exception occurred while stopping master", e);
            }
            LOG.info("Stopped backup Master {} is stopped: {}",
                t.master.hashCode(), t.master.isStopped());
          } else {
            if (activeMaster != null) {
              LOG.warn("Found more than 1 active master, hash {}", activeMaster.master.hashCode());
            }
            activeMaster = t;
            LOG.debug("Found active master hash={}, stopped={}",
                t.master.hashCode(), t.master.isStopped());
          }
        }
      }
      // Do active after.
      if (activeMaster != null) {
        try {
          activeMaster.master.shutdown();
        } catch (IOException e) {
          LOG.error("Exception occurred in HMaster.shutdown()", e);
        }
      }
    }
    boolean wasInterrupted = false;
    final long maxTime = System.currentTimeMillis() + 30 * 1000;
    if (regionservers != null) {
      // first try nicely.
      for (RegionServerThread t : regionservers) {
        t.getRegionServer().stop("Shutdown requested");
      }
      for (RegionServerThread t : regionservers) {
        long now = System.currentTimeMillis();
        if (t.isAlive() && !wasInterrupted && now < maxTime) {
          try {
            t.join(maxTime - now);
          } catch (InterruptedException e) {
            LOG.info("Got InterruptedException on shutdown - " +
                "not waiting anymore on region server ends", e);
            wasInterrupted = true; // someone wants us to speed up.
          }
        }
      }

      // Let's try to interrupt the remaining threads if any.
      for (int i = 0; i < 100; ++i) {
        boolean atLeastOneLiveServer = false;
        for (RegionServerThread t : regionservers) {
          if (t.isAlive()) {
            atLeastOneLiveServer = true;
            try {
              LOG.warn("RegionServerThreads remaining, give one more chance before interrupting");
              t.join(1000);
            } catch (InterruptedException e) {
              wasInterrupted = true;
            }
          }
        }
        if (!atLeastOneLiveServer) break;
        for (RegionServerThread t : regionservers) {
          if (t.isAlive()) {
            LOG.warn("RegionServerThreads taking too long to stop, interrupting; thread dump "  +
              "if > 3 attempts: i=" + i);
            if (i > 3) {
              Threads.printThreadInfo(System.out, "Thread dump " + t.getName());
            }
            t.interrupt();
          }
        }
      }
    }

    if (masters != null) {
      for (JVMClusterUtil.MasterThread t : masters) {
        while (t.master.isAlive() && !wasInterrupted) {
          try {
            // The below has been replaced to debug sometime hangs on end of
            // tests.
            // this.master.join():
            Threads.threadDumpingIsAlive(t.master.getThread());
          } catch(InterruptedException e) {
            LOG.info("Got InterruptedException on shutdown - " +
                "not waiting anymore on master ends", e);
            wasInterrupted = true;
          }
        }
      }
    }
    LOG.info("Shutdown of " +
      ((masters != null) ? masters.size() : "0") + " master(s) and " +
      ((regionservers != null) ? regionservers.size() : "0") +
      " regionserver(s) " + (wasInterrupted ? "interrupted" : "complete"));

    if (wasInterrupted){
      Thread.currentThread().interrupt();
    }
  }
}

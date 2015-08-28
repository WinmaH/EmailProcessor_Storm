package org.wso2.storm.benchmarks.emailprocessor.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by miyurud on 6/8/15.
 */

public class PerfStats {
    public long count = 0;
    public AtomicInteger atomicInt = new AtomicInteger(0);
    public int window_count = 0;
    public long totalLatency = 0;
    public long lastEventTime;
}
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.controller;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.controller.BrokerHeartbeatManager.BrokerHeartbeatState;
import org.apache.kafka.controller.BrokerHeartbeatManager.UsableBrokerIterator;
import org.apache.kafka.metadata.UsableBroker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(40)
public class BrokerHeartbeatManagerTest {
    private static BrokerHeartbeatManager newBrokerHeartbeatManager() {
        LogContext logContext = new LogContext();
        MockTime time = new MockTime(0, 1000000, 0);
        return new BrokerHeartbeatManager(logContext, time,
            TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testHasValidSession() {
        BrokerHeartbeatManager manager = newBrokerHeartbeatManager();
        MockTime time = (MockTime)  manager.time();
        assertFalse(manager.hasValidSession(0));
        manager.touch(0, false, 0);
        time.sleep(5);
        manager.touch(1, false, 0);
        manager.touch(2, false, 0);
        assertTrue(manager.hasValidSession(0));
        assertTrue(manager.hasValidSession(1));
        assertTrue(manager.hasValidSession(2));
        assertFalse(manager.hasValidSession(3));
        time.sleep(6);
        assertFalse(manager.hasValidSession(0));
        assertTrue(manager.hasValidSession(1));
        assertTrue(manager.hasValidSession(2));
        assertFalse(manager.hasValidSession(3));
        manager.remove(2);
        assertFalse(manager.hasValidSession(2));
        manager.remove(1);
        assertFalse(manager.hasValidSession(1));
    }

    @Test
    public void testMaybeFenceLeastRecentlyContacted() {
        BrokerHeartbeatManager manager = newBrokerHeartbeatManager();
        MockTime time = (MockTime)  manager.time();
        assertFalse(manager.hasValidSession(0));
        manager.touch(0, false, 0);
        time.sleep(5);
        manager.touch(1, false, 0);
        time.sleep(1);
        manager.touch(2, false, 0);

        Iterator<BrokerHeartbeatState> iter = manager.unfenced().iterator();
        assertEquals(0, iter.next().id());
        assertEquals(1, iter.next().id());
        assertEquals(2, iter.next().id());
        assertFalse(iter.hasNext());
        assertEquals(-1, manager.maybeFenceLeastRecentlyContacted());

        time.sleep(5);
        assertEquals(0, manager.maybeFenceLeastRecentlyContacted());
        assertEquals(-1, manager.maybeFenceLeastRecentlyContacted());
        iter = manager.unfenced().iterator();
        assertEquals(1, iter.next().id());
        assertEquals(2, iter.next().id());
        assertFalse(iter.hasNext());

        time.sleep(20);
        assertEquals(1, manager.maybeFenceLeastRecentlyContacted());
        assertEquals(2, manager.maybeFenceLeastRecentlyContacted());
        assertEquals(-1, manager.maybeFenceLeastRecentlyContacted());
        iter = manager.unfenced().iterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testNextCheckTimeNs() {
        BrokerHeartbeatManager manager = newBrokerHeartbeatManager();
        MockTime time = (MockTime)  manager.time();
        assertEquals(Long.MAX_VALUE, manager.nextCheckTimeNs());
        manager.touch(0, false, 0);
        time.sleep(1);
        manager.touch(1, false, 0);
        time.sleep(1);
        manager.touch(2, false, 0);
        time.sleep(1);
        manager.touch(3, false, 0);
        assertEquals(10_000_000, manager.nextCheckTimeNs());
        time.sleep(20);
        assertEquals(10_000_000, manager.nextCheckTimeNs());
        assertEquals(0, manager.maybeFenceLeastRecentlyContacted());
        assertEquals(11_000_000, manager.nextCheckTimeNs());
        assertEquals(1, manager.maybeFenceLeastRecentlyContacted());
        assertEquals(12_000_000, manager.nextCheckTimeNs());
    }

    @Test
    public void testMetadataOffsetComparator() {
        TreeSet<BrokerHeartbeatState> set =
            new TreeSet<>(BrokerHeartbeatManager.MetadataOffsetComparator.INSTANCE);
        BrokerHeartbeatState broker1 = new BrokerHeartbeatState(1);
        BrokerHeartbeatState broker2 = new BrokerHeartbeatState(2);
        BrokerHeartbeatState broker3 = new BrokerHeartbeatState(3);
        set.add(broker1);
        set.add(broker2);
        set.add(broker3);
        Iterator<BrokerHeartbeatState> iterator = set.iterator();
        assertEquals(broker1, iterator.next());
        assertEquals(broker2, iterator.next());
        assertEquals(broker3, iterator.next());
        assertFalse(iterator.hasNext());
        assertTrue(set.remove(broker1));
        assertTrue(set.remove(broker2));
        assertTrue(set.remove(broker3));
        assertTrue(set.isEmpty());
        broker1.metadataOffset = 800;
        broker2.metadataOffset = 400;
        broker3.metadataOffset = 100;
        set.add(broker1);
        set.add(broker2);
        set.add(broker3);
        iterator = set.iterator();
        assertEquals(broker3, iterator.next());
        assertEquals(broker2, iterator.next());
        assertEquals(broker1, iterator.next());
        assertFalse(iterator.hasNext());
    }

    private static Set<UsableBroker> usableBrokersToSet(BrokerHeartbeatManager manager) {
        Set<UsableBroker> brokers = new HashSet<>();
        for (Iterator<UsableBroker> iterator = new UsableBrokerIterator(
            manager.unfenced().iterator(),
            id -> id % 2 == 0 ? Optional.of("rack1") : Optional.of("rack2"));
             iterator.hasNext(); ) {
            brokers.add(iterator.next());
        }
        return brokers;
    }

    @Test
    public void testUsableBrokerIterator() {
        BrokerHeartbeatManager manager = newBrokerHeartbeatManager();
        assertEquals(Collections.emptySet(), usableBrokersToSet(manager));
        manager.touch(0, false, 100);
        manager.touch(1, false, 98);
        manager.touch(2, false, 100);
        manager.touch(3, false, 0);
        manager.touch(4, true, 0);
        assertEquals(98L, manager.lowestActiveOffset());
        Set<UsableBroker> expected = new HashSet<>();
        expected.add(new UsableBroker(0, Optional.of("rack1")));
        expected.add(new UsableBroker(1, Optional.of("rack2")));
        expected.add(new UsableBroker(2, Optional.of("rack1")));
        expected.add(new UsableBroker(3, Optional.of("rack2")));
        assertEquals(expected, usableBrokersToSet(manager));
        manager.beginBrokerShutDown(2);
        assertEquals(100L, manager.lowestActiveOffset());
        manager.beginBrokerShutDown(4);
        expected.remove(new UsableBroker(2, Optional.of("rack1")));
        assertEquals(expected, usableBrokersToSet(manager));
    }
}

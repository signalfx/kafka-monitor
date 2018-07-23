/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.services;


import static com.linkedin.kmf.common.Utils.ZK_CONNECTION_TIMEOUT_MS;
import static com.linkedin.kmf.common.Utils.ZK_SESSION_TIMEOUT_MS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Total;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.kmf.common.DefaultTopicSchema;
import com.linkedin.kmf.common.Utils;
import com.linkedin.kmf.consumer.BaseConsumerRecord;
import com.linkedin.kmf.consumer.KMBaseConsumer;
import com.linkedin.kmf.consumer.NewConsumer;
import com.linkedin.kmf.consumer.OldConsumer;
import com.linkedin.kmf.services.configs.ConsumeServiceConfig;

import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.utils.ZkUtils;

public class ConsumeService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(ConsumeService.class);
  private static final String METRIC_GROUP_NAME = "consume-service";
  private static final String[] NONOVERRIDABLE_PROPERTIES =
    new String[] {ConsumeServiceConfig.BOOTSTRAP_SERVERS_CONFIG,
      ConsumeServiceConfig.ZOOKEEPER_CONNECT_CONFIG};

  private final String _name;
  private final ConsumeMetrics _sensors;
  private final KMBaseConsumer _consumer;
  private final Thread _thread;
  private final int _latencyPercentileMaxMs;
  private final int _latencyPercentileGranularityMs;
  private final AtomicBoolean _running;
  private final int _latencySlaMs;
  private final String _zkConnect;
  private final String _topic;
  private final int _intervalPartitionLeaderExecutor;
  private final ScheduledExecutorService _handlePartitionLeaderInfoExecutor;
  private final Map<Integer, Broker> _partitionToBroker;

  public ConsumeService(Map<String, Object> props, String name) throws Exception {
    _name = name;
    Map consumerPropsOverride = props.containsKey(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG)
      ? (Map) props.get(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG) : new HashMap<>();
    ConsumeServiceConfig config = new ConsumeServiceConfig(props);
    _topic = config.getString(ConsumeServiceConfig.TOPIC_CONFIG);
    _zkConnect = config.getString(ConsumeServiceConfig.ZOOKEEPER_CONNECT_CONFIG);
    String brokerList = config.getString(ConsumeServiceConfig.BOOTSTRAP_SERVERS_CONFIG);
    String consumerClassName = config.getString(ConsumeServiceConfig.CONSUMER_CLASS_CONFIG);
    _latencySlaMs = config.getInt(ConsumeServiceConfig.LATENCY_SLA_MS_CONFIG);
    _latencyPercentileMaxMs = config.getInt(ConsumeServiceConfig.LATENCY_PERCENTILE_MAX_MS_CONFIG);
    _latencyPercentileGranularityMs = config.getInt(ConsumeServiceConfig.LATENCY_PERCENTILE_GRANULARITY_MS_CONFIG);
    _intervalPartitionLeaderExecutor = config.getInt(ConsumeServiceConfig.PARTITION_TO_LEADER_REFRESH_INTERVAL_MS_CONFIG);
    _running = new AtomicBoolean(false);
    _partitionToBroker = new ConcurrentHashMap<>();

    for (String property: NONOVERRIDABLE_PROPERTIES) {
      if (consumerPropsOverride.containsKey(property)) {
        throw new ConfigException("Override must not contain " + property + " config.");
      }
    }

    _handlePartitionLeaderInfoExecutor = Executors.newSingleThreadScheduledExecutor(new HandlePartitionLeaderInfoThreadFactory());

    Properties consumerProps = new Properties();

    // Assign default config. This has the lowest priority.
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "kmf-consumer");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kmf-consumer-group-" + new Random().nextInt());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    if (consumerClassName.equals(NewConsumer.class.getCanonicalName()) || consumerClassName.equals(NewConsumer.class.getSimpleName())) {
      consumerClassName = NewConsumer.class.getCanonicalName();
    } else if (consumerClassName.equals(OldConsumer.class.getCanonicalName()) || consumerClassName.equals(OldConsumer.class.getSimpleName())) {
      consumerClassName = OldConsumer.class.getCanonicalName();
      // The name/value of these configs are changed in the new consumer.
      consumerProps.put("auto.commit.enable", "false");
      consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "largest");
    }

    // Assign config specified for ConsumeService.
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    consumerProps.put("zookeeper.connect", _zkConnect);

    // Assign config specified for consumer. This has the highest priority.
    consumerProps.putAll(consumerPropsOverride);

    _consumer = (KMBaseConsumer) Class.forName(consumerClassName).getConstructor(String.class, Properties.class).newInstance(_topic, consumerProps);

    _thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          consume();
        } catch (Exception e) {
          LOG.error(_name + "/ConsumeService failed", e);
        }
      }
    }, _name + " consume-service");
    _thread.setDaemon(true);

    MetricConfig metricConfig = new MetricConfig().samples(60).timeWindow(1000, TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(JMX_PREFIX));
    Metrics metrics = new Metrics(metricConfig, reporters, new SystemTime());
    Map<String, String> tags = new HashMap<>();
    tags.put("name", _name);
    _sensors = new ConsumeMetrics(metrics, tags);
  }

  private void consume() throws Exception {
    // Delay 1 second to reduce the chance that consumer creates topic before TopicManagementService
    Thread.sleep(1000);

    Map<Integer, Long> nextIndexes = new HashMap<>();

    while (_running.get()) {
      BaseConsumerRecord record;
      try {
        record = _consumer.receive();
      } catch (Exception e) {
        _sensors._consumeError.record();
        LOG.warn(_name + "/ConsumeService failed to receive record", e);
        // Avoid busy while loop
        Thread.sleep(100);
        continue;
      }

      if (record == null)
        continue;

      GenericRecord avroRecord = Utils.genericRecordFromJson(record.value());
      if (avroRecord == null) {
        _sensors._consumeError.record();
        continue;
      }
      int partition = record.partition();
      long index = (Long) avroRecord.get(DefaultTopicSchema.INDEX_FIELD.name());
      long currMs = System.currentTimeMillis();
      long prevMs = (Long) avroRecord.get(DefaultTopicSchema.TIME_FIELD.name());
      _sensors._recordsConsumed.record();
      _sensors._bytesConsumed.record(record.value().length());
      _sensors._recordsDelay.record(currMs - prevMs);

      if (currMs - prevMs > _latencySlaMs)
        _sensors._recordsDelayed.record();

      if (index == -1L || !nextIndexes.containsKey(partition)) {
        nextIndexes.put(partition, -1L);
        continue;
      }

      long nextIndex = nextIndexes.get(partition);
      if (nextIndex == -1 || index == nextIndex) {
        nextIndexes.put(partition, index + 1);
      } else if (index < nextIndex) {
        _sensors._recordsDuplicated.record();
      } else if (index > nextIndex) {
        nextIndexes.put(partition, index + 1);
        _sensors._recordsLost.record(index - nextIndex);
      }

      Broker broker = _partitionToBroker.get(partition);
      Collection<EndPoint> endPoints = scala.collection.JavaConversions.asJavaCollection(broker.endPoints());
      for (EndPoint endpoint : endPoints) {
        String brokerUrl = endpoint.host() + ":" + endpoint.port();
        Sensor delay = _sensors.getOrCreateBrokerSensor(brokerUrl);
        delay.record(currMs - prevMs);
      }
    }
  }

  @Override
  public synchronized void start() {
    if (_running.compareAndSet(false, true)) {
      _thread.start();
      _handlePartitionLeaderInfoExecutor.scheduleWithFixedDelay(new PartitionLeaderInfoHandler(), 1000, _intervalPartitionLeaderExecutor, TimeUnit.MILLISECONDS);
      LOG.info("{}/ConsumeService started", _name);
    }
  }

  @Override
  public synchronized void stop() {
    if (_running.compareAndSet(true, false)) {
      try {
        _consumer.close();
      } catch (Exception e) {
        LOG.warn(_name + "/ConsumeService while trying to close consumer.", e);
      }
      LOG.info("{}/ConsumeService stopped", _name);
    }
  }

  @Override
  public void awaitShutdown() {
    LOG.info("{}/ConsumeService shutdown completed", _name);
  }

  @Override
  public boolean isRunning() {
    return _running.get() && _thread.isAlive();
  }

  /**
   * This should be periodically run to check for partition leadership assignments.
   */
  private class PartitionLeaderInfoHandler implements Runnable {

    @Override
    public void run() {
      ZkUtils zkUtils = ZkUtils.apply(_zkConnect, ZK_SESSION_TIMEOUT_MS, ZK_CONNECTION_TIMEOUT_MS, JaasUtils.isZkSecurityEnabled());
      scala.collection.mutable.ArrayBuffer<String> topicList = new scala.collection.mutable.ArrayBuffer<>();
      topicList.$plus$eq(_topic);
      scala.collection.Map<Object, scala.collection.Seq<Object>> partitionAssignments =
          zkUtils.getPartitionAssignmentForTopics(topicList).apply(_topic);
      scala.collection.Iterator<scala.Tuple2<Object, scala.collection.Seq<Object>>> it = partitionAssignments.iterator();
      while (it.hasNext()) {
        scala.Tuple2<Object, scala.collection.Seq<Object>> scalaTuple = it.next();
        Integer partition = (Integer) scalaTuple._1();
        scala.Option<Object> leaderOption = zkUtils.getLeaderForPartition(_topic, partition);
        Broker broker = zkUtils.getBrokerInfo((Integer) leaderOption.get()).get();
        _partitionToBroker.put(partition, broker);
      }
    }
  }

  private class ConsumeMetrics {
    private final Sensor _bytesConsumed;
    private final Sensor _consumeError;
    private final ConcurrentMap<String, Sensor> _delayPerBroker;
    private final Metrics _metrics;
    private final Sensor _recordsConsumed;
    private final Sensor _recordsDuplicated;
    private final Sensor _recordsLost;
    private final Sensor _recordsDelay;
    private final Sensor _recordsDelayed;
    private final Map<String, String> _tags;

    public ConsumeMetrics(final Metrics metrics, final Map<String, String> tags) {
      this._metrics = metrics;
      this._tags = tags;

      _delayPerBroker = new ConcurrentHashMap<>();

      _bytesConsumed = metrics.sensor("bytes-consumed");
      _bytesConsumed.add(new MetricName("bytes-consumed-rate", METRIC_GROUP_NAME, "The average number of bytes per second that are consumed", tags), new Rate());

      _consumeError = metrics.sensor("consume-error");
      _consumeError.add(new MetricName("consume-error-rate", METRIC_GROUP_NAME, "The average number of errors per second", tags), new Rate());
      _consumeError.add(new MetricName("consume-error-total", METRIC_GROUP_NAME, "The total number of errors", tags), new Total());

      _recordsConsumed = metrics.sensor("records-consumed");
      _recordsConsumed.add(new MetricName("records-consumed-rate", METRIC_GROUP_NAME, "The average number of records per second that are consumed", tags), new Rate());
      _recordsConsumed.add(new MetricName("records-consumed-total", METRIC_GROUP_NAME, "The total number of records that are consumed", tags), new Total());

      _recordsDuplicated = metrics.sensor("records-duplicated");
      _recordsDuplicated.add(new MetricName("records-duplicated-rate", METRIC_GROUP_NAME, "The average number of records per second that are duplicated", tags), new Rate());
      _recordsDuplicated.add(new MetricName("records-duplicated-total", METRIC_GROUP_NAME, "The total number of records that are duplicated", tags), new Total());

      _recordsLost = metrics.sensor("records-lost");
      _recordsLost.add(new MetricName("records-lost-rate", METRIC_GROUP_NAME, "The average number of records per second that are lost", tags), new Rate());
      _recordsLost.add(new MetricName("records-lost-total", METRIC_GROUP_NAME, "The total number of records that are lost", tags), new Total());

      _recordsDelayed = metrics.sensor("records-delayed");
      _recordsDelayed.add(new MetricName("records-delayed-rate", METRIC_GROUP_NAME, "The average number of records per second that are either lost or arrive after maximum allowed latency under SLA", tags), new Rate());
      _recordsDelayed.add(new MetricName("records-delayed-total", METRIC_GROUP_NAME, "The total number of records that are either lost or arrive after maximum allowed latency under SLA", tags), new Total());

      _recordsDelay = metrics.sensor("records-delay");
      _recordsDelay.add(new MetricName("records-delay-ms-avg", METRIC_GROUP_NAME, "The average latency of records from producer to consumer", tags), new Avg());
      _recordsDelay.add(new MetricName("records-delay-ms-max", METRIC_GROUP_NAME, "The maximum latency of records from producer to consumer", tags), new Max());

      // There are 2 extra buckets use for values smaller than 0.0 or larger than max, respectively.
      int bucketNum = _latencyPercentileMaxMs / _latencyPercentileGranularityMs + 2;
      int sizeInBytes = 4 * bucketNum;
      _recordsDelay.add(new Percentiles(sizeInBytes, _latencyPercentileMaxMs, Percentiles.BucketSizing.CONSTANT,
        new Percentile(new MetricName("records-delay-ms-99th", METRIC_GROUP_NAME, "The 99th percentile latency of records from producer to consumer", tags), 99.0),
        new Percentile(new MetricName("records-delay-ms-999th", METRIC_GROUP_NAME, "The 999th percentile latency of records from producer to consumer", tags), 99.9)));

      metrics.addMetric(new MetricName("consume-availability-avg", METRIC_GROUP_NAME, "The average consume availability", tags),
        new Measurable() {
          @Override
          public double measure(MetricConfig config, long now) {
            double recordsConsumedRate = metrics.metrics().get(metrics.metricName("records-consumed-rate", METRIC_GROUP_NAME, tags)).value();
            double recordsLostRate = metrics.metrics().get(metrics.metricName("records-lost-rate", METRIC_GROUP_NAME, tags)).value();
            double recordsDelayedRate = metrics.metrics().get(metrics.metricName("records-delayed-rate", METRIC_GROUP_NAME, tags)).value();

            if (new Double(recordsLostRate).isNaN())
              recordsLostRate = 0;
            if (new Double(recordsDelayedRate).isNaN())
              recordsDelayedRate = 0;

            double consumeAvailability = recordsConsumedRate + recordsLostRate > 0
              ? (recordsConsumedRate - recordsDelayedRate) / (recordsConsumedRate + recordsLostRate) : 0;

            return consumeAvailability;
          }
        }
      );
    }

    Sensor getOrCreateBrokerSensor(String endpoint) {
      if (_delayPerBroker.containsKey(endpoint)) {
        return _delayPerBroker.get(endpoint);
      }
      Sensor delayBrokerSensor = _metrics.sensor("delay-broker-" + endpoint);
      delayBrokerSensor.add(new MetricName("delay-ms-avg-broker-" + endpoint, METRIC_GROUP_NAME,
          "The average latency of records from producer to consumer for broker", _tags),  new Avg());
      _delayPerBroker.put(endpoint, delayBrokerSensor);
      return delayBrokerSensor;
    }
  }

  private class HandlePartitionLeaderInfoThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(Runnable r) {
      return new Thread(r, _name + "-consume-service-partition-leader-handler");
    }
  }

}


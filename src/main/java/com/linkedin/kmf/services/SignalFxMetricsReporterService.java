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

import static com.linkedin.kmf.common.Utils.getMBeanAttributeValues;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kmf.common.MbeanAttributeValue;
import com.linkedin.kmf.services.configs.SignalFxMetricsReporterServiceConfig;
import com.signalfx.codahale.metrics.SettableDoubleGauge;
import com.signalfx.codahale.reporter.MetricMetadata;
import com.signalfx.codahale.reporter.SignalFxReporter;
import com.signalfx.endpoint.SignalFxEndpoint;

public class SignalFxMetricsReporterService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(SignalFxMetricsReporterService.class);

  private final String _name;
  private final List<String> _metricNames;
  private final List<String> _dimensions;
  private final int _reportIntervalSec;
  private final ScheduledExecutorService _executor;
  private final MetricRegistry _metricRegistry;
  private final SignalFxReporter _signalfxReporter;
  private final String _signalfxUrl;
  private final String _signalfxToken;
  
  private MetricMetadata _metricMetadata;
  private Map<String, SettableDoubleGauge> _metricMap; 
  private Map<String, String> _dimensionsMap;
  
  public SignalFxMetricsReporterService(Map<String, Object> props, String name) throws Exception {
      
    SignalFxMetricsReporterServiceConfig config = new SignalFxMetricsReporterServiceConfig(props);
    
    _name = name;
    _metricNames = config.getList(SignalFxMetricsReporterServiceConfig.REPORT_METRICS_CONFIG);
    _dimensions = config.getList(SignalFxMetricsReporterServiceConfig.SIGNALFX_METRIC_DIMENSION);
    _reportIntervalSec = config.getInt(SignalFxMetricsReporterServiceConfig.REPORT_INTERVAL_SEC_CONFIG);
    _signalfxUrl = config.getString(SignalFxMetricsReporterServiceConfig.REPORT_SIGNALFX_URL);
    _signalfxToken = config.getString(SignalFxMetricsReporterServiceConfig.SIGNALFX_TOKEN);
    
    if (_signalfxToken.length() < 1) {
      throw new IllegalArgumentException("SignalFx token is not configured");
    }
    
    _executor = Executors.newSingleThreadScheduledExecutor();
    _metricRegistry = new MetricRegistry();
    _metricMap = new HashMap<String, SettableDoubleGauge>();
    _dimensionsMap = new HashMap<String, String>();
    setUpDimensionMap();
    
    SignalFxReporter.Builder sfxReportBuilder = new SignalFxReporter.Builder(
        _metricRegistry,
        _signalfxToken
    );
    if (_signalfxUrl.length() > 1) {
      SignalFxEndpoint signalFxEndpoint = getSignalFxEndpoint(_signalfxUrl);
      _signalfxReporter = sfxReportBuilder.setEndpoint(signalFxEndpoint).build();
    } else {
      _signalfxReporter = sfxReportBuilder.build();
    }
    
    _metricMetadata = _signalfxReporter.getMetricMetadata();    
    _signalfxReporter.start(_reportIntervalSec, TimeUnit.SECONDS);    
  }

  @Override
  public synchronized void start() {
    _executor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          captureMetrics();          
        } catch (Exception e) {
          LOG.error(_name + "/SignalFxMetricsReporterService failed to report metrics", e);
        }
      }
    }, _reportIntervalSec, _reportIntervalSec, TimeUnit.SECONDS);
    LOG.info("{}/SignalFxMetricsReporterService started", _name);
  }

  @Override
  public synchronized void stop() {
    _executor.shutdown();
    LOG.info("{}/SignalFxMetricsReporterService stopped", _name);
  }

  @Override
  public boolean isRunning() {
    return !_executor.isShutdown();
  }

  @Override
  public void awaitShutdown() {
    try {
      _executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.info("Thread interrupted when waiting for {}/SignalFxMetricsReporterService to shutdown", _name);
    }
    LOG.info("{}/SignalFxMetricsReporterService shutdown completed", _name);
  }
  
  private void setUpDimensionMap() {
    if (_dimensions.size() == 0) {
      return;
    }
    for (String dimension : _dimensions) {
      String dimensionKey = dimension.split(":")[0];
      String dimensionValue = dimension.split(":")[1];
      _dimensionsMap.put(dimensionKey, dimensionValue);
    }
  }
  
  private SignalFxEndpoint getSignalFxEndpoint(String urlStr) throws Exception {    
    URL url = new URL(urlStr);
    String scheme = url.getProtocol();
    String host = url.getHost();
    int port = url.getPort();
    return new SignalFxEndpoint(scheme, host, port);    
  }

  private String generateSignalFxMetricName(String bean, String attribute) {    
    String service = bean.split(":")[1];
    String serviceType = service.split(",")[1].split("=")[1];
    String[] segs = {serviceType, attribute};
    return StringUtils.join(segs, ".");
  }

  private void captureMetrics() {
    for (String metricName : _metricNames) {
      String mbeanExpr = metricName.substring(0, metricName.lastIndexOf(":"));
      String attributeExpr = metricName.substring(metricName.lastIndexOf(":") + 1);
      
      List<MbeanAttributeValue> attributeValues = getMBeanAttributeValues(mbeanExpr, attributeExpr);

      for (final MbeanAttributeValue attributeValue : attributeValues) {
        String metric = attributeValue.toString();
        String key = metric.substring(0, metric.lastIndexOf("="));
        String[] parts = key.split(",");
        if (parts.length < 2) {
          continue;
        }
        parts = parts[0].split("=");
        if (parts.length < 2 || !parts[1].contains("cluster-monitor")) {
          continue;
        }
        String signalFxMetricName = generateSignalFxMetricName(attributeValue.mbean(), attributeValue.attribute());        
        LOG.info("Sending metric name : " + signalFxMetricName + " and value : " + attributeValue.value());
        setMetricValue(signalFxMetricName, attributeValue.value());
      }      
    }
  }
  
  private void setMetricValue(String metric, double value) {
    if (!_metricMap.containsKey(metric)) {
      createMetric(metric);
    }
    _metricMap.get(metric).setValue(value);
  }
  
  private void createMetric(String metric) {
    LOG.info("Creating metric : " + metric);
    SettableDoubleGauge s = _metricMetadata.forMetric(new SettableDoubleGauge())
        .withMetricName(metric).metric();    
    for (String key : _dimensionsMap.keySet()) {
      String value = _dimensionsMap.get(key);
      _metricMetadata.forMetric(s).withDimension(key, value);
    }
    if (metric.contains("partition")) {          
      String partitionNumber = "" + metric.charAt(metric.length() - 1);
      _metricMetadata.forMetric(s).withDimension("partition", partitionNumber);
    }
    _metricMetadata.forMetric(s).register(_metricRegistry);
    _metricMap.put(metric, s);    
  }
}

/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.sample-enterprise.ping;

import com.google.common.base.Joiner;
import io.crate.ClusterIdService;
import io.crate.Version;
import io.crate.monitor.ExtendedNodeInfo;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PingTask extends TimerTask {

    public static final TimeValue HTTP_TIMEOUT = new TimeValue(5, TimeUnit.SECONDS);

    private static final ESLogger logger = Loggers.getLogger(PingTask.class);

    private final ClusterService clusterService;
    private final ClusterIdService clusterIdService;
    private final ExtendedNodeInfo extendedNodeInfo;
    private final String pingUrl;

    private AtomicLong successCounter = new AtomicLong(0);
    private AtomicLong failCounter = new AtomicLong(0);

    public PingTask(ClusterService clusterService,
                    ClusterIdService clusterIdService,
                    ExtendedNodeInfo extendedNodeInfo,
                    String pingUrl) {
        this.clusterService = clusterService;
        this.clusterIdService = clusterIdService;
        this.extendedNodeInfo = extendedNodeInfo;
        this.pingUrl = pingUrl;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getKernelData() {
        return extendedNodeInfo.osInfo().kernelData();
    }

    public
    @Nullable
    String getClusterId() {
        // wait until clusterId is available (master has been elected)
        try {
            return clusterIdService.clusterId().get().value().toString();
        } catch (InterruptedException | ExecutionException e) {
            if (logger.isTraceEnabled()) {
                logger.trace("Error getting cluster id", e);
            }
            return null;
        }
    }

    public Boolean isMasterNode() {
        return clusterService.localNode().isMasterNode();
    }

    public Map<String, Object> getCounters() {
        return new HashMap<String, Object>() {{
            put("success", successCounter.get());
            put("failure", failCounter.get());
        }};
    }

    @Nullable
    public String getHardwareAddress() {
        String macAddress = extendedNodeInfo.networkInfo().primaryInterface().macAddress();
        return (macAddress == null || macAddress.equals("")) ? null : macAddress.toLowerCase(Locale.ENGLISH);
    }

    public String getCrateVersion() {
        return Version.CURRENT.number();
    }

    public String getJavaVersion() {
        return System.getProperty("java.version");
    }

    private URL buildPingUrl() throws URISyntaxException, IOException, NoSuchAlgorithmException {

        URI uri = new URI(this.pingUrl);

        Map<String, String> queryMap = new HashMap<>();
        queryMap.put("cluster_id", getClusterId()); // block until clusterId is available
        queryMap.put("kernel", XContentFactory.jsonBuilder().map(getKernelData()).string());
        queryMap.put("master", isMasterNode().toString());
        queryMap.put("ping_count", XContentFactory.jsonBuilder().map(getCounters()).string());
        queryMap.put("hardware_address", getHardwareAddress());
        queryMap.put("crate_version", getCrateVersion());
        queryMap.put("java_version", getJavaVersion());

        if (logger.isDebugEnabled()) {
            logger.debug("Sending data: {}", queryMap);
        }

        final Joiner joiner = Joiner.on('=');
        List<String> params = new ArrayList<>(queryMap.size());
        for (Map.Entry<String, String> entry : queryMap.entrySet()) {
            if (entry.getValue() != null) {
                params.add(joiner.join(entry.getKey(), entry.getValue()));
            }
        }
        String query = Joiner.on('&').join(params);

        return new URI(
            uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(),
            uri.getPath(), query, uri.getFragment()
        ).toURL();
    }

    @Override
    public void run() {
        try {
            URL url = buildPingUrl();
            if (logger.isDebugEnabled()) {
                logger.debug("Sending UDC information to {}...", url);
            }
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout((int) HTTP_TIMEOUT.millis());
            conn.setReadTimeout((int) HTTP_TIMEOUT.millis());

            if (conn.getResponseCode() >= 300) {
                throw new Exception(String.format(Locale.ENGLISH, "%s Responded with Code %d", url.getHost(), conn.getResponseCode()));
            }
            if (logger.isDebugEnabled()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
                String line = reader.readLine();
                while (line != null) {
                    logger.debug(line);
                    line = reader.readLine();
                }
                reader.close();
            } else {
                conn.getInputStream().close();
            }
            successCounter.incrementAndGet();
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Error sending UDC information", e);
            }
            failCounter.incrementAndGet();
        }
    }
}
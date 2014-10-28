/*******************************************************************************
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 ******************************************************************************/
package com.amazonaws.services.kinesis.log4j;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.log4j.helpers.*;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamStatus;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.core.util.Booleans;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Log4J Appender implementation to support sending data from java applications
 * directly into a Kinesis stream.
 * 
 * More details are available <a
 * href="https://github.com/prasad/kinesis-log4j-appender">here</a>
 */
@Plugin(name = "KinesisBatch", category = "Core", elementType = "appender", printObject = true)
public class KinesisAppenderBatch extends AbstractAppender {
 // private static final Logger LOGGER = LogManager.getLogger(KinesisAppender.class);
  private  String encoding ;
  private  int maxRetries;
  private  int bufferSize;
  private  int threadCount;
  private  int shutdownTimeout;
  private  String streamName;
  private  String accessKey;
  private  String secret;
  private boolean initializationFailed = false;
  private BlockingQueue<Runnable> taskBuffer;
  private AmazonKinesisAsyncClient kinesisClient;
  private AmazonKinesisPutRecordsHelper amazonKinesisPutRecordsHelper;
  private int batchSize;
  private long timeThreshHoldForFlushInMilli;

    protected KinesisAppenderBatch(String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions,
                                   String encoding,
                                   int maxRetries,
                                   int bufferSize,
                                   int threadCount,
                                   int shutdownTimeout,
                                   String streamName,
                                   String accessKey,
                                   String secret,
                                   int batchSize,
                                   long timeThreshHoldForFlushInMilli

    ) {
        super(name, filter, layout, ignoreExceptions);
        setEncoding(encoding);
        setMaxRetries(maxRetries);
        setBufferSize(bufferSize);
        setThreadCount(threadCount);
        setShutdownTimeout(shutdownTimeout);
        setStreamName(streamName);
        this.accessKey = accessKey;
        this.secret = secret;
        this.batchSize = batchSize;
        this.timeThreshHoldForFlushInMilli = timeThreshHoldForFlushInMilli;

        activateOptions();
    }


    /**
     * Create a Kinesis Appender.
     * @param layout The layout to use (required).
     * @param filter The Filter or null.
     * @param ignore If {@code "true"} (default) exceptions encountered when appending events are logged; otherwise
     *               they are propagated to the caller.
     * @return The KinesisAppender.
     */
    @PluginFactory
    public static KinesisAppenderBatch createAppender(
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") final Filter filter,
            @PluginAttribute("name") final String name,
            @PluginAttribute(value = "ignoreExceptions", defaultBoolean = true) final String ignore,
            @PluginAttribute(value = "encoding", defaultString = AppenderConstants.DEFAULT_ENCODING) final String encoding,
            @PluginAttribute(value = "maxRetries", defaultInt = AppenderConstants.DEFAULT_MAX_RETRY_COUNT)  int maxRetries,
            @PluginAttribute(value = "bufferSize", defaultInt = AppenderConstants.DEFAULT_BUFFER_SIZE) int bufferSize,
            @PluginAttribute(value = "threadCount", defaultInt = AppenderConstants.DEFAULT_THREAD_COUNT) int threadCount,
            @PluginAttribute(value = "shutdownTimeout", defaultInt = AppenderConstants.DEFAULT_SHUTDOWN_TIMEOUT_SEC)  int shutdownTimeout,
            @PluginAttribute("streamName") String streamName,
            @PluginAttribute("accessKey") String accessKey,
            @PluginAttribute("secret") String secret,
            @PluginAttribute("batchSize") int batchSize,
            @PluginAttribute(value = "timeThreshHoldForFlushInMilli", defaultLong = AppenderConstants.DEFAULT_TIME_THRESHHOLD_FOR_FLUSH_IN_MILLI) Long timeThreshHoldForFlushInMilli

    ) {
        if (name == null) {
            LOGGER.error("No name provided for KinesisAppender");
            return null;
        }
        if (layout == null) {
            layout = PatternLayout.createDefaultLayout();
        }
        final boolean ignoreExceptions = Booleans.parseBoolean(ignore, true);

        return new KinesisAppenderBatch(name,filter, layout, ignoreExceptions, encoding, maxRetries, bufferSize, threadCount,
                shutdownTimeout, streamName, accessKey, secret, batchSize, timeThreshHoldForFlushInMilli);
    }

    public void error(String message) {

    error(message, null);
  }

  private void error(String message, Exception e) {
    LOGGER.error(message, e);
    getHandler().error(message, e);
    throw new IllegalStateException(message, e);
  }

  /**
   * Configures this appender instance and makes it ready for use by the
   * consumers. It validates mandatory parameters and confirms if the configured
   * stream is ready for publishing data yet.
   * 
   * Error details are made available through the fallback handler for this
   * appender
   * 
   * @throws IllegalStateException
   *           if we encounter issues configuring this appender instance
   */
  public void activateOptions() {
    if (streamName == null) {
      initializationFailed = true;
      error("Invalid configuration - streamName cannot be null for appender: " + getName());
    }

    if (getLayout() == null) {
      initializationFailed = true;
      error("Invalid configuration - No layout for appender: " + getName());
    }

    ClientConfiguration clientConfiguration = new ClientConfiguration();
    clientConfiguration.setMaxConnections(threadCount);
    clientConfiguration.setMaxErrorRetry(maxRetries);
    clientConfiguration.setRetryPolicy(new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
        PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY, maxRetries, true));
    clientConfiguration.setUserAgent(AppenderConstants.USER_AGENT_STRING);

 //   clientConfiguration.withConnectionTimeout(1000).withSocketTimeout(1000);


    BlockingQueue<Runnable> taskBuffer = new LinkedBlockingDeque<Runnable>(bufferSize);
    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(threadCount, threadCount,
        AppenderConstants.DEFAULT_THREAD_KEEP_ALIVE_SEC, TimeUnit.SECONDS, taskBuffer, new DiscardFastProducerPolicy());

//      ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(threadCount, threadCount,
//              AppenderConstants.DEFAULT_THREAD_KEEP_ALIVE_SEC, TimeUnit.SECONDS, taskBuffer, new BlockFastProducerPolicy());

    threadPoolExecutor.prestartAllCoreThreads();

    LOGGER.info("configured max connection:  " + clientConfiguration.getMaxConnections() +
            " max pool size  " + threadPoolExecutor.getMaximumPoolSize() + " core pool size " +
          threadPoolExecutor.getCorePoolSize()
    );

//      AWSCredentialsProvider provider = new AWSCredentialsProvider(
//              ({
//          @Override
//          public AWSCredentials getCredentials() {
//              return new BasicAWSCredentials(accessKey, secret);
//          }
//
//          @Override
//          public void refresh() {
//
//          }
//      };
//      );

      AWSCredentialsProvider provider = new AWSCredentialsProvider() {
          @Override
          public AWSCredentials getCredentials() {
              return new BasicAWSCredentials(accessKey, secret);
          }

          @Override
          public void refresh() {

          }
      };

    kinesisClient = new AmazonKinesisAsyncClient(provider, clientConfiguration,
        threadPoolExecutor);

    DescribeStreamResult describeResult = null;
    int numOfShards = 1;
    try {
      describeResult = kinesisClient.describeStream(streamName);

      String streamStatus = describeResult.getStreamDescription().getStreamStatus();
      if (!StreamStatus.ACTIVE.name().equals(streamStatus) && !StreamStatus.UPDATING.name().equals(streamStatus)) {
        initializationFailed = true;
        error("Stream " + streamName + " is not ready (in active/updating status) for appender: " + getName());
      }
      else
      {
          numOfShards = describeResult.getStreamDescription().getShards().size();
          LOGGER.info("number of shards for stream is " + numOfShards);

      }

    } catch (ResourceNotFoundException rnfe) {
      initializationFailed = true;
      error("Stream " + streamName + " doesn't exist for appender: " + getName(), rnfe);
    }

  //clientConfiguration.withConnectionTimeout(50).withSocketTimeout(50);

    amazonKinesisPutRecordsHelper = new AmazonKinesisPutRecordsHelper(kinesisClient, streamName, batchSize, numOfShards, timeThreshHoldForFlushInMilli);




  }

  /**
   * Closes this appender instance. Before exiting, the implementation tries to
   * flush out buffered log events within configured shutdownTimeout seconds. If
   * that doesn't finish within configured shutdownTimeout, it would drop all
   * the buffered log events.
   *
   * Log4j 2 has currently no way to get this invoked.
   */
  @Override
  public void stop() {
    ThreadPoolExecutor threadpool = (ThreadPoolExecutor) kinesisClient.getExecutorService();
    threadpool.shutdown();
    BlockingQueue<Runnable> taskQueue = threadpool.getQueue();
    int bufferSizeBeforeShutdown = threadpool.getQueue().size();
    boolean gracefulShutdown = true;
    try {
      gracefulShutdown = threadpool.awaitTermination(shutdownTimeout, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // we are anyways cleaning up
    } finally {
      int bufferSizeAfterShutdown = taskQueue.size();
      if (!gracefulShutdown || bufferSizeAfterShutdown > 0) {
        String errorMsg = "Kinesis Log4J Appender (" + getName() + ") waited for " + shutdownTimeout
            + " seconds before terminating but could send only " + (bufferSizeAfterShutdown - bufferSizeBeforeShutdown)
            + " logevents, it failed to send " + bufferSizeAfterShutdown
            + " pending log events from it's processing queue";
        LOGGER.error(errorMsg);
        getHandler().error(errorMsg, null);
      }
    }
    kinesisClient.shutdown();
  }



  /**
   * This method is called whenever a logging happens via logger.log(..) API
   * calls. Implementation for this appender will take in log events instantly
   * as long as the buffer is not full (as per user configuration). This call
   * will block if internal buffer is full until internal threads create some
   * space by publishing some of the records.
   * 
   * If there is any error in parsing logevents, those logevents would be
   * dropped.
   */
  @Override
  public void append(LogEvent logEvent) {
    if (initializationFailed) {
      error("Check the configuration and whether the configured stream " + streamName
          + " exists and is active. Failed to initialize kinesis log4j appender: " + getName());
      return;
    }
    try {
      String message = logEvent.getMessage().getFormattedMessage();
      if (message != null && !message.isEmpty()) {
          String[] partionkeyAndData = message.split("\t");
          String partitionKey = (partionkeyAndData.length == 2) ? partionkeyAndData[0] : UUID.randomUUID().toString();
          String dataStr = (partionkeyAndData.length == 2) ? partionkeyAndData[1] : partionkeyAndData[0];

          ByteBuffer data = ByteBuffer.wrap(dataStr.getBytes(encoding));

          amazonKinesisPutRecordsHelper.addRecord(data, partitionKey, null);
      }
      else {
          LOGGER.debug("kinesis batch appender called with empty message");
      }

    } catch (Exception e) {
      LOGGER.error("Failed to schedule log entry for publishing into Kinesis stream: " + streamName);
      getHandler().error("Failed to schedule log entry for publishing into Kinesis stream: " + streamName, logEvent, e);
    }
  }

  /**
   * Returns configured stream name
   * 
   * @return configured stream name
   */
  public String getStreamName() {
    return streamName;
  }

  /**
   * Sets streamName for the kinesis stream to which data is to be published.
   *
   * @param streamName
   *          name of the kinesis stream to which data is to be published.
   */
  public void setStreamName(String streamName) {
    Validator.validate(!Validator.isBlank(streamName), "streamName cannot be blank");
    this.streamName = streamName.trim();
  }

  /**
   * Configured encoding for the data to be published. If none specified,
   * default is UTF-8
   * 
   * @return encoding for the data to be published. If none specified, default
   *         is UTF-8
   */
  public String getEncoding() {
    return this.encoding;
  }

  /**
   * Sets encoding for the data to be published. If none specified, default is
   * UTF-8
   * 
   * @param encoding
   *          encoding for expected log messages
   */
  public void setEncoding(String encoding) {
    Validator.validate(!Validator.isBlank(encoding), "encoding cannot be blank");
    this.encoding = encoding.trim();
  }

  /**
   * Returns configured maximum number of retries between API failures while
   * communicating with Kinesis. This is used in AWS SDK's default retries for
   * HTTP exceptions, throttling errors etc.
   * 
   * @return configured maximum number of retries between API failures while
   *         communicating with Kinesis
   */
  public int getMaxRetries() {
    return maxRetries;
  }

  /**
   * Configures maximum number of retries between API failures while
   * communicating with Kinesis. This is used in AWS SDK's default retries for
   * HTTP exceptions, throttling errors etc.
   * 
   */
  public void setMaxRetries(int maxRetries) {
    Validator.validate(maxRetries > 0, "maxRetries must be > 0");
    this.maxRetries = maxRetries;
  }

  /**
   * Returns configured buffer size for this appender. This implementation would
   * buffer these many log events in memory while parallel threads are trying to
   * publish them to Kinesis.
   * 
   * @return configured buffer size for this appender.
   */
  public int getBufferSize() {
    return bufferSize;
  }

  /**
   * Configures buffer size for this appender. This implementation would buffer
   * these many log events in memory while parallel threads are trying to
   * publish them to Kinesis.
   */
  public void setBufferSize(int bufferSize) {
    Validator.validate(bufferSize > 0, "bufferSize must be >0");
    this.bufferSize = bufferSize;
  }

  /**
   * Returns configured number of parallel thread count that would work on
   * publishing buffered events to Kinesis
   * 
   * @return configured number of parallel thread count that would work on
   *         publishing buffered events to Kinesis
   */
  public int getThreadCount() {
    return threadCount;
  }

  /**
   * Configures number of parallel thread count that would work on publishing
   * buffered events to Kinesis
   */
  public void setThreadCount(int parallelCount) {
    Validator.validate(parallelCount > 0, "threadCount must be >0");
    this.threadCount = parallelCount;
  }

  /**
   * Returns configured timeout between shutdown and clean up. When this
   * appender is asked to close/stop, it would wait for at most these many
   * seconds and try to send all buffered records to Kinesis. However if it
   * fails to publish them before timeout, it would drop those records and exit
   * immediately after timeout.
   * 
   * @return configured timeout for shutdown and clean up.
   */
  public int getShutdownTimeout() {
    return shutdownTimeout;
  }

  /**
   * Configures timeout between shutdown and clean up. When this appender is
   * asked to close/stop, it would wait for at most these many seconds and try
   * to send all buffered records to Kinesis. However if it fails to publish
   * them before timeout, it would drop those records and exit immediately after
   * timeout.
   */
  public void setShutdownTimeout(int shutdownTimeout) {
    Validator.validate(shutdownTimeout > 0, "shutdownTimeout must be >0");
    this.shutdownTimeout = shutdownTimeout;
  }

  /**
   * Returns count of tasks scheduled to send records to Kinesis. Since
   * currently each task maps to sending one record, it is equivalent to number
   * of records in the buffer scheduled to be sent to Kinesis.
   * 
   * @return count of tasks scheduled to send records to Kinesis.
   */
  public int getTaskBufferSize() {
    return taskBuffer.size();
  }


}

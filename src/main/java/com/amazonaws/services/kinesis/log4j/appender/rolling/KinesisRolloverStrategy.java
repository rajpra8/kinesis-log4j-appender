package com.amazonaws.services.kinesis.log4j.appender.rolling;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.log4j.AppenderConstants;
import com.amazonaws.services.kinesis.log4j.FilePublisher;
import com.amazonaws.services.kinesis.log4j.appender.rolling.action.KinesisAsyncPutAction;
import com.amazonaws.services.kinesis.log4j.helpers.AsyncPutCallStatsReporter;
import com.amazonaws.services.kinesis.log4j.helpers.DiscardFastProducerPolicy;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamStatus;
import org.apache.logging.log4j.core.appender.rolling.AbstractRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.RollingFileManager;
import org.apache.logging.log4j.core.appender.rolling.RolloverDescription;
import org.apache.logging.log4j.core.appender.rolling.action.Action;
import org.apache.logging.log4j.core.appender.rolling.action.CompositeAction;
import org.apache.logging.log4j.core.appender.rolling.action.FileRenameAction;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginConfiguration;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.lookup.StrSubstitutor;
import org.joda.time.DateTime;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by prasad on 9/30/14.
 */
@Plugin(name = "KinesisRolloverStrategy", category = "Core", printObject = true)
public class KinesisRolloverStrategy extends DefaultRolloverStrategy{

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
    private AsyncPutCallStatsReporter asyncCallHander;

    /**
     * Constructs a new instance.
     *
     * @param minIndex         The minimum index.
     * @param maxIndex         The maximum index.
     * @param useMax
     * @param compressionLevel
     * @param subst
     */
    protected KinesisRolloverStrategy(int minIndex, int maxIndex, boolean useMax, int compressionLevel, StrSubstitutor subst) {
        super(minIndex, maxIndex, useMax, compressionLevel, subst);
    }

    public KinesisRolloverStrategy(int minIndex, int maxIndex, boolean useMax, int compressionLevel, StrSubstitutor subst,
                                   String encoding, int maxRetries, int bufferSize, int threadCount, int shutdownTimeout, String streamName,
                                   String accessKey, String secret) {
        super(minIndex, maxIndex, useMax, compressionLevel, subst);
        this.encoding = encoding;
        this.maxRetries = maxRetries;
        this.bufferSize = bufferSize;
        this.threadCount = threadCount;
        this.shutdownTimeout = shutdownTimeout;
        this.streamName = streamName;
        this.accessKey = accessKey;
        this.secret = secret;

        activateOptions();
    }

    /**
     * Create the DefaultRolloverStrategy.
     * @param max The maximum number of files to keep.
     * @param min The minimum number of files to keep.
     * @param fileIndex If set to "max" (the default), files with a higher index will be newer than files with a
     * smaller index. If set to "min", file renaming and the counter will follow the Fixed Window strategy.
     * @param compressionLevelStr The compression level, 0 (less) through 9 (more); applies only to ZIP files.
     * @param config The Configuration.
     * @return A DefaultRolloverStrategy.
     */
    @PluginFactory
    public static KinesisRolloverStrategy createStrategy(
            @PluginAttribute("max") final String max,
            @PluginAttribute("min") final String min,
            @PluginAttribute("fileIndex") final String fileIndex,
            @PluginAttribute("compressionLevel") final String compressionLevelStr,
            @PluginAttribute(value = "encoding", defaultString = AppenderConstants.DEFAULT_ENCODING) final String encoding,
            @PluginAttribute(value = "maxRetries", defaultInt = AppenderConstants.DEFAULT_MAX_RETRY_COUNT)  int maxRetries,
            @PluginAttribute(value = "bufferSize", defaultInt = AppenderConstants.DEFAULT_BUFFER_SIZE) int bufferSize,
            @PluginAttribute(value = "threadCount", defaultInt = AppenderConstants.DEFAULT_THREAD_COUNT) int threadCount,
            @PluginAttribute(value = "shutdownTimeout", defaultInt = AppenderConstants.DEFAULT_SHUTDOWN_TIMEOUT_SEC)  int shutdownTimeout,
            @PluginAttribute("streamName") String streamName,
            @PluginAttribute("accessKey") String accessKey,
            @PluginAttribute("secret") String secret,
            @PluginConfiguration final Configuration config
    ) {
        DefaultRolloverStrategy drs = DefaultRolloverStrategy.createStrategy(max, min, fileIndex, compressionLevelStr, config);
        final boolean useMax = fileIndex == null ? true : fileIndex.equalsIgnoreCase("max");
        return new KinesisRolloverStrategy(drs.getMinIndex(), drs.getMaxIndex(),useMax, drs.getCompressionLevel(),
                config.getStrSubstitutor(),encoding, maxRetries, bufferSize, threadCount, shutdownTimeout, streamName, accessKey, secret
                );

    }

    @Override
    public RolloverDescription rollover(RollingFileManager manager) throws SecurityException {
        RolloverDescription rd = super.rollover(manager);


        KinesisAsyncPutAction kinesisAsyncPutAction =
                new KinesisAsyncPutAction(kinesisClient, asyncCallHander, rd.getActiveFileName(), streamName);
        List<Action> actions = new ArrayList<Action>();
        actions.add(kinesisAsyncPutAction);
        actions.add(rd.getSynchronous());

        CompositeAction compositeAction = new CompositeAction(actions, false);

        KinesisRolloverDescriptionImpl augmented = new KinesisRolloverDescriptionImpl(rd.getActiveFileName(),
                rd.getAppend(), compositeAction, rd.getAsynchronous());


        return augmented;
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
        try {
            describeResult = kinesisClient.describeStream(streamName);

            String streamStatus = describeResult.getStreamDescription().getStreamStatus();
            if (!StreamStatus.ACTIVE.name().equals(streamStatus) && !StreamStatus.UPDATING.name().equals(streamStatus)) {
                initializationFailed = true;
            }
            else
            {
                LOGGER.info("number of shards for stream is " + describeResult.getStreamDescription().getShards().size());
            }

        } catch (ResourceNotFoundException rnfe) {
            initializationFailed = true;
        }

        //clientConfiguration.withConnectionTimeout(50).withSocketTimeout(50);

        asyncCallHander = new AsyncPutCallStatsReporter(streamName);
    }



}

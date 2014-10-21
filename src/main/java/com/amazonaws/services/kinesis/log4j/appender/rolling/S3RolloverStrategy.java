package com.amazonaws.services.kinesis.log4j.appender.rolling;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.kinesis.log4j.AppenderConstants;
import com.amazonaws.services.kinesis.log4j.appender.rolling.action.S3AsyncPutAction;
import com.amazonaws.services.kinesis.log4j.helpers.AsyncPutCallStatsReporter;
import com.amazonaws.services.kinesis.log4j.helpers.DiscardFastProducerPolicy;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
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

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by prasad on 9/30/14.
 */
@Plugin(name = "S3RolloverStrategy", category = "Core", printObject = true)
public class S3RolloverStrategy extends DefaultRolloverStrategy{

    private  int bufferSize;
    private  int threadCount;

    private  String bucketName;
    private  String accessKey;
    private  String secret;
    private boolean initializationFailed = false;
    private BlockingQueue<Runnable> taskBuffer;
    private TransferManager s3TransferManager;


    /**
     * Constructs a new instance.
     *
     * @param minIndex         The minimum index.
     * @param maxIndex         The maximum index.
     * @param useMax
     * @param compressionLevel
     * @param subst
     */
    protected S3RolloverStrategy(int minIndex, int maxIndex, boolean useMax, int compressionLevel, StrSubstitutor subst) {
        super(minIndex, maxIndex, useMax, compressionLevel, subst);
    }

    public S3RolloverStrategy(int minIndex, int maxIndex, boolean useMax, int compressionLevel, StrSubstitutor subst,
                               String bucketName, int bufferSize, int threadCount,
                              String accessKey, String secret) {
        super(minIndex, maxIndex, useMax, compressionLevel, subst);
        this.bufferSize = bufferSize;
        this.threadCount = threadCount;
        this.bucketName = bucketName;
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
    public static S3RolloverStrategy createStrategy(
            @PluginAttribute("max") final String max,
            @PluginAttribute("min") final String min,
            @PluginAttribute("fileIndex") final String fileIndex,
            @PluginAttribute("compressionLevel") final String compressionLevelStr,
            @PluginAttribute("bucketName") String bucketName,
            @PluginAttribute("bufferSize") int bufferSize,
            @PluginAttribute("threadCount") int threadCount,
            @PluginAttribute("accessKey") String accessKey,
            @PluginAttribute("secret") String secret,
            @PluginConfiguration final Configuration config
    ) {
        DefaultRolloverStrategy drs = DefaultRolloverStrategy.createStrategy(max, min, fileIndex, compressionLevelStr, config);
        final boolean useMax = fileIndex == null ? true : fileIndex.equalsIgnoreCase("max");
        return new S3RolloverStrategy(drs.getMinIndex(), drs.getMaxIndex(),useMax, drs.getCompressionLevel(),
                config.getStrSubstitutor(), bucketName,bufferSize, threadCount, accessKey, secret
                );

    }

    @Override
    public RolloverDescription rollover(RollingFileManager manager) throws SecurityException {
        RolloverDescription rd = super.rollover(manager);

        //@todo change it to construct the renamed file name from file pattern processor of appender
        //unfortunaletly default rollover strategy does actual file operation while generating the rename action
        //so no easy way to get the renamed file currently

        FileRenameAction fileRenameAction = (FileRenameAction) rd.getSynchronous();

        String renamedFileAbsolutePath = "";
        try {
            Field f = null; //NoSuchFieldException
            f = fileRenameAction.getClass().getDeclaredField("destination");
            f.setAccessible(true);
            renamedFileAbsolutePath = ((File)f.get(fileRenameAction)).getAbsolutePath();
        } catch (Exception e) {
            e.printStackTrace();
        }

        S3AsyncPutAction s3AsyncPutAction =
                new S3AsyncPutAction(s3TransferManager, renamedFileAbsolutePath, bucketName);
        List<Action> actions = new ArrayList<>();
        actions.add(s3AsyncPutAction);

        if (rd.getAsynchronous() != null){
            actions.add(rd.getAsynchronous());
        }


        CompositeAction compositeAction = new CompositeAction(actions, false);

        KinesisRolloverDescriptionImpl augmented = new KinesisRolloverDescriptionImpl(rd.getActiveFileName(),
                rd.getAppend(), rd.getSynchronous(), compositeAction);


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
        if (bucketName == null) {
            initializationFailed = true;
        } else {

            BlockingQueue<Runnable> taskBuffer = new LinkedBlockingDeque<Runnable>(bufferSize);
            ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(threadCount, threadCount,
                    AppenderConstants.DEFAULT_THREAD_KEEP_ALIVE_SEC, TimeUnit.SECONDS, taskBuffer, new DiscardFastProducerPolicy());


            threadPoolExecutor.prestartAllCoreThreads();


            AWSCredentialsProvider provider = new AWSCredentialsProvider() {
                @Override
                public AWSCredentials getCredentials() {
                    return new BasicAWSCredentials(accessKey, secret);
                }

                @Override
                public void refresh() {

                }
            };

            s3TransferManager = new TransferManager(new AmazonS3Client(provider), threadPoolExecutor);

        }
    }


}

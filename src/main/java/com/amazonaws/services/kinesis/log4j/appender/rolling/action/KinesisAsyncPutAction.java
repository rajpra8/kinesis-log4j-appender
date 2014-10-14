package com.amazonaws.services.kinesis.log4j.appender.rolling.action;

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.log4j.helpers.AsyncPutCallStatsReporter;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import org.apache.logging.log4j.core.appender.rolling.action.AbstractAction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by prasad on 9/30/14.
 */
public class KinesisAsyncPutAction extends AbstractAction {

    private AmazonKinesisAsyncClient kinesisClient;
    private AsyncPutCallStatsReporter asyncCallHander;
    private String fileName;
    private String streamName;


    public KinesisAsyncPutAction(AmazonKinesisAsyncClient kinesisClient, AsyncPutCallStatsReporter asyncCallHander, String fileName, String streamName) {
        this.kinesisClient = kinesisClient;
        this.asyncCallHander = asyncCallHander;
        this.fileName = fileName;
        this.streamName = streamName;
    }

    @Override
    public boolean execute() {

        return publish();
    }

    public  boolean publish()  {

        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            LOGGER.info("Started reading: " + new File(fileName).getAbsolutePath());
            String line = null;
            List<String> messageList = new ArrayList();
            StringBuilder sb = new StringBuilder();
            int j = 0;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\t");
                j++;
                if (j % 100 == 0 ) {
                    messageList.add(sb.toString());
                    sb = new StringBuilder();
                }
            }
            messageList.add(sb.toString());
            reader.close();


            for (String message : messageList){
                    ByteBuffer data = ByteBuffer.wrap(message.getBytes("UTF-8"));
                    kinesisClient.putRecordAsync(new PutRecordRequest().withPartitionKey(UUID.randomUUID().toString())
                            .withStreamName(streamName).withData(data), asyncCallHander);
            }

            return true;

        } catch (IOException e) {
            LOGGER.error("not able to publish while rolling over", e);
            return false;
        }

    }


}

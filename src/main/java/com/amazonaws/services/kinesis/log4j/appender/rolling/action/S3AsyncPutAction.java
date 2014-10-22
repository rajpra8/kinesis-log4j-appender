package com.amazonaws.services.kinesis.log4j.appender.rolling.action;

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.log4j.helpers.AsyncPutCallStatsReporter;
import com.amazonaws.services.kinesis.log4j.helpers.S3ProgressListener;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
public class S3AsyncPutAction extends AbstractAction {

    private String fileName;
    private String bucketName;
    private TransferManager transferManager;
    private static final Logger logger = LogManager.getLogger(S3AsyncPutAction.class);

    public S3AsyncPutAction(TransferManager transferManager, String fileName, String bucketName) {
        this.transferManager = transferManager;
        this.fileName = fileName;
        this.bucketName = bucketName;
    }

    @Override
    public boolean execute() {

        return putFile();
    }

    public  boolean putFile()  {

        File file = new File(fileName);
        try {
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, file.getName(), file);

            Upload myUpload = transferManager.upload(putObjectRequest);
            myUpload.waitForUploadResult();
            if (myUpload.isDone()){
                logger.info("Transfer: " + myUpload.getDescription());
                logger.info("  - State: " + myUpload.getState());
                logger.info("  - Progress: " + myUpload.getProgress().getBytesTransferred());
            }

          //  myUpload.addProgressListener(new S3ProgressListener(myUpload));

            return true;

        } catch (Exception e) {
            LOGGER.error("not able to write while rolling over", e);
            return false;
        }

    }


}
package com.amazonaws.services.kinesis.log4j.appender.rolling.action;

import com.amazonaws.services.kinesis.log4j.helpers.S3ProgressListener;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.apache.logging.log4j.core.appender.rolling.action.AbstractAction;

import java.io.File;

/**
 * Created by prasad on 9/30/14.
 */
public class S3AsyncPutActionWithTransferManager extends AbstractAction {

    private String fileName;
    private String bucketName;
    private TransferManager transferManager;

    public S3AsyncPutActionWithTransferManager(TransferManager transferManager, String fileName, String bucketName) {
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
            LOGGER.info("starting transfer for fileName " + fileName);
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, file.getName(), file);


           Upload myUpload = transferManager.upload(putObjectRequest);

            myUpload.addProgressListener(new S3ProgressListener(myUpload, file));

            return true;

        } catch (Exception e) {
            LOGGER.error("not able to write while rolling over", e);
            return false;
        }

    }


}

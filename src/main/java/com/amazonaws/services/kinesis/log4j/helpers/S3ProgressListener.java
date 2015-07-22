package com.amazonaws.services.kinesis.log4j.helpers;

import com.amazonaws.AmazonClientException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.internal.S3SyncProgressListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.status.StatusLogger;

import java.io.File;

import static com.amazonaws.event.ProgressEventType.TRANSFER_COMPLETED_EVENT;
import static com.amazonaws.event.ProgressEventType.TRANSFER_FAILED_EVENT;


/**
 * Created by prasad on 10/21/14.
 */
public class S3ProgressListener extends S3SyncProgressListener {


    private static Logger logger = StatusLogger.getLogger();

    Upload upload;
    File file;
    @Override
    public void progressChanged(ProgressEvent progressEvent) {

        switch (progressEvent.getEventType()) {
            case TRANSFER_COMPLETED_EVENT:
                logger.info("upload completed " + upload.getDescription());
                this.file.delete();

                break;
            case TRANSFER_FAILED_EVENT:
                try {
                    AmazonClientException e = upload.waitForException();
                    logger.error("transferred failed ", e);

                } catch (InterruptedException e) {}
                break;
             default :
                 logger.debug("event type " + progressEvent.getEventType());
                 logger.debug("Transfer: " + upload.getDescription());
                 logger.debug("  - State: " + upload.getState());
                 logger.debug("  - Progress: " + upload.getProgress().getBytesTransferred());
        }

//      if (upload.isDone()){
//          logger.info("upload completed " + upload.getDescription());
//          this.file.delete();
//      }
//      else
//      {
//          if (logger.isDebugEnabled()){
//
//              logger.debug("Transfer: " + upload.getDescription());
//              logger.debug("  - State: " + upload.getState());
//              logger.debug("  - Progress: " + upload.getProgress().getBytesTransferred());
//          }
//      }
    }

    public S3ProgressListener(Upload upload, File file) {

        this.upload = upload;
        this.file = file;
    }

    @Override
    public void onPersistableTransfer(PersistableTransfer persistableTransfer) {

    }
}

package com.amazonaws.services.kinesis.log4j.helpers;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.transfer.Upload;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by prasad on 10/21/14.
 */
public class S3ProgressListener implements ProgressListener {

    private static Logger logger = LogManager.getLogger(S3ProgressListener.class);

    Upload upload;
    @Override
    public void progressChanged(ProgressEvent progressEvent) {
      if (upload.isDone()){
          logger.info("upload of file " + upload.getDescription() + "completed");
      }
      else
      {
          if (logger.isDebugEnabled()){

              logger.debug("Transfer: " + upload.getDescription());
              logger.debug("  - State: " + upload.getState());
              logger.debug("  - Progress: " + upload.getProgress().getBytesTransferred());
          }
      }
    }

    public S3ProgressListener(Upload upload) {
        this.upload = upload;
    }
}
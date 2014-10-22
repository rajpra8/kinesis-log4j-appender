package com.amazonaws.services.kinesis.log4j.appender.rolling.action;

import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.appender.rolling.action.AbstractAction;

import java.io.File;

/**
 * Created by prasad on 9/30/14.
 */
public class FileDeleteAction extends AbstractAction {

    private String filepath;


    public FileDeleteAction(String filepath) {
        this.filepath = filepath;
    }

    @Override
    public boolean execute() {

        File file = new File(filepath);
        file.delete();
        return !file.exists();
    }




}

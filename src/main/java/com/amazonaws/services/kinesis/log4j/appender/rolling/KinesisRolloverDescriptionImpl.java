package com.amazonaws.services.kinesis.log4j.appender.rolling;

import org.apache.logging.log4j.core.appender.rolling.RolloverDescription;
import org.apache.logging.log4j.core.appender.rolling.action.Action;

/**
 * Created by prasad on 9/30/14.
 */
public class KinesisRolloverDescriptionImpl implements RolloverDescription {

    /**
     * Active log file name after rollover.
     */
    private  String activeFileName;

    /**
     * Should active file be opened for appending.
     */
    private  boolean append;

    /**
     * Action to be completed after close of current active log file
     * before returning control to caller.
     */
    private  Action synchronous;

    /**
     * Action to be completed after close of current active log file
     * and before next rollover attempt, may be executed asynchronously.
     */
    private  Action asynchronous;

    @Override
    public String getActiveFileName() {
        return activeFileName;
    }

    public void setActiveFileName(String activeFileName) {
        this.activeFileName = activeFileName;
    }

    @Override
    public boolean getAppend() {
        return append;
    }

    public void setAppend(boolean append) {
        this.append = append;
    }

    @Override
    public Action getSynchronous() {
        return synchronous;
    }

    public void setSynchronous(Action synchronous) {
        this.synchronous = synchronous;
    }

    @Override
    public Action getAsynchronous() {
        return asynchronous;
    }

    public void setAsynchronous(Action asynchronous) {
        this.asynchronous = asynchronous;
    }

    public KinesisRolloverDescriptionImpl(String activeFileName, boolean append, Action synchronous, Action asynchronous) {
        this.activeFileName = activeFileName;
        this.append = append;
        this.synchronous = synchronous;
        this.asynchronous = asynchronous;
    }
}

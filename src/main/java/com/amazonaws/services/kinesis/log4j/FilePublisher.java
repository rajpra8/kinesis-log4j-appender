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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormat;

/**
 * {@code FilePublisher} can be used to push log files to Kinesis stream
 * directly. It reads input file line by line and uses log4j to write into a
 * logger instance by name <b>KinesisLogger</b>. Implementation assumes that
 * <b>KinesisLogger</b> is configured to log into a Kinesis stream.
 * 
 * Sample configuration is available in
 * src/main/resources/log4j-sample.properties
 */
public class FilePublisher {
  private static final Logger LOGGER = LogManager.getLogger(FilePublisher.class);
  private static final long SLEEP_INTERVAL = 5000;

  private static long getBufferedRecordsCountFromKinesisAppenders() {
    long bufferedRecordsCount = 0;
    Map<String, Appender> allAppenders = ((org.apache.logging.log4j.core.Logger)LOGGER).getAppenders();

    for (Map.Entry<String, Appender> entry : allAppenders.entrySet() ){
        Appender appender = entry.getValue();
        if (appender instanceof KinesisAppender) {
            bufferedRecordsCount += ((KinesisAppender) appender).getTaskBufferSize();
        }
    }
    return bufferedRecordsCount;
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: java " + FilePublisher.class.getName() + " <file_path>");
      System.err.println();
      System.err.println("<file_path>\t-\tabsolute path for the input file, this file will be read line by line and ");
      System.err.println("\t\t\tpublished to Kinesis");
      System.exit(1);
    }
    String fileAbsolutePath = args[0];
    File logFile = new File(fileAbsolutePath);
    if (!logFile.exists() || !logFile.canRead()) {
      System.err.println("File " + args[0] + " doesn't exist or is not readable.");
      System.exit(2);
    }

    //Logger kinesisLogger = LogManager.getLogger("kinesis");
     Logger kinesisBatchLogger = LogManager.getLogger("kinesisBatch");

    Logger kinesisLogger = LogManager.getLogger("kinesisRolling1");




    BufferedReader reader = new BufferedReader(new FileReader(logFile));
    LOGGER.info("Started reading: " + fileAbsolutePath);
    String line = null;
    List<String> messageList = new ArrayList();
    StringBuilder sb = new StringBuilder();
    int j = 0;
      while ((line = reader.readLine()) != null) {
        int loggerNum = randInt(1,4);
//          System.out.println("logger number picked " +  loggerNum );
//          Logger logger = LogManager.getLogger("kinesisRolling" + loggerNum);
//          System.out.println("logger name " + logger.getName());
          //logger.info(line);
        //  LogManager.getLogger("kinesis").info(line);

          kinesisBatchLogger.info(line);
      }

//    while ((line = reader.readLine()) != null) {
//      sb.append(line).append("\t");
//                  j++;
//          if (j % 100 == 0 ) {
//              messageList.add(sb.toString());
//              sb = new StringBuilder();
//          }
//    }
    reader.close();

      DateTime startTime = DateTime.now();
      int i = 0;
//      for (String message : messageList){
//          kinesisLogger.info(message);
//          i++;
////          if (i % 100 == 0 && LOGGER.isInfoEnabled()) {
////              LOGGER.info("Total " + i + " records written to logger");
////          }
//      }
//
//      kinesisLogger.info(sb.toString());

//    long bufferedRecordsCount = getBufferedRecordsCountFromKinesisAppenders();
//    while (bufferedRecordsCount > 0) {
//      LOGGER.info("Publisher threads within log4j appender are still working on sending " + bufferedRecordsCount
//          + " buffered records to Kinesis");
//      try {
//        Thread.sleep(SLEEP_INTERVAL);
//      } catch (InterruptedException e) {
//        // do nothing
//      }
//      bufferedRecordsCount = getBufferedRecordsCountFromKinesisAppenders();
//    }
//    LOGGER.info("Published " + i + " records from " + fileAbsolutePath + " to the logger, took "
//        + PeriodFormat.getDefault().print(new Period(startTime, DateTime.now())));
  }

    /**
     * Returns a pseudo-random number between min and max, inclusive.
     * The difference between min and max can be at most
     * <code>Integer.MAX_VALUE - 1</code>.
     *
     * @param min Minimum value
     * @param max Maximum value.  Must be greater than min.
     * @return Integer between min and max, inclusive.
     * @see java.util.Random#nextInt(int)
     */
    public static int randInt(int min, int max) {

        // NOTE: Usually this should be a field rather than a method
        // variable so that it is not re-seeded every call.
        Random rand = new Random();

        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt((max - min) + 1) + min;

        return randomNum;
    }
}

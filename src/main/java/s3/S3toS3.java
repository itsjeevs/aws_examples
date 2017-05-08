package s3;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * Created by itsjeevs on 4/20/17.
 */


/**
 * This is an example of a lambda that will trigger when there is a file dropped into an s3 bucket.
 * You can do further ETL before saving two data streams based on the input data into s3.
 */

public class S3toS3 implements RequestHandler<S3Event, String> {

    private LambdaLogger logger;

    @Override
    public String handleRequest(S3Event event, Context context) {
        logger = context.getLogger();

        AmazonS3 s3Client = new AmazonS3Client();

        try {
            for (S3EventNotification.S3EventNotificationRecord record : event.getRecords()) {
                S3Object object = s3Client.getObject(new GetObjectRequest(record.getS3().getBucket().getName(), record.getS3().getObject().getKey()));
                String bucket_name = object.getBucketName();
                logger.log("New file: " + object.getKey() + " in " + record.getS3().getBucket().getName());

                BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
                StringBuilder responseStrBuilder = new StringBuilder();

                String line;
                while ((line = reader.readLine()) != null) {
                    responseStrBuilder.append(line);
                }
//
// This is where you can parse your message and do further etl. For the time being, I'm not doing anything.
// I'm just going to write two copies of the file into different folders.
//
//
//                JsonModel checkoutMessage = ParserUtil.readJson(responseStrBuilder.toString());


                ByteArrayInputStream stream1 = new ByteArrayInputStream(responseStrBuilder.toString().getBytes("UTF-8"));
                ByteArrayInputStream stream1_len = new ByteArrayInputStream(responseStrBuilder.toString().getBytes("UTF-8"));

                ByteArrayInputStream stream2 = new ByteArrayInputStream(responseStrBuilder.toString().getBytes("UTF-8"));
                ByteArrayInputStream stream2_len = new ByteArrayInputStream(responseStrBuilder.toString().getBytes("UTF-8"));


                s3Client.putObject(bucket_name + "_copy_one", record.getS3().getObject().getKey(), stream1, getObjMetadata(stream1_len, logger));
                s3Client.putObject(bucket_name + "_copy_two", record.getS3().getObject().getKey(), stream2, getObjMetadata(stream2_len, logger));

            }
        } catch (Exception e) {
            logger.log(e.toString());
        }
        return null;
    }

    ObjectMetadata getObjMetadata(ByteArrayInputStream stream, LambdaLogger logger) {
        ObjectMetadata meta = new ObjectMetadata();
        try {
            meta.setContentLength(Long.valueOf(IOUtils.toByteArray(stream).length));
            meta.setContentType("text/plain");
        } catch (IOException e) {
            logger.log(e.toString());
        }
        return meta;
    }
}

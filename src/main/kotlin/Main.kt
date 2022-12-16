package com.example.s3
// snippet-start:[s3.java2.list_objects.import]
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*
import java.io.FileReader
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.*
import kotlin.system.exitProcess


// snippet-end:[s3.java2.list_objects.import]
/**
 * Before running this Java V2 code example, set up your development environment, including your credentials.
 *
 * For more information, see the following documentation topic:
 *
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */
object ListObjects {
    @JvmStatic
    fun main(args: Array<String>) {
        val bucketName = "rm-berkley-data"
        println("Entered Bucket name # $bucketName")

        val credentialsProvider: ProfileCredentialsProvider = ProfileCredentialsProvider.create()
        val region: Region = Region.US_EAST_1
        val s3Client: S3Client = S3Client.builder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .build()



        listBucketObjects(s3Client, bucketName)
        s3Client.close()
    }

    // snippet-start:[s3.java2.list_objects.main]
    private fun listBucketObjects(s3: S3Client, bucketName: String) {
        try {
            val listObjects: ListObjectsRequest = ListObjectsRequest
                .builder()
                .bucket(bucketName)
                .build()
            val res: ListObjectsResponse = s3.listObjects(listObjects)
            val objects: List<S3Object> = res.contents()
            println("Filename,Timestamp")

            val timeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

            objects.filter { !it.key().endsWith("manifest") }.
            forEach {
                val getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(it.key())
                    .build()

                val ts = timeFormat.format(LocalDateTime.ofEpochSecond(it.lastModified().epochSecond,0, ZoneOffset.UTC))
                println("${it.key()},${it.size()},$ts" )
                var recCount = 0
//                s3.getObject(getObjectRequest).bufferedReader().readLines().forEach { Line ->
//                    println(Line)
//                    recCount++
//                }

                //val  fr: FileReader = FileReader(s3.getObject(getObjectRequest))
                //FileReader will not take S3 Object
                val  sc: Scanner = Scanner(s3.getObject(getObjectRequest))
                while(sc.hasNextLine()) {
                    recCount++
                    println("Scanner # ${sc.nextLine()}" )}
                println("${it.key()}, Record Count # $recCount")


                exitProcess(0)

            }
        } catch (e: S3Exception) {
            System.err.println(e.awsErrorDetails().errorMessage())
            exitProcess(1)
        }
    }


}
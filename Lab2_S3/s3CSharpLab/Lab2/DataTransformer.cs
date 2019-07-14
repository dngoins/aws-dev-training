// Copyright 2017 Amazon Web Services, Inc. or its affiliates. All rights reserved.

using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace Lab2
{
    // The DataTransformer class transforms objects in the input S3 bucket and
    // puts the transformed objects into the output S3 bucket.
    public static class DataTransformer
    {
        public static readonly string[] Attributes = { "genericDrugName", "adverseReaction" };

        //  Set input bucket name (must be globally unique)
        public static readonly string InputBucketName = "dng-aws-dev-training-input";

        // Set output bucket name (must be globally unique)
        public static readonly string OutputBucketName = "dng-aws-dev-training-output";

        public static readonly string JsonComment = "\"comment\": \"DataTransformer JSON\",";

        // The Amazon S3 client allows you to manage buckets and objects programmatically.
        public static AmazonS3Client s3ForStudentBuckets;

        // List used to store pre-signed URLs generated.
        public static Collection<string> preSignedUrls = new Collection<string>();

        public static void Main()
        {
            ListObjectsRequest inputFileObjects;
            string fileKey = null;
            string transformedFile = null;
            string url = null;

            Init();

            try
            {
                Debug.WriteLine("Transformer: Here we go...");
                CreateBucket(InputBucketName);
                CreateBucket(OutputBucketName);

                inputFileObjects = new ListObjectsRequest
                {
                    BucketName = InputBucketName
                };

                ListObjectsResponse listResponse;
                do
                {
                    // Get a list of objects
                    listResponse = s3ForStudentBuckets.ListObjects(inputFileObjects);
                    foreach (S3Object obj in listResponse.S3Objects)
                    {
                        fileKey = obj.Key;
                        Debug.WriteLine("Transformer: Transforming file: " + fileKey);
                        if (fileKey.EndsWith(".txt"))
                        {
                            GetObjectResponse curObject = GetS3Object(s3ForStudentBuckets, InputBucketName, fileKey);
                            transformedFile = TransformText(curObject);

                            //  Switch to enhanced file upload
                            //var putResponse = PutObjectBasic(s3ForStudentBuckets, OutputBucketName, fileKey, transformedFile);

                            // PutObjectEnhanced(OutputBucketName, fileKey, transformedFile);
                            var putResponse = PutObjectEnhanced(s3ForStudentBuckets, OutputBucketName, fileKey, transformedFile);

                            // File is now encrypted to decrypt use the DownloadObjectAsync 
                            // method logic from here: https://docs.aws.amazon.com/AmazonS3/latest/dev/sse-c-using-dot-net-sdk.html


                            url = GeneratePresignedURL(fileKey, OutputBucketName);
                            if (url != null)
                            {
                                preSignedUrls.Add(url);
                            }
                        }
                    }

                    // Set the marker property
                    inputFileObjects.Marker = listResponse.NextMarker;
                } while (listResponse.IsTruncated);

                PrintPresignedUrls();
                Debug.WriteLine("Transformer: DONE");
            }
            catch (AmazonServiceException ase)
            {
                Debug.WriteLine("Error Message:    " + ase.Message);
                Debug.WriteLine("HTTP Status Code: " + ase.StatusCode);
                Debug.WriteLine("AWS Error Code:   " + ase.ErrorCode);
                Debug.WriteLine("Error Type:       " + ase.ErrorType);
                Debug.WriteLine("Request ID:       " + ase.RequestId);
            }
            catch (AmazonClientException ace)
            {
                Debug.WriteLine("Error Message: " + ace.Message);
            }
        }

        private static void PrintPresignedUrls()
        {
            Debug.WriteLine("Transformer: Pre-signed URLs: ");
            foreach (string url in preSignedUrls)
            {
                Debug.WriteLine(url + "\n");
            }
        }

        // Create the output bucket if it does not exist already.
        public static void CreateBucket(string bucket)
        {
            ListBucketsResponse responseBuckets = s3ForStudentBuckets.ListBuckets();
            bool found = false;

            foreach (S3Bucket s3Bucket in responseBuckets.Buckets)
            {
                if (s3Bucket.BucketName == bucket)
                {
                    found = true;
                    VerifyBucketOwnership(bucket);
                    break;
                }
                else
                {
                    found = false;
                }
            }

            if (found == false)
            {
                Debug.Write("Transformer: Creating bucket: " + bucket);
                PutBucketRequest request = new PutBucketRequest();
                request.BucketName = bucket;
                s3ForStudentBuckets.PutBucket(request);
            }
        }

        // Verify that this AWS account is the owner of the bucket.
        public static void VerifyBucketOwnership(string bucketName)
        {
            bool ownedByYou = false;
            ListBucketsResponse responseBuckets = s3ForStudentBuckets.ListBuckets();

            foreach (S3Bucket bucket in responseBuckets.Buckets)
            {
                if (bucket.BucketName.Equals(bucketName))
                {
                    ownedByYou = true;
                }
            }

            if (!ownedByYou)
            {
                Debug.WriteLine(String.Format("The {0} bucket is owned by another account. Specify a unique name for your bucket. ", bucketName));
            }
        }

        private static void Init()
        {
            s3ForStudentBuckets = CreateS3Client();
            Utils.Setup(s3ForStudentBuckets);
        }

        // Reads the input stream of the S3 object. Transforms content to JSON format.
        // Return the transformed text in a File object.
        private static string TransformText(GetObjectResponse response)
        {
            string transformedText = null;
            StringBuilder sbJSON = new StringBuilder();
            string line;

            try
            {
                // Transform to JSON then write to file
                StreamReader reader = new StreamReader(response.ResponseStream);
                while((line = reader.ReadLine()) != null)
                {
                    sbJSON.Append(TransformLineToJson(line));
                }
                reader.Close();
            }
            catch (IOException ex)
            {
                Debug.WriteLine("Transformer: Unable to create transformed file");
                Debug.WriteLine(ex.Message);
            }

            transformedText = sbJSON.ToString();
            return transformedText;
        }

        private static string TransformLineToJson(string inputLine)
        {
            string[] inputLineParts = inputLine.Split(',');
            int len = inputLineParts.Length;

            string jsonAttrText = "{\n  " + JsonComment + "\n";
            for (int i = 0; i < len; i++)
            {
                jsonAttrText = jsonAttrText + "  \"" + Attributes[i] + "\"" + ":" + "\"" + inputLineParts[i] + "\"";
                if (i != len - 1)
                {
                    jsonAttrText = jsonAttrText + ",\n";
                }
                else
                {
                    jsonAttrText = jsonAttrText + "\n";
                }
            }
            jsonAttrText = jsonAttrText + "},\n";
            return jsonAttrText;
        }

        /**
         * Create an instance of the AmazonS3Client object
         *
         * @return      The S3 Client
         */
        private static AmazonS3Client CreateS3Client()
        {
            // Replace the solution with your own code

            var client = new AmazonS3Client();
            return client;

        }

        /**
         * Retrieve each object from the input S3 bucket
         *
         * @param s3Client      The S3 Client
         * @param bucketName    Name of the S3 bucket
         * @param fileKey       Key (path) to the file
         * @return              The file contents
         */
        private static GetObjectResponse GetS3Object(AmazonS3Client s3Client, string bucketName, string fileKey)
        {
            //  Replace the solution with your own code
           
            GetObjectRequest request = new GetObjectRequest
            {
                BucketName = bucketName,
                Key = fileKey,
                //ByteRange = new ByteRange(0, 10)
            };

            GetObjectResponse response = s3Client.GetObject(request);
            return response;
        }

        /**
         * Upload object to output bucket
         *
         * @param bucketName          Name of the S3 bucket
         * @param fileKey             Key (path) to the file
         * @param transformedFile     Contents of the file
         */
        private static PutObjectResponse PutObjectBasic(AmazonS3Client s3Client, string bucketName, string fileKey, string transformedFile)
        {
            //  Replace the solution with your own code
            var putRequest1 = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = fileKey,
                ContentBody = transformedFile
            };
            putRequest1.Metadata.Add("x-amz-meta-title", fileKey);
            putRequest1.Metadata.Add("contact", "John Doe");

            PutObjectResponse response1 = s3Client.PutObject(putRequest1);

            return response1;
        }

        /**
         * Generate a pre-signed URL to retrieve object
         *
         * @param objectKey     Key (path) to the file
         * @return              Presigned URL
         */
        private static string GeneratePresignedURL(string objectKey, string bucketName)
        {
            // Replace the solution with your own code
            var urlString = string.Empty;

            GetPreSignedUrlRequest request1 = new GetPreSignedUrlRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                Verb = HttpVerb.GET,
                Expires = DateTime.Now.AddMinutes(15)
            };
            urlString = s3ForStudentBuckets.GetPreSignedURL(request1);

            return urlString;

        }

        /**
         * Upload a file to a S3 bucket using AES 256 server-side encryption
         *
         * @param bucketName          Name of the S3 bucket
         * @param fileKey             Key (path) to the file
         * @param transformedFile     Contents of the file
         */
        private static PutObjectResponse PutObjectEnhanced(AmazonS3Client s3Client, string bucketName, string fileKey, string transformedFile)
        {
            // Replace the solution with your own code
            //Solution.PutObjectEnhanced(s3ForStudentBuckets, bucketName, fileKey, transformedFile);

            //  Replace the solution with your own code
            
            Aes aesEncryption = Aes.Create();
            aesEncryption.KeySize = 256;
            aesEncryption.GenerateKey();
            string base64Key = Convert.ToBase64String(aesEncryption.Key);

            Debug.WriteLine("KEY: " + base64Key);

            // 1. Upload the object.
            PutObjectRequest putObjectRequest = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = fileKey,
                ContentBody = transformedFile,
                ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
                ServerSideEncryptionCustomerProvidedKey = base64Key
            };

            putObjectRequest.Metadata.Add("x-amz-meta-title", fileKey);
            putObjectRequest.Metadata.Add("contact", "John Doe");

            PutObjectResponse putObjectResponse = s3Client.PutObject(putObjectRequest);
            return putObjectResponse;

           

        }
    }
}

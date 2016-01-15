namespace AliyunToAzureSample
{
    using System;
    using System.Collections.Concurrent;
    using System.Configuration;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Aliyun;
    using Aliyun.OSS;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using Microsoft.WindowsAzure.Storage.Auth;
    using System.Diagnostics;
    using System.Net;
    class Program
    {
        // This sample uses two threads working as producer and consumer. This job queue is used to share data between two threads.
        private static BlockingCollection<AliyunToAzureTransferJob> jobQueue;

        // CountdownEvent to indicate the overall transfer completion.
        private static CountdownEvent countdownEvent = new CountdownEvent(1);

        // Local folder used to store temporary files
        private static string tempFolder = "TempFolder";

        private static PocMarker markerJournal = new PocMarker(PocConstant.MarkerFileName);

        private static PocManifest pocManifest = new PocManifest();

        // Amazon s3 client.
        private static OssClient ossClient;

        // Microsoft Azure cloud blob client.
        private static CloudBlobClient azureClient;
        static void Main(string[] args)
        {
            try
            {
                Stopwatch watch = new Stopwatch();
                watch.Start();

                PocConfig.Init(args);
                ServicePointManager.DefaultConnectionLimit = Environment.ProcessorCount * 8;
                ServicePointManager.Expect100Continue = false;

                jobQueue = new BlockingCollection<AliyunToAzureTransferJob>(PocConfig.QueueCapacity);

                string endpoint = LoadSettingFromAppConfig("AliyunEndpoint");
                string aliyunAccessKeyId = LoadSettingFromAppConfig("AliyunAccessKeyId");
                string aliyunSecretAccessKey = LoadSettingFromAppConfig("AliyunSecretAccessKey");
                ossClient = new OssClient(endpoint, aliyunAccessKeyId, aliyunSecretAccessKey);

                // Create Microsoft Azure client
                string azureConnectionString = LoadSettingFromAppConfig("AzureStorageConnectionString");
                CloudStorageAccount account = CloudStorageAccount.Parse(azureConnectionString);
                azureClient = account.CreateCloudBlobClient();

                // Create local temporary folder
                Directory.CreateDirectory(tempFolder);

                // Configue DataMovement library
                TransferManager.Configurations.UserAgentSuffix = "AliyunToAzureSample";

                Console.WriteLine("===Transfer begins===");

                // Start a thread to list objects from your Amazon s3 bucket
                Task.Run(() => { ListFromAliyun(); });

                // Start a thread to transfer listed objects into your Microsoft Azure blob container
                Task.Run(() => { TransferToAzure(); });

                // Wait until all data are copied into Azure
                countdownEvent.Wait();

                Console.WriteLine("===Transfer finishes===");

                watch.Stop();
                Console.WriteLine("Elapsed time: {0}", watch.Elapsed);
            }
            finally
            {
                // Delete the temporary folder
                Directory.Delete(tempFolder, true);
            }
        }

        /// <summary>
        /// Get job from jobQueue and transfer data into your Azure blob container.
        /// </summary>
        private static void TransferToAzure()
        {
            // Create the container if it doesn't exist yet
            CloudBlobClient client = azureClient;
            CloudBlobContainer container = client.GetContainerReference(PocConfig.DestContainer);
            container.CreateIfNotExists();

            while (!jobQueue.IsCompleted)
            {
                AliyunToAzureTransferJob job = null;
                try
                {
                    job = jobQueue.Take();
                }
                catch (InvalidOperationException)
                {
                    // No more jobs to do
                }

                if (job == null)
                {
                    break;
                }

                countdownEvent.AddCount();

                CloudBlockBlob cloudBlob = container.GetBlockBlobReference(job.Name);

                Console.WriteLine("Start to transfer {0} to azure.", job.Name);

                Task task = null;
                bool destExist = false;

                try
                {
                    TransferContext context = new TransferContext();

                    context.OverwriteCallback = (source, dest) =>
                    {
                        Console.WriteLine("Dest already exist {0}", dest);
                        destExist = true;
                        return false;
                    };

                    // By default, the sample will download an amazon s3 object into a local file and
                    // then upload it into Microsoft Azure Stroage with DataMovement library.
                    task = TransferManager.UploadAsync(job.Source, cloudBlob, null, context);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error occurs when transferring {0}: {1}", job.Name, e.ToString());
                }

                if (task != null)
                {
                    task.ContinueWith(t =>
                    {
                        if (t.IsFaulted)
                        {
                            if (!destExist)
                            {
                                pocManifest.AddFailed(job.Name, PocErrorString.UploadFailed);
                                Console.Error.WriteLine("Error occurs when transferring {0}: {1}", job.Name, t.Exception.ToString());
                            }
                            else
                            {
                                pocManifest.AddFailed(job.Name, PocErrorString.DestAlreadyExist);    
                            }
                        }
                        else
                        {
                            cloudBlob.FetchAttributes(options: PocConstant.DefaultBlobRequestOptions);
                            if (job.ContentMD5 != cloudBlob.Properties.ContentMD5)
                            {
                                pocManifest.AddFailed(job.Name, PocErrorString.UploadContentMissMatch);
                                Console.Error.WriteLine("Data loss! {0}", job.Name);
                            }
                            else
                            {
                                pocManifest.AddTransferred(job.Name);
                            }

                            Console.WriteLine("Source md5: {0}, Destination md5: {1}. Succeed to transfer data to blob {2}", job.ContentMD5, cloudBlob.Properties.ContentMD5, job.Name);
                        }

                        // Signal the countdown event when one transfer job finishes.
                        countdownEvent.Signal();
                    });
                }
                else
                {
                    // Signal the countdown event when one transfer job finishes.
                    countdownEvent.Signal();
                }
            }

            // Signal the countdown event to unblock the main thread when all data are transferred.
            countdownEvent.Signal();
        }

        /// <summary>
        /// Create a <see cref="S3ToAzureTransferJob"/> representing a download-to-local-and-upload copy from one S3 object to Azure blob.
        /// </summary>
        /// <param name="sourceObject">S3 object used to create the job.</param>
        /// <returns>A job representing a download-to-local-and-upload copy from one S3 object to Azure blob.</returns>
        private static AliyunToAzureTransferJob CreateTransferJob(OssObjectSummary objectSummary)
        {
            // Download the source object to a temporary file
            GetObjectRequest getObjectRequest = new GetObjectRequest(PocConfig.SourceBucket, objectSummary.Key);

            using (OssObject ossObject = ossClient.GetObject(getObjectRequest))
            {
                string tempFile = Path.Combine(tempFolder, Guid.NewGuid().ToString());
                string md5;
                string sha256;
                try
                {
                    PocUtil.DownloadOssObject2File(ossObject, tempFile, out md5, out sha256);
                }
                catch(Exception e)
                {
                    Console.Error.WriteLine("Fail to download from aliyun {0}. Error: {1}", objectSummary.Key, e.ToString());
                    pocManifest.AddFailed(objectSummary.Key, PocErrorString.DownloadFailed);
                    return null;
                }

                if (!VerifyDownloadSHA(objectSummary.Key, sha256))
                {
                    Console.Error.WriteLine("Download content miss match {0}. SHA: {1}", objectSummary.Key, sha256);
                    pocManifest.AddFailed(objectSummary.Key, PocErrorString.DownloadContentMissMatch);
                    return null;
                }

                AliyunToAzureTransferJob job = new AliyunToAzureTransferJob()
                {
                    Source = tempFile,
                    Name = ossObject.Key,
                    ContentMD5 = md5,
                };

                return job;
            }
        }

        static void ListFromAliyun()
        {
            string marker = markerJournal.Marker;
            int listedCount = 0;
            long totalSize = 0;
            do
            {
                markerJournal.Update(marker);

                ListObjectsRequest listObjectsReq = new ListObjectsRequest(PocConfig.SourceBucket)
                {
                    MaxKeys = PocConfig.ListStep,
                    Marker = marker,
                    Prefix = PocConfig.Prefix,
                };

                ObjectListing listedObjects = ossClient.ListObjects(listObjectsReq);
                marker = listedObjects.NextMarker;

                foreach (OssObjectSummary objectSummary in listedObjects.ObjectSummaries)
                {
                    Console.WriteLine("{0} (size:{1}; LMT:{2})", objectSummary.Key, objectSummary.Size, objectSummary.LastModified);
                    totalSize += objectSummary.Size;
                    AliyunToAzureTransferJob job = CreateTransferJob(objectSummary);
                    
                    if (++listedCount > PocConfig.MaxFileNumber)
                    {
                        goto Finish;
                    }

                    if (job == null)
                    {
                        continue;
                    }

                    jobQueue.Add(job);
                }

            }
            while (!string.IsNullOrEmpty(marker));

            Finish:
            jobQueue.CompleteAdding();

            Console.WriteLine("List complete");
        }

        private static bool VerifyDownloadSHA(string name, string sha256)
        {
            string sha256InName = name.Substring(PocConfig.SourceBucket.Length + 1);
            return string.Equals(sha256InName, sha256);
        }

        /// <summary>
        /// Load a setting from app.config.
        /// </summary>
        /// <param name="key">Key of setting.</param>
        /// <returns>Value of setting.</returns>
        private static string LoadSettingFromAppConfig(string key)
        {
            string result = ConfigurationManager.AppSettings[key];

            if (string.IsNullOrEmpty(result))
            {
                throw new InvalidOperationException(string.Format("{0} is not set in App.config.", key));
            }

            return result;
        }

        /// <summary>
        /// Entity class to represent a job to transfer from s3 to azure
        /// </summary>
        class AliyunToAzureTransferJob
        {
            public string Name;
            public string Source;
            public string ContentMD5;
        }

        /// <summary>
        /// A helper class to record progress reported by data movement library.
        /// </summary>
        class ProgressRecorder : IProgress<TransferProgress>
        {
            private long latestBytesTransferred;
            private long latestNumberOfFilesTransferred;
            private long latestNumberOfFilesSkipped;
            private long latestNumberOfFilesFailed;

            /// <summary>
            /// Callback to get the progress from data movement library.
            /// </summary>
            /// <param name="progress">Transfer progress.</param>
            public void Report(TransferProgress progress)
            {
                this.latestBytesTransferred = progress.BytesTransferred;
                this.latestNumberOfFilesTransferred = progress.NumberOfFilesTransferred;
                this.latestNumberOfFilesSkipped = progress.NumberOfFilesSkipped;
                this.latestNumberOfFilesFailed = progress.NumberOfFilesFailed;
            }

            /// <summary>
            /// Return the recorded progress information.
            /// </summary>
            /// <returns>Recorded progress information.</returns>
            public override string ToString()
            {
                return string.Format("Transferred bytes: {0}; Transfered: {1}, Skipped: {2}, Failed: {3}",
                    this.latestBytesTransferred,
                    this.latestNumberOfFilesTransferred,
                    this.latestNumberOfFilesSkipped,
                    this.latestNumberOfFilesFailed);
            }
        }
    }
}

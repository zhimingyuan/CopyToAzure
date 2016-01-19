using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace AliyunToAzureSample
{
    class PocConstant
    {
        public const long KB = 1024L;
        public const long MB = 1024L * 1024;
        public const long GB = 1024L * 1024 * 1024;
        public const long TB = 1024L * 1024 * 1024 * 1024;

        public const int ListStep = 8;
        public const int QueueCapacity = 4;
        public const int MaxFileNumber = 10;

        public const string ListStepName = "liststep";
        public const string QueueCapacityName = "queuecapacity";
        public const string MaxFileNumberName = "maxfilenumber";
        public const string SourceName = "source";
        public const string DestName = "dest";
        public const string PrefixName = "prefix";

        public const string MarkerFileName = "Marker.bin";

        public static readonly BlobRequestOptions DefaultBlobRequestOptions = new BlobRequestOptions()
        {
            RetryPolicy = new LinearRetry(new TimeSpan(0, 0 , 10), 3)
        };

    }
}

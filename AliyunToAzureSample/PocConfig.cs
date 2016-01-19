using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AliyunToAzureSample
{
    static class PocConfig
    {
        public static void Init(string[] args)
        {
            PocConfig.ListStep = PocConstant.ListStep;
            PocConfig.QueueCapacity = PocConstant.QueueCapacity;
            PocConfig.MaxFileNumber = PocConstant.MaxFileNumber;
            PocConfig.SourceDir = string.Empty;
            PocConfig.DestDir = string.Empty;

            foreach(string arg in args)
            {
                string[] pair = arg.Split(new char[] {':'}, StringSplitOptions.RemoveEmptyEntries);
                string key = pair[0].Substring(1);
                string value = pair[1];
                ParseOptions(key, value);
            }

            if (string.IsNullOrEmpty(SourceBucket) || string.IsNullOrEmpty(DestContainer))
            {
                throw new InvalidOperationException("Missing source bucket or dest container");
            }
        }

        public static void ParseOptions(string key, string value)
        {
            if (string.Equals(key, PocConstant.ListStepName, StringComparison.OrdinalIgnoreCase))
            {
                PocConfig.ListStep = int.Parse(value);
            }
            else if (string.Equals(key, PocConstant.QueueCapacityName, StringComparison.OrdinalIgnoreCase))
            {
                PocConfig.QueueCapacity = int.Parse(value);
            }
            else if (string.Equals(key, PocConstant.MaxFileNumberName, StringComparison.OrdinalIgnoreCase))
            {
                PocConfig.MaxFileNumber = int.Parse(value);
            }
            else if (string.Equals(key, PocConstant.SourceName, StringComparison.OrdinalIgnoreCase))
            {
                string[] tokens = value.Split(new char[] { '/' }, 2);
                PocConfig.SourceBucket = tokens[0];

                if (tokens.Length == 2)
                {
                    PocConfig.SourceDir = tokens[1].TrimEnd('/');
                }
            }
            else if (string.Equals(key, PocConstant.DestName, StringComparison.OrdinalIgnoreCase))
            {
                string[] tokens = value.Split(new char[] { '/' }, 2);
                PocConfig.DestContainer = tokens[0];

                if (tokens.Length == 2)
                {
                    PocConfig.DestContainer = tokens[1].TrimEnd('/');
                }
            }
            else if (string.Equals(key, PocConstant.PrefixName, StringComparison.OrdinalIgnoreCase))
            {
                PocConfig.Prefix = value;
            }
        }

        public static int ListStep
        {
            get;
            private set;
        }     
        
        public static int QueueCapacity
        {
            get;
            private set;
        }

        public static int MaxFileNumber
        {
            get;
            private set;
        }

        public static string SourceBucket
        {
            get;
            private set;
        }
        
        public static string SourceDir
        {
            get;
            private set;
        }

        public static string DestContainer
        {
            get;
            private set;
        }

        public static string DestDir
        {
            get;
            private set;
        }

        public static string Prefix
        {
            get;
            private set;
        }
    }
}

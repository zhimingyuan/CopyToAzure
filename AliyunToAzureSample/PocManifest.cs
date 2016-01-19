using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AliyunToAzureSample
{
    class PocManifest : IDisposable
    {
        private StreamWriter transferredWriter;
        private StreamWriter failedWriter;

        private const string TransferredFileName = "Transferred.log";
        private const string FailedFileName = "Failed.log";

        public PocManifest()
        {
            string timeStamp = DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss");
            this.transferredWriter = new StreamWriter(timeStamp + TransferredFileName);
            this.failedWriter = new StreamWriter(timeStamp + FailedFileName);
        }
        
        public void AddTransferred(string fileName)
        {
            lock (this.transferredWriter)
            {
                this.transferredWriter.WriteLine(fileName);
                this.transferredWriter.Flush();
            }
        }

        public void AddFailed(string fileName, string errorString)
        {
            lock (this.failedWriter)
            {
                this.failedWriter.WriteLine(string.Format("{0}${1}", fileName, errorString));
                this.failedWriter.Flush();
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (this.failedWriter != null)
                    {
                        this.failedWriter.Dispose();
                        this.failedWriter = null;
                    }

                    if (this.transferredWriter != null)
                    {
                        this.transferredWriter.Dispose();
                        this.transferredWriter = null;
                    }
                }

                disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace AliyunToAzureSample
{
    /// <summary>
    /// Layout of marker in file
    /// [Flag ][First Marker][Second Marker]
    /// [ 64B ][    2048B   ][    2048B    ]
    /// </summary>

    class PocMarker : IDisposable
    {
        // All size and offset are in byte
        private const long FlagLength = 64;
        private const long MarkerLength = 2048;
        private const long FlagOffset = 0;
        private const long FirstMarkerOffset = FlagLength;
        private const long SecondMarkerOffset = FlagLength + MarkerLength;

        // If flag is true, write to first marker, otherwise write to second marker.
        private bool flag = true;

        private FileStream fileStream;

        private IFormatter formatter = new BinaryFormatter();

        public PocMarker(string filePath)
        {
            if (File.Exists(filePath))
            {
                this.fileStream = new FileStream(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                this.flag = (bool)this.formatter.Deserialize(fileStream);

                long offset = this.flag ? FirstMarkerOffset : SecondMarkerOffset;

                this.fileStream.Seek(offset, SeekOrigin.Begin);
                this.Marker = (string)this.formatter.Deserialize(fileStream);
            }
            else
            {
                this.fileStream = new FileStream(filePath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);

                // initialize the journal file
                this.fileStream.Seek(FlagOffset, SeekOrigin.Begin);
                this.formatter.Serialize(this.fileStream, true);

                this.fileStream.Seek(FirstMarkerOffset, SeekOrigin.Begin);
                this.formatter.Serialize(this.fileStream, string.Empty);

                this.fileStream.Seek(SecondMarkerOffset, SeekOrigin.Begin);
                this.formatter.Serialize(this.fileStream, string.Empty);

                this.Marker = string.Empty;
            }
        }

        public string Marker
        {
            get;
            private set;
        }

        // Note that this call is not thread safe
        public void Update(string marker)
        {
            long offset = this.flag ? FirstMarkerOffset : SecondMarkerOffset;
            fileStream.Seek(offset, SeekOrigin.Begin);

            formatter.Serialize(fileStream, marker);

            // Commit the update
            fileStream.Seek(FlagOffset, SeekOrigin.Begin);
            formatter.Serialize(fileStream, this.flag);

            this.flag = !this.flag;
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    this.fileStream.Dispose();
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

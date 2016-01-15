using Aliyun.OSS;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace AliyunToAzureSample
{
    class PocUtil
    {
        public static void DownloadOssObject2File(OssObject ossObject, string filePath, out string md5, out string sha256)
        {
            using (SHA256 sha256Hash = SHA256Managed.Create())
            using (MD5 md5Hash = MD5.Create())
            using (FileStream fileStream = new FileStream(filePath, FileMode.Create, FileAccess.ReadWrite, FileShare.None))
            using (Stream content = ossObject.Content)
            {
                byte[] buf = new byte[1024];
                int length = 0;

                while ((length = content.Read(buf, 0, 1024)) != 0)
                {
                    md5Hash.TransformBlock(buf, 0, length, null, 0);
                    sha256Hash.TransformBlock(buf, 0, length, null, 0);
                    fileStream.Write(buf, 0, length);
                }

                md5Hash.TransformFinalBlock(new byte[0], 0, 0);
                sha256Hash.TransformFinalBlock(new byte[0], 0, 0);
                md5 = Convert.ToBase64String(md5Hash.Hash);
                sha256 = BitConverter.ToString(sha256Hash.Hash).Replace("-", string.Empty).ToLower();
            }
        }
    }
}

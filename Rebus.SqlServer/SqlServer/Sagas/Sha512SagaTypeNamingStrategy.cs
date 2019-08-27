using System;
using System.Security.Cryptography;
using System.Text;
using Rebus.Extensions;

namespace Rebus.SqlServer.Sagas
{
    /// <summary>
    /// Implementation of <seealso cref="ISagaTypeNamingStrategy"/> that uses as many bytes as it can from an SHA2-512 hash to generat ea name
    /// </summary>
    /// <remarks>Note: SHA-2 is seemingly safe to truncate <a href="https://crypto.stackexchange.com/questions/161/should-i-use-the-first-or-last-bits-from-a-sha-256-hash/163#163">Reference</a></remarks>
    public class Sha512SagaTypeNamingStrategy : ISagaTypeNamingStrategy
    {
        /// <inheritdoc />
        public virtual string GetSagaTypeName(Type sagaDataType, int maximumLength)
        {
            return GetBase64EncodedPartialHash(sagaDataType.GetSimpleAssemblyQualifiedName(), maximumLength, Encoding.UTF8);
        }

        /// <summary>
        /// Returns a string of up to <paramref name="numberOfBytes"/> characters long that represent the Base64 encoded SHA2-512 hash of <paramref name="inputString"/>. The string will be encoded using <paramref name="encoding"/> first
        /// </summary>
        protected string GetBase64EncodedPartialHash(string inputString, int numberOfBytes, Encoding encoding)
        {
            using (SHA512Managed sha = new SHA512Managed())
            {
                var encodedBytes = encoding.GetBytes(inputString);
                var hashedBytes = sha.ComputeHash(encodedBytes, 0, encodedBytes.Length);
                var bytesToUseFromHash = GetMaximumBase64EncodedBytesThatFit(Math.Min(numberOfBytes, hashedBytes.Length));

                return Convert.ToBase64String(hashedBytes, 0, bytesToUseFromHash);
            }
        }

        /// <summary>
        /// Returns the number of bytes of bytes that can be encoded in Base64 and still fit in <paramref name="maximumSize"/>
        /// </summary>
        private static int GetMaximumBase64EncodedBytesThatFit(int maximumSize) => (int)Math.Floor(0.75f * maximumSize);
    }
}

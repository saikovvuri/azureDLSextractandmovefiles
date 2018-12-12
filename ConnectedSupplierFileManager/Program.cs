using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates; // Required only if you are using an Azure AD application created with certificates

using Microsoft.Rest;
using Microsoft.Rest.Azure.Authentication;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.Azure.Management.DataLake.Store.Models;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Azure.DataLake.Store;

namespace ConnectedSupplierFileManager
{
    public class Program
    {
        private static string _adlsg1AccountName = "{adls account}.azuredatalakestore.net";

        public static void Main(string[] args)  
        {
            
            // Service principal / appplication authentication with client secret / key
            // Use the client ID of an existing AAD "Web App" application.
            string TENANT = "{tenant or directory id}";
            string CLIENTID = "{client id of AAD app}";            
            string secret_key = "{client access key}";

            System.Uri ARM_TOKEN_AUDIENCE = new System.Uri(@"https://management.core.windows.net/");
            System.Uri ADL_TOKEN_AUDIENCE = new System.Uri(@"https://datalake.azure.net/");
            var armCreds = GetCreds_SPI_SecretKey(TENANT, ARM_TOKEN_AUDIENCE, CLIENTID, secret_key);
            var adlCreds = GetCreds_SPI_SecretKey(TENANT, ADL_TOKEN_AUDIENCE, CLIENTID, secret_key);

             AdlsClient client = AdlsClient.CreateClient(_adlsg1AccountName, adlCreds);

            try
            {               
                // The azure data lake store currently stores sales data collected over a period of 3 years per day per store of this retail customer

                // current folder hierarchy is "/platform/fact/sales/{year}/{month}/{day}/{store}
                // goal is to move a specific store's sales data into a folder hierarchy like 
                // "/platform/{store}/fact/sales/{year}/{month}/{day}"

                // Helper function to extract each day in a given date range
                var datesinRange = GetDatesBetweenARange(new DateTime(2016, 1, 1), new DateTime(2018, 12, 31));

                // for each day, lookup if there is sales data for the store in this case 'fabrikam' and then move it to a more organized (by store) folder path under the 
                // same data lake store account
                foreach (var dateItem in datesinRange)
                {
                    // build the folder path
                    var yearPartition = dateItem.ToString("yyyy");
                    var monthPartition = dateItem.ToString("MM");
                    var dayPartition = dateItem.ToString("dd");

                    var directoryPath = "/platform/fact/sales/" + yearPartition + "/" + monthPartition + "/" + dayPartition + "/fabrikam";
                    var destinationPath = "/newplatform/fabrikam/fact/sales/" + yearPartition + "/" + monthPartition + "/" + dayPartition;
                    if (client.CheckExists(directoryPath))
                    {
                        // lookup file items and move them to new path
                        foreach (var entry in client.EnumerateDirectory(directoryPath))
                        {
                            if (entry.Type == DirectoryEntryType.DIRECTORY)
                                continue;

                            if (!client.CheckExists(destinationPath))
                            {
                                // create destination folder recursively
                                client.CreateDirectory(destinationPath);
                            }
                            bool moveSuccessful = client.Rename(entry.FullName, destinationPath + "/" + entry.Name, true);
                        }
                    }
                }

                

            }
            catch (AdlsException e)
            {
                PrintAdlsException(e);
            }

            Console.WriteLine("Done. Press ENTER to continue ...");
            Console.ReadLine();
        }

        private static ServiceClientCredentials GetCreds_SPI_SecretKey(
       string tenant,
       Uri tokenAudience,
       string clientId,
       string secretKey)
        {
            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());

            var serviceSettings = ActiveDirectoryServiceSettings.Azure;
            serviceSettings.TokenAudience = tokenAudience;

            var creds = ApplicationTokenProvider.LoginSilentAsync(
             tenant,
             clientId,
             secretKey,
             serviceSettings).GetAwaiter().GetResult();
            return creds;
        }

        private static void PrintDirectoryEntry(DirectoryEntry entry)
        {
            Console.WriteLine($"Name: {entry.Name}");
            Console.WriteLine($"FullName: {entry.FullName}");
            Console.WriteLine($"Length: {entry.Length}");
            Console.WriteLine($"Type: {entry.Type}");
            Console.WriteLine($"User: {entry.User}");
            Console.WriteLine($"Group: {entry.Group}");
            Console.WriteLine($"Permission: {entry.Permission}");
            Console.WriteLine($"Modified Time: {entry.LastModifiedTime}");
            Console.WriteLine($"Last Accessed Time: {entry.LastAccessTime}");
            Console.WriteLine();
        }

        private static void PrintAdlsException(AdlsException exp)
        {
            Console.WriteLine("ADLException");
            Console.WriteLine($"   Http Status: {exp.HttpStatus}");
            Console.WriteLine($"   Http Message: {exp.HttpMessage}");
            Console.WriteLine($"   Remote Exception Name: {exp.RemoteExceptionName}");
            Console.WriteLine($"   Server Trace Id: {exp.TraceId}");
            Console.WriteLine($"   Exception Message: {exp.Message}");
            Console.WriteLine($"   Exception Stack Trace: {exp.StackTrace}");
            Console.WriteLine();
        }

        private static IEnumerable<DateTime> GetDatesBetweenARange(DateTime fromDate, DateTime toDate)
        {
            return Enumerable.Range(0, toDate.Subtract(fromDate).Days + 1)
                             .Select(d => fromDate.AddDays(d));
        }
    }
}
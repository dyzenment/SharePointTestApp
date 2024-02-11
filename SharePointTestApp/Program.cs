using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SharePointTestApp {
    internal class Program {

        static SharePointFileSystemService _sharePointService;

        static async Task Main(string[] args) {

            _sharePointService = new SharePointFileSystemService();

            bool exitApp = false;
            while (!exitApp) {
                Console.Clear();
                Console.WriteLine("SharePoint File Operations Demo");
                Console.WriteLine("");
                Console.WriteLine("Authentication Status: " + (_sharePointService.IsAuthenticated ? "Authenticated" : "Not Authenticated"));
                Console.WriteLine("");
                Console.WriteLine("To start type 'A' to authenticate and then start with 'H' to list some files.");
                Console.WriteLine("");
                Console.WriteLine("A. Authenticate");
                Console.WriteLine("B. Add Folder");
                Console.WriteLine("C. Copy File");
                Console.WriteLine("D. Delete File");
                Console.WriteLine("E. Delete Folder");
                Console.WriteLine("F. File Exists");
                Console.WriteLine("G. Folder Exists");
                Console.WriteLine("H. Get Files In Folder");
                Console.WriteLine("I. Get Folders In Folder");
                Console.WriteLine("J. Download File");
                Console.WriteLine("K. Upload File");
                Console.WriteLine("L. Move File");
                Console.WriteLine("Q. Exit");
                Console.Write("\nSelect an option: ");

                char choice = Char.ToUpper(Console.ReadLine().FirstOrDefault());
                
                switch (choice) {
                    case 'A':
                        await Authenticate();
                        break;
                    case 'B':
                        await AddFolder();
                        break;
                    case 'C':
                        await CopyFile();
                        break;
                    case 'D':
                        await DeleteFile();
                        break;
                    case 'E':
                        await DeleteFolder();
                        break;
                    case 'F':
                        await FileExists();
                        break;
                    case 'G':
                        await FolderExists();
                        break;
                    case 'H':
                        await GetFilesInFolder();
                        break;
                    case 'I':
                        await GetFoldersInFolder();
                        break;
                    case 'J':
                        await DownloadFile();
                        break;
                    case 'K':
                        await UploadFile();
                        break;
                    case 'L':
                        await MoveFile();
                        break;
                    case 'Q':
                        exitApp = true;
                        break;
                    default:
                        Console.WriteLine("Invalid option, please try again.");
                        break;
                }

                if (!exitApp) {
                    Console.WriteLine("\nPress any key to return to the menu...");
                    Console.ReadKey();
                }
            }


        }

        static (string SiteUrl, string AppId, string Username, string Password)? GetAuthenticationDetailsFromFile() {
            var desktopPath = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
            var filePath = desktopPath + "\\SharePointAuthInfo.txt";
            if (System.IO.File.Exists(filePath)) {
                // Parse the file to siteUrl, appId, username, password
                var fileContents = System.IO.File.ReadAllText(filePath);
                var data = Newtonsoft.Json.JsonConvert.DeserializeAnonymousType(fileContents, new { siteUrl = "", appId = "", username = "", password = "" });
                return (data.siteUrl, data.appId, data.username, data.password);
            }
            return null;
        }

        static void SaveAuthenticationDetailsToFile(string siteUrl, string appId, string username, string password) {
            var fileContents = (new { siteUrl = siteUrl, appId = appId, username = username, password = password });
            var desktopPath = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
            var filePath = desktopPath + "\\SharePointAuthInfo.txt";
            // Serialize fileContents to json
            var json = Newtonsoft.Json.JsonConvert.SerializeObject(fileContents);
            System.IO.File.WriteAllText(filePath, json);
            Console.WriteLine("Info Saved To: " + filePath);
        }

        static (string SiteUrl, string AppId, string Username, string Password)? GetAuthenticationDetailsFromUser() {
            Console.WriteLine("");
            Console.WriteLine("The site url typically looks like 'https://coldfreight.sharepoint.com/sites/sitenamehere'");
            Console.WriteLine("When you login to the web version you can see the name in the url of the browser.");
            Console.WriteLine("");
            Console.WriteLine("Enter Site URL: ");
            var siteUrl = Console.ReadLine();
            Console.WriteLine("Enter AppId / ClientId: ");
            var appId = Console.ReadLine();
            Console.WriteLine("Enter Username: ");
            var username = Console.ReadLine();
            Console.WriteLine("Enter Password: ");
            var password = ReadPassword();

            return (siteUrl, appId, username, password);
        }

        static async Task Authenticate() {

            // Check for desktop file
            var data = GetAuthenticationDetailsFromFile();
            if (data == null) {
                data = GetAuthenticationDetailsFromUser();
            }

            var (siteUrl, appId, username, password) = data.Value;
            
            Console.WriteLine("");
            Console.WriteLine("Authenticating...");
            var results = await _sharePointService.AuthenticateAsync(siteUrl, appId, username, password);
            if (results.HasErrors) {
                Console.WriteLine("Error: " + results.ErrorMessageSingleLine);
            } else {
                Console.WriteLine("Success");
            }
            Console.WriteLine("");
            Console.WriteLine("Save Info To Desktop File For Future Use? (Y/N): ");
            var saveInfo = Char.ToUpper(Console.ReadLine().FirstOrDefault());
            if (saveInfo == 'Y') {
                SaveAuthenticationDetailsToFile(siteUrl, appId, username, password);
            }
        }

        static async Task AddFolder() {
            Console.WriteLine("");
            Console.WriteLine("Add Folder");
            Console.WriteLine("");
            Console.WriteLine("Enter SharePoint Path: ");
            var path = Console.ReadLine();
            Console.WriteLine("Adding Folder...");
            var results = await _sharePointService.AddFolderAsync(path, true);
            if (results.HasErrors) {
                Console.WriteLine("Error: " + results.ErrorMessageSingleLine);
            } else {
                Console.WriteLine("Success");
            }
        }

        static async Task CopyFile() {
            Console.WriteLine("");
            Console.WriteLine("Copy File");
            Console.WriteLine("");
            Console.WriteLine("Source SharePoint Path: ");
            var fromPath = Console.ReadLine();
            Console.WriteLine("Destination SharePoint Path: ");
            var toPath = Console.ReadLine();
            Console.WriteLine("Copying File...");
            var results = await _sharePointService.CopyFileAsync(fromPath, toPath);
            if (results.HasErrors) {
                Console.WriteLine("Error: " + results.ErrorMessageSingleLine);
            } else {
                Console.WriteLine("Success");
            }
            
        }

        static async Task DeleteFile() {
            Console.WriteLine("");
            Console.WriteLine("Delete File");
            Console.WriteLine("");
            Console.WriteLine("Enter SharePoint Path: ");
            var path = Console.ReadLine();
            Console.WriteLine("Deleting File...");
            var results = await _sharePointService.DeleteFileAsync(path, true);
            if (results.HasErrors) {
                Console.WriteLine("Error: " + results.ErrorMessageSingleLine);
            } else {
                Console.WriteLine("Success");
            }
        }

        static async Task DeleteFolder() {
            Console.WriteLine("");
            Console.WriteLine("Delete Folder");
            Console.WriteLine("");
            Console.WriteLine("Enter SharePoint Path: ");
            var path = Console.ReadLine();
            Console.WriteLine("Deleting Folder...");
            var results = await _sharePointService.DeleteFolderAsync(path, true);
            if (results.HasErrors) {
                Console.WriteLine("Error: " + results.ErrorMessageSingleLine);
            } else {
                Console.WriteLine("Success");
            }
        }

        static async Task FileExists() {
            Console.WriteLine("");
            Console.WriteLine("Check File Existance");
            Console.WriteLine("");
            Console.WriteLine("Enter SharePoint Path: ");
            var path = Console.ReadLine();
            Console.WriteLine("Checking File Existance...");
            var results = await _sharePointService.FileExistAsync(path);
            if (results.HasErrors) {
                Console.WriteLine("Error: " + results.ErrorMessageSingleLine);
            } else {
                if (results.Value == true) {
                    Console.WriteLine("Success: File Exists");
                } else {
                    Console.WriteLine("Success: File Does Not Exist");
                }
            }
        }

        static async Task FolderExists() {
            Console.WriteLine("");
            Console.WriteLine("Check Folder Existance");
            Console.WriteLine("");
            Console.WriteLine("Enter SharePoint Path: ");
            var path = Console.ReadLine();
            Console.WriteLine("Checking Folder Existance...");
            var results = await _sharePointService.FileExistAsync(path);
            if (results.HasErrors) {
                Console.WriteLine("Error: " + results.ErrorMessageSingleLine);
            } else {
                if (results.Value == true) {
                    Console.WriteLine("Success: Folder Exists");
                } else {
                    Console.WriteLine("Success: Folder Does Not Exist");
                }
            }
        }

        static async Task GetFilesInFolder() {
            Console.WriteLine("");
            Console.WriteLine("This will be a relative path. So it will start with the name of the document library ending with /");
            Console.WriteLine("If you don't know where to start type 'Shared Documents/'");
            Console.WriteLine("");
            Console.WriteLine("List All Files In Folder");
            Console.WriteLine("");
            Console.WriteLine("Enter SharePoint Path: ");
            var path = Console.ReadLine();
            Console.WriteLine("Getting Files in Folder...");
            var results = await _sharePointService.GetFilesInFolderAsync(path);
            if (results.HasErrors) {
                Console.WriteLine("Error: " + results.ErrorMessageSingleLine);
            } else {
                Console.WriteLine("Success");
                Console.WriteLine("");
                Console.WriteLine("List Of Files: (Count: " + results.Value.Count + ")");
                Console.WriteLine("");
                for (var i = 0; i < results.Value.Count; i++)
                {
                    var file = results.Value[i];
                    Console.WriteLine(file);
                }
            }
        }

        static async Task GetFoldersInFolder() {
            Console.WriteLine("");
            Console.WriteLine("List All Folders In Folder");
            Console.WriteLine("");
            Console.WriteLine("Enter SharePoint Path: ");
            var path = Console.ReadLine();
            Console.WriteLine("Getting Folders in Folder...");
            var results = await _sharePointService.GetFoldersInFolderAsync(path);
            if (results.HasErrors) {
                Console.WriteLine("Error: " + results.ErrorMessageSingleLine);
            } else {
                Console.WriteLine("Success");
                Console.WriteLine("");
                Console.WriteLine("List Of Folders: (Count: " + results.Value.Count + ")");
                Console.WriteLine("");
                for (var i = 0; i < results.Value.Count; i++) {
                    var folder = results.Value[i];
                    Console.WriteLine(folder);
                }
            }
        }

        static async Task DownloadFile() {
            Console.WriteLine("");
            Console.WriteLine("Download File (will save to desktop)");
            Console.WriteLine("");
            Console.WriteLine("Enter SharePoint Path: ");
            var path = Console.ReadLine();
            Console.WriteLine("Downloading File...");
            var results = await _sharePointService.GetFileStreamAsync(path);
            if (results.HasErrors) {
                Console.WriteLine("Error: " + results.ErrorMessageSingleLine);
            } else {
                Console.WriteLine("Success");
                Console.WriteLine("Saving File...");
                using (var sourceStream = results.Value) {
                    string desktopPath = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
                    var filename = System.IO.Path.GetFileName(path);
                    var desinationPath = System.IO.Path.Combine(desktopPath, filename); 
                    using (var destinationStream = System.IO.File.OpenWrite(desinationPath)) {
                        sourceStream.CopyTo(destinationStream);
                        destinationStream.Close();
                    }
                    Console.WriteLine("Saved file: " + filename);
                }
            }
        }

        static async Task UploadFile() {
            Console.WriteLine("");
            Console.WriteLine("Upload File");
            Console.WriteLine("");
            Console.WriteLine("Enter Source/Local Path of File (or drag/drop file): ");
            var fromPath = Console.ReadLine();
            Console.WriteLine("");
            Console.WriteLine("Enter Destination/SharePoint Path: ");
            var toPath = Console.ReadLine();
            using (var localFile = System.IO.File.OpenRead(fromPath)) {

                Console.WriteLine("Uploading File...");
                var results = await _sharePointService.SaveFileStreamAsync(toPath, localFile);
                if (results.HasErrors) {
                    Console.WriteLine("Error: " + results.ErrorMessageSingleLine);
                } else {
                    Console.WriteLine("Success");
                    Console.WriteLine("Saved at " + results.Value);
                }
            }
        }

        static async Task MoveFile() {
            Console.WriteLine("");
            Console.WriteLine("Add Folder");
            Console.WriteLine("");
            Console.WriteLine("Source Path: ");
            var fromPath = Console.ReadLine();
            Console.WriteLine("Destination Path: ");
            var toPath = Console.ReadLine();
            Console.WriteLine("Moving File...");
            var results = await _sharePointService.MoveFileAsync(fromPath, toPath);
            if (results.HasErrors) {
                Console.WriteLine("Error: " + results.ErrorMessageSingleLine);
            } else {
                Console.WriteLine("Success");
            }
        }
        static string ReadPassword() {
            string password = "";
            while (true) {
                ConsoleKeyInfo info = Console.ReadKey(true);
                if (info.Key == ConsoleKey.Enter) {
                    break;
                } else if (info.Key == ConsoleKey.Backspace) {
                    if (password.Length > 0) {
                        password = password.Substring(0, password.Length - 1);
                        // Remove the last asterisk from console
                        Console.Write("\b \b");
                    }
                } else {
                    password += info.KeyChar;
                    Console.Write("*");
                }
            }
            return password;
        }
    }
}

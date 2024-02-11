using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Security;
using System.Threading;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.SharePoint.Client;
using Newtonsoft.Json.Linq;

namespace SharePointTestApp {

    public class SharePointFileSystemService : IDisposable {

        private ClientContext _clientContext;
        private bool disposedValue;
        private readonly IMemoryCache _memoryCache = new MemoryCache(new MemoryCacheOptions());
        private static readonly SemaphoreSlim semaphoreSlimTokens = new SemaphoreSlim(1);
        private string _accessToken;
        private DateTimeOffset? _accessTokenExpiration;
        private const int _maxMemoryCacheMinutes = 1;
        private const string _tokenEndpoint = "https://login.microsoftonline.com/common/oauth2/token";

        public SharePointFileSystemService() {

        }

        public Task<Result> AuthenticateAsync(string siteUrl, string appId, string username, string password) {
            return AuthenticateAsync(new Uri(siteUrl), appId, username, password);
        }

        public Result Authenticate(string siteUrl, string appId, string username, string password) {
            return Authenticate(new Uri(siteUrl), appId, username, password);
        }

        public async Task<Result> AuthenticateAsync(Uri siteUrl, string appId, string username, string password) {
            System.Net.ServicePointManager.SecurityProtocol |= System.Net.SecurityProtocolType.Tls12;

            // Check connection
            var accessTokenResult = await GetAccessTokenAsync(siteUrl, appId, username, password);
            if (accessTokenResult.HasErrors) {
                return Result.Error(accessTokenResult.Errors);
            }

            _clientContext = GetClientContext(siteUrl, appId, username, password);

            return Result.Successful();
        }

        public Result Authenticate(Uri siteUrl, string appId, string username, string password) {
            System.Net.ServicePointManager.SecurityProtocol |= System.Net.SecurityProtocolType.Tls12;

            // Check connection
            var accessTokenResult = GetAccessToken(siteUrl, appId, username, password);
            if (accessTokenResult.HasErrors) {
                return Result.Error(accessTokenResult.Errors);
            }

            _clientContext = GetClientContext(siteUrl, appId, username, password);

            return Result.Successful();
        }

        public bool IsAuthenticated {
            get {
                return _clientContext != null;
            }
        }

        private ClientContext GetClientContext(Uri siteUrl, string appId, string userPrincipalName, string userPassword) {
            var clientContext = new ClientContext(siteUrl);

            clientContext.ExecutingWebRequest += (sender, e) => {
                string accessToken = GetAccessTokenAsync(new Uri($"{siteUrl.Scheme}://{siteUrl.DnsSafeHost}"), appId, userPrincipalName, userPassword).GetAwaiter().GetResult();
                e.WebRequestExecutor.RequestHeaders["Authorization"] = "Bearer " + accessToken;
            };

            return clientContext;
        }

        public async Task<Result<string>> GetAccessTokenAsync(Uri resourceUri, string appId, string userPrincipalName, string userPassword) {

            if (_accessToken != null && _accessTokenExpiration > DateTimeOffset.UtcNow) {
                return Result.Successful(_accessToken);
            }

            if (_memoryCache == null) {
                var newAccessTokenResult = await AcquireTokenAsync(resourceUri, appId, userPrincipalName, userPassword).ConfigureAwait(false);
                if (newAccessTokenResult.HasErrors) {
                    return Result.Error(newAccessTokenResult.Errors);
                }
                var newAccessToken = newAccessTokenResult.Value;
                _accessToken = newAccessToken.AccessToken;
                _accessTokenExpiration = newAccessToken.Expiration;
                return Result.Successful(_accessToken);
            }

            var key = "SharePoint" + resourceUri.ToString() + "AppId" + appId + "User" + userPrincipalName;

            if (_memoryCache.TryGetValue<(string AccessToken, DateTimeOffset? Expiration)>(key, out var accessTokenAndExpiration)
                && accessTokenAndExpiration.AccessToken != null
                && (accessTokenAndExpiration.Expiration == null || accessTokenAndExpiration.Expiration > DateTimeOffset.UtcNow)) {
                return Result.Successful(accessTokenAndExpiration.AccessToken);
            }

            // Since memory cache is thread safe but does not lock, we will add a semaphoreslim to ensure only one thread is getting the access token at one time.
            await semaphoreSlimTokens.WaitAsync().ConfigureAwait(false);

            try {
                // Ensure another thread has already got the token.
                var exists = _memoryCache.TryGetValue<(string AccessToken, DateTimeOffset? Expiration)>(key, out var secondTryAccessToken);
                if (exists) {// && secondTryAccessToken != null
                             // Check if the token is null 
                    if (secondTryAccessToken.AccessToken == null) {
                        _memoryCache.Remove(key);
                    } else if (secondTryAccessToken.Expiration != null && secondTryAccessToken.Expiration <= DateTimeOffset.UtcNow) {
                        _memoryCache.Remove(key);
                    } else {
                        return Result.Successful(secondTryAccessToken.AccessToken);
                    }
                }

                // Get a new token
                var newAccessTokenResult = await AcquireTokenAsync(resourceUri, appId, userPrincipalName, userPassword).ConfigureAwait(false);
                if (newAccessTokenResult.HasErrors) {
                    return Result.Error(newAccessTokenResult.Errors);
                }
                var newAccessToken = newAccessTokenResult.Value;

                var maxCacheTime = DateTimeOffset.UtcNow.AddMinutes(_maxMemoryCacheMinutes);
                var cacheTime = newAccessToken.Expiration == null || newAccessToken.Expiration > maxCacheTime ? maxCacheTime : newAccessToken.Expiration.Value;

                // Add to the memory cache.
                _memoryCache.Set(key, newAccessToken, cacheTime);

                _accessToken = newAccessToken.AccessToken;
                _accessTokenExpiration = newAccessToken.Expiration;

                return Result.Successful(_accessToken);
            } finally {
                semaphoreSlimTokens.Release();
            }

        }

        public Result<string> GetAccessToken(Uri resourceUri, string appId, string userPrincipalName, string userPassword) {

            if (_accessToken != null && _accessTokenExpiration > DateTimeOffset.UtcNow) {
                return Result.Successful(_accessToken);
            }

            if (_memoryCache == null) {
                var newAccessTokenResult = AcquireToken(resourceUri, appId, userPrincipalName, userPassword);
                if (newAccessTokenResult.HasErrors) {
                    return Result.Error(newAccessTokenResult.Errors);
                }
                var newAccessToken = newAccessTokenResult.Value;
                _accessToken = newAccessToken.AccessToken;
                _accessTokenExpiration = newAccessToken.Expiration;
                return Result.Successful(_accessToken);
            }

            var key = "SharePoint" + resourceUri.ToString() + "AppId" + appId + "User" + userPrincipalName;

            if (_memoryCache.TryGetValue<(string AccessToken, DateTimeOffset? Expiration)>(key, out var accessTokenAndExpiration)
                && accessTokenAndExpiration.AccessToken != null
                && (accessTokenAndExpiration.Expiration == null || accessTokenAndExpiration.Expiration > DateTimeOffset.UtcNow)) {
                return Result.Successful(accessTokenAndExpiration.AccessToken);
            }

            // Since memory cache is thread safe but does not lock, we will add a semaphoreslim to ensure only one thread is getting the access token at one time.
            semaphoreSlimTokens.Wait();

            try {
                // Ensure another thread has already got the token.
                var exists = _memoryCache.TryGetValue<(string AccessToken, DateTimeOffset? Expiration)>(key, out var secondTryAccessToken);
                if (exists) {// && secondTryAccessToken != null
                             // Check if the token is null 
                    if (secondTryAccessToken.AccessToken == null) {
                        _memoryCache.Remove(key);
                    } else if (secondTryAccessToken.Expiration != null && secondTryAccessToken.Expiration <= DateTimeOffset.UtcNow) {
                        _memoryCache.Remove(key);
                    } else {
                        return Result.Successful(secondTryAccessToken.AccessToken);
                    }
                }

                // Get a new token
                var newAccessTokenResult = AcquireToken(resourceUri, appId, userPrincipalName, userPassword);
                if (newAccessTokenResult.HasErrors) {
                    return Result.Error(newAccessTokenResult.Errors);
                }
                var newAccessToken = newAccessTokenResult.Value;

                var maxCacheTime = DateTimeOffset.UtcNow.AddMinutes(_maxMemoryCacheMinutes);
                var cacheTime = newAccessToken.Expiration == null || newAccessToken.Expiration > maxCacheTime ? maxCacheTime : newAccessToken.Expiration.Value;

                // Add to the memory cache.
                _memoryCache.Set(key, newAccessToken, cacheTime);

                _accessToken = newAccessToken.AccessToken;
                _accessTokenExpiration = newAccessToken.Expiration;

                return Result.Successful(_accessToken);
            } finally {
                semaphoreSlimTokens.Release();
            }

        }

        private async Task<Result<(string AccessToken, DateTimeOffset? Expiration)>> AcquireTokenAsync(Uri resourceUri, string appId, string username, string password) {
            string resource = $"{resourceUri.Scheme}://{resourceUri.DnsSafeHost}";

            var now = DateTimeOffset.UtcNow;

            var clientId = appId;
            var body = $"resource={resource}&client_id={clientId}&grant_type=password&username={WebUtility.UrlEncode(username)}&password={WebUtility.UrlEncode(password)}";
            using (var stringContent = new StringContent(body, Encoding.UTF8, "application/x-www-form-urlencoded"))
            using (var httpClient = new HttpClient()) {
                var result = await httpClient.PostAsync(_tokenEndpoint, stringContent).ContinueWith((response) => {
                    return response.Result.Content.ReadAsStringAsync().Result;
                }).ConfigureAwait(false);

                var tokenResult = JObject.Parse(result); //JsonSerializer..Deserialize(result); //Newtonsoft.Json.JsonConvert.DeserializeObject(result);

                if (!tokenResult.TryGetValue("access_token", out JToken accessTokenToken)
                    || accessTokenToken == null
                    || accessTokenToken.Type == JTokenType.Null) {
                    return Result.Error(result);
                }

                string accessToken = accessTokenToken.ToString();
                DateTimeOffset? expiration = null;

                if (tokenResult.TryGetValue("expires_in", out JToken expiresInToken)
                    && expiresInToken != null
                    && expiresInToken.Type != JTokenType.Null) {
                    if (int.TryParse(expiresInToken.ToString(), out var expiresInSeconds)) {
                        expiration = now.AddSeconds(expiresInSeconds);
                    }

                }

                return Result.Successful((accessToken, expiration));
            }
        }

        private Result<(string AccessToken, DateTimeOffset? Expiration)> AcquireToken(Uri resourceUri, string appId, string username, string password) {
            string resource = $"{resourceUri.Scheme}://{resourceUri.DnsSafeHost}";

            var now = DateTimeOffset.UtcNow;

            var clientId = appId;
            var body = $"resource={resource}&client_id={clientId}&grant_type=password&username={WebUtility.UrlEncode(username)}&password={WebUtility.UrlEncode(password)}";
            using (var stringContent = new StringContent(body, Encoding.UTF8, "application/x-www-form-urlencoded"))
            using (var httpClient = new HttpClient()) {
                
                var task = Task.Run(() => httpClient.PostAsync(_tokenEndpoint, stringContent));
                task.Wait();
                var result = task.Result.Content.ReadAsStringAsync().Result;

                var tokenResult = JObject.Parse(result); //JsonSerializer..Deserialize(result); //Newtonsoft.Json.JsonConvert.DeserializeObject(result);

                if (!tokenResult.TryGetValue("access_token", out JToken accessTokenToken)
                    || accessTokenToken == null
                    || accessTokenToken.Type == JTokenType.Null) {
                    return Result.Error(result);
                }

                string accessToken = accessTokenToken.ToString();
                DateTimeOffset? expiration = null;

                if (tokenResult.TryGetValue("expires_in", out JToken expiresInToken)
                    && expiresInToken != null
                    && expiresInToken.Type != JTokenType.Null) {
                    if (int.TryParse(expiresInToken.ToString(), out var expiresInSeconds)) {
                        expiration = now.AddSeconds(expiresInSeconds);
                    }

                }

                return Result.Successful((accessToken, expiration));
            }
        }

        public async Task<Result> AddFolderAsync(string path, bool errorIfAlreadyExists = false, bool recursiveAdd = true) {
            if (_clientContext == null) {
                return Result.Error($"You must authenticate first. Call {nameof(AuthenticateAsync)}.");
            }
            if (string.IsNullOrWhiteSpace(path)) {
                return Result.Error("The url is empty");
            }
            path = NormalizeRelativeUrl(path);
            var directories = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (directories.Last() != String.Empty) {
                // We have reached the root.
                return Result.Error("The url must be a folder. Folder urls must end with a /");
            }
            directories = directories.Take(directories.Length - 1).ToArray();
            var folderExists = await FolderExistAsync(path);
            if (folderExists.HasErrors) {
                return Result.Error(folderExists.Errors);
            }
            if (folderExists.Value == true) {
                return errorIfAlreadyExists
                    ? Result.Error($"The folder '{path}' already exists.")
                    : Result.Successful();
            }
            // The folder doesnt exist.
            if (directories.Length == 1) {
                // We have reached the root.
                return Result.Error("The root folder does not exist.");
            }
            var parentSiteRelativeUrl = string.Join("/", directories.Take(directories.Length - 1)) + "/";
            var folderName = directories.Last();

            if (recursiveAdd) {
                var addParent = await AddFolderAsync(parentSiteRelativeUrl, false, true);
                if (addParent.HasErrors) {
                    return addParent;
                }
            } else {
                var parentFolderExists = await FolderExistAsync(parentSiteRelativeUrl);
                if (parentFolderExists.HasErrors) {
                    return Result.Error(parentFolderExists.Errors);
                }
                if (parentFolderExists.Value == false) {
                    return Result.Error("The parent folder does not exist.");
                }
            }

            // The parent folder exists here.
            // The folder needs to be created.
            var serverRelativeUrl = GetServerRelativeUrl(path);
            var folder = _clientContext.Web.Folders.Add(serverRelativeUrl);
            try {
                await _clientContext.ExecuteQueryAsync();
            } catch (Microsoft.SharePoint.Client.ServerException ex) {
                if (ex.ServerErrorCode == -2147024894) {
                    // serverRelativeUrl doesn't correspond to a folder
                    return Result.Error("The folder can not be created.");
                }
                return Result.Error(ex.Message);
            } catch (Exception ex) {
                //Console.WriteLine("Could not find folder.");
                return Result.Error(ex.Message);
            }

            var verification = await FolderExistAsync(path);
            if (verification.HasErrors) {
                return Result.Error(verification.Errors);
            }
            return verification.Value == false ? Result.Error("The folder was not created.") : Result.Successful();
        }

        public async Task<Result> CopyFileAsync(string fromPath, string toPath, ConflictResolution conflictResolution = ConflictResolution.Error, bool createFoldersIfDoesntExist = false) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(fromPath)) {
                return Result.Error("The from url is empty");
            }
            fromPath = NormalizeRelativeUrl(fromPath);
            toPath = NormalizeRelativeUrl(toPath);
            var fromDirectories = fromPath.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (fromDirectories.Last() == String.Empty) {
                // We have reached the root.
                return Result.Error("The url must be a file. File urls must not end with a /");
            }
            if (string.IsNullOrWhiteSpace(toPath)) {
                return Result.Error("The to url is empty");
            }
            var fromServerRelativeUrl = GetServerRelativeUrl(fromPath);
            var file = _clientContext.Web.GetFileByServerRelativeUrl(fromServerRelativeUrl);
            _clientContext.Load(file);
            try {
                await _clientContext.ExecuteQueryAsync();
            } catch (Exception) {

                throw;
            }

            string toFolderSiteRelativeUrl;
            string toFilename;
            var toDirectories = toPath.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (toDirectories.Last() == String.Empty) {
                // The destination is a folder with no filename
                // First we need to split the filename from the path
                //var fromFolderSiteRelativeUrl = string.Join("/", fromDirectories.Take(fromDirectories.Length - 1));
                toFilename = fromDirectories.Last();
                toFolderSiteRelativeUrl = toPath;
            } else {
                // The destination has a filename
                toFilename = toDirectories.Last();
                toFolderSiteRelativeUrl = string.Join("/", toDirectories.Take(toDirectories.Length - 1)) + "/";
            }

            if (createFoldersIfDoesntExist) {
                var toFolderExists = await AddFolderAsync(toFolderSiteRelativeUrl, false, true);
                if (toFolderExists.HasErrors) {
                    return toFolderExists;
                }
            } else {
                var toFolderExists = await FolderExistAsync(toFolderSiteRelativeUrl);
                if (toFolderExists.HasErrors) {
                    return Result.Error(toFolderExists.Errors);
                }
                if (toFolderExists.Value == false) {
                    return Result.Error("The destination folder does not exist.");
                }
            }

            if (conflictResolution == ConflictResolution.MakeFilenameUnique) {
                // Get the destination filename
                var filesInFolder = await GetFilesInFolderAsync(toFolderSiteRelativeUrl);
                if (filesInFolder.HasErrors) {
                    return Result.Error(filesInFolder.Errors);
                }
                var foldersInFolder = await GetFoldersInFolderAsync(toFolderSiteRelativeUrl);
                if (foldersInFolder.HasErrors) {
                    return Result.Error(foldersInFolder.Errors);
                }
                var existingNames = filesInFolder.Value.Union(foldersInFolder.Value);
                toFilename = CreateUniqueFilename(toFilename, existingNames);

            }

            toPath = $"{toFolderSiteRelativeUrl}{toFilename}";

            var toServerRelativeUrl = GetServerRelativeUrl(toPath);
            file.CopyTo(toServerRelativeUrl, conflictResolution == ConflictResolution.Overwrite);
            try {
                _clientContext.ExecuteQuery();
            } catch (Microsoft.SharePoint.Client.ServerException ex) {
                return ex.ServerErrorCode == -2130575257
                    ? Result.Error("The file already exists.")
                    : Result.Error(ex.Message);
            } catch (Exception ex) {
                return Result.Error(ex.Message);
            }

            var verification1 = await FileExistAsync(toPath);
            if (!verification1.HasErrors) {
                return Result.Successful();
            }
            if (verification1.Value == false) {
                return Result.Error("The file was not moved.");
            }

            return Result.Successful();
        }

        public async Task<Result> DeleteFileAsync(string path, bool errorIfFileDoesntExist = false) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(path)) {
                return Result.Error("The url is empty");
            }
            path = NormalizeRelativeUrl(path);
            var directories = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (directories.Last() == String.Empty) {
                return Result.Error("The url provided does not have a filename. Files do not end with /");
            }
            var serverRelativeUrl = GetServerRelativeUrl(path);

            if (errorIfFileDoesntExist) {
                var fileExist = await FileExistAsync(path);
                if (fileExist.HasErrors) {
                    return Result.Error(fileExist.Errors);
                }
                if (fileExist.Value == false) {
                    return Result.Error("The file does not exist.");
                }
            }

            var file = _clientContext.Web.GetFileByServerRelativeUrl(serverRelativeUrl);
            file.DeleteObject();
            try {
                await _clientContext.ExecuteQueryAsync();
            } catch (Exception) {

                throw;
            }
            var verification = await FileExistAsync(path);
            if (verification.HasErrors) {
                return Result.Error(verification.Errors);
            }
            return verification.Value == true ? Result.Error("The file was not deleted.") : Result.Successful();
        }

        public async Task<Result> DeleteFolderAsync(string path, bool errorIfFolderDoesntExist = false, bool errorIfFolderIsNotEmpty = true) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(path)) {
                return Result.Error("The url is empty");
            }
            path = NormalizeRelativeUrl(path);
            var directories = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (directories.Last() != String.Empty) {
                // We have reached the root.
                return Result.Error("The url must be a folder. Folder urls must end with a /");
            }
            //directories = directories.Take(directories.Length - 1).ToArray();
            var serverRelativeUrl = GetServerRelativeUrl(path);

            var folderExist = await FolderExistAsync(path);
            if (folderExist.HasErrors) {
                return Result.Error(folderExist.Errors);
            }
            if (errorIfFolderDoesntExist) {
                if (folderExist.Value == false) {
                    return Result.Error("The folder does not exist.");
                }
            } else {
                if (folderExist.Value == false) {
                    return Result.Successful();
                }
            }

            if (errorIfFolderIsNotEmpty) {
                var filesInFolder = await GetFilesInFolderAsync(path);
                if (filesInFolder.HasErrors) {
                    return Result.Error(filesInFolder.Errors);
                }
                if (filesInFolder.Value.Count > 0) {
                    return Result.Error($"There are {filesInFolder.Value.Count} files in this folder.");
                }
                var foldersInFolder = await GetFoldersInFolderAsync(path);
                if (foldersInFolder.HasErrors) {
                    return Result.Error(foldersInFolder.Errors);
                }
                if (foldersInFolder.Value.Count > 0) {
                    return Result.Error($"There are {foldersInFolder.Value.Count} folders in this folder.");
                }
            }

            var folder = _clientContext.Web.GetFolderByServerRelativeUrl(serverRelativeUrl);
            folder.DeleteObject();
            try {
                await _clientContext.ExecuteQueryAsync();
            } catch (Exception ex) {
                return Result.Error(ex.Message);
            }
            var verification = await FolderExistAsync(path);
            if (verification.HasErrors) {
                return Result.Error(verification.Errors);
            }
            return verification.Value == true ? Result.Error("The folder was not deleted.") : Result.Successful();
        }

        public async Task<Result<bool>> FileExistAsync(string path) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(path)) {
                return Result.Error("The url is empty");
            }
            path = NormalizeRelativeUrl(path);
            var directories = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (directories.Last() == String.Empty) {
                // We have reached the root.
                return Result.Error("The url must be a file. File urls must not end with a /");
            }
            var serverRelativeUrl = GetServerRelativeUrl(path);
            var file = _clientContext.Web.GetFileByServerRelativeUrl(serverRelativeUrl);
            _clientContext.Load(file, f => f.Exists);
            try {
                await _clientContext.ExecuteQueryAsync();

                return file.Exists ? Result.Successful(true) : Result.Successful(false);
            } catch (ServerUnauthorizedAccessException) {
                return Result.Error("You are not allowed to access this folder");
            } catch (Exception ex) {
                return Result.Error(ex.Message);
            }
        }

        public async Task<Result<bool>> FolderExistAsync(string path) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(path)) {
                return Result.Error("The url is empty");
            }
            path = NormalizeRelativeUrl(path);
            var directories = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (directories.Last() != String.Empty) {
                // We have reached the root.
                return Result.Error("The url must be a folder. Folder urls must end with a /");
            }
            var serverRelativeUrl = GetServerRelativeUrl(path);
            var folder = _clientContext.Web.GetFolderByServerRelativeUrl(serverRelativeUrl);
            _clientContext.Load(folder, f => f.Exists);
            try {
                await _clientContext.ExecuteQueryAsync();

                return folder.Exists ? Result.Successful(true) : Result.Successful(false);
            } catch (ServerUnauthorizedAccessException) {
                return Result.Error("You are not allowed to access this folder");
            } catch (Microsoft.SharePoint.Client.ServerException ex) {
                if (ex.ServerErrorCode == -2147024894) {
                    // serverRelativeUrl doesn't correspond to a folder
                    return Result.Successful(false);
                }
                return Result.Error(ex);
            } catch (Exception ex) {
                return Result.Error(ex);
            }
        }

        public async Task<Result<List<string>>> GetFilesInFolderAsync(string path) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(path)) {
                return Result.Error("The url is empty");
            }
            path = NormalizeRelativeUrl(path);
            var directories = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (directories.Last() != String.Empty) {
                // We have reached the root.
                return Result.Error("The url must be a folder. Folder urls must end with a /");
            }
            //directories = directories.Take(directories.Length - 1).ToArray();
            var folderServerRelativeUrl = GetServerRelativeUrl(path);
            FileCollection files = _clientContext.Web.GetFolderByServerRelativeUrl(folderServerRelativeUrl).Files;
            _clientContext.Load(files);
            try {
                await _clientContext.ExecuteQueryAsync();
            } catch (Exception ex) {
                return Result.Error(ex);
            }

            return Result.Successful(files.Select(e => e.Name).ToList());
        }

        public async Task<Result<Stream>> GetFileStreamAsync(string path) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(path)) {
                return Result.Error("The url is empty");
            }
            path = NormalizeRelativeUrl(path);
            var directories = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (directories.Last() == String.Empty) {
                // We have reached the root.
                return Result.Error("The url must be a file. File urls must not end with a /");
            }
            var fileServerRelativeUrl = GetServerRelativeUrl(path);
            var file = _clientContext.Web.GetFileByServerRelativeUrl(fileServerRelativeUrl);
            var clientResult = file.OpenBinaryStream(); // has an options version
            _clientContext.Load(file);
            try {
                await _clientContext.ExecuteQueryAsync();
            } catch (Exception ex) {
                return Result.Error(ex);
            }

            return Result.Successful(clientResult.Value);
        }

        public async Task<Result<List<string>>> GetFoldersInFolderAsync(string path) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(path)) {
                return Result.Error("The url is empty");
            }
            path = NormalizeRelativeUrl(path);
            var directories = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (directories.Last() != String.Empty) {
                // We have reached the root.
                return Result.Error("The url must be a folder. Folder urls must end with a /");
            }
            //directories = directories.Take(directories.Length - 1).ToArray();
            var folderServerRelativeUrl = GetServerRelativeUrl(path);
            FolderCollection folders = _clientContext.Web.GetFolderByServerRelativeUrl(folderServerRelativeUrl).Folders;
            _clientContext.Load(folders);
            try {
                await _clientContext.ExecuteQueryAsync();
            } catch (Exception) {

                throw;
            }

            return Result.Successful(folders.Select(e => e.Name).ToList());
        }

        public string CreateUniqueFilename(string filename, IEnumerable<string> existingFiles) {
            if (existingFiles.Any(e => string.Compare(e, filename, true) == 0)) {
                var filenameWithoutExtension = System.IO.Path.GetFileNameWithoutExtension(filename);
                var extension = System.IO.Path.GetExtension(filename);
                var appendedMatches = System.Text.RegularExpressions.Regex.Match(filenameWithoutExtension, @"^(.*?)(?:\(([0-9]*)\)|)$");
                var numberToAppend = 2;
                if (appendedMatches.Success == true) {
                    var number = appendedMatches.Groups[2].Value;
                    if (int.TryParse(number, out var numberInt)) {
                        numberToAppend = numberInt + 1;
                        filenameWithoutExtension = appendedMatches.Groups[1].Value;
                    }
                }
                var lastChar = filenameWithoutExtension.LastOrDefault();
                if (char.IsWhiteSpace(lastChar)) {
                    filename = $"{filenameWithoutExtension}({numberToAppend:0000}){extension}";
                } else {
                    filename = $"{filenameWithoutExtension} ({numberToAppend:0000}){extension}";
                }

                return CreateUniqueFilename(filename, existingFiles);
            } else {
                return filename;
            }
        }

        public async Task<Result> MoveFileAsync(string fromPath, string toPath, ConflictResolution conflictResolution = ConflictResolution.Error, bool createFoldersIfDoesntExist = false) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(fromPath)) {
                return Result.Error("The from url is empty");
            }
            fromPath = NormalizeRelativeUrl(fromPath);
            toPath = NormalizeRelativeUrl(toPath);
            var fromDirectories = fromPath.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (fromDirectories.Last() == String.Empty) {
                // We have reached the root.
                //Storage.AddSystemLogEntry(Storage.LogCategory.Debug, "ScanProcessing.cs:1500", "The url must be a file. File urls must not end with a /");
                return Result.Error("The url must be a file. File urls must not end with a /");
            }
            if (string.IsNullOrWhiteSpace(toPath)) {
                //Storage.AddSystemLogEntry(Storage.LogCategory.Debug, "ScanProcessing.cs:1504", "The to url is empty");
                return Result.Error("The to url is empty");
            }
            var fromServerRelativeUrl = GetServerRelativeUrl(fromPath);
            //Storage.AddSystemLogEntry(Storage.LogCategory.Debug, "ScanProcessing.cs:1508", "Init GetFileByServerRelativeUrl");
            var file = _clientContext.Web.GetFileByServerRelativeUrl(fromServerRelativeUrl);
            //Storage.AddSystemLogEntry(Storage.LogCategory.Debug, "ScanProcessing.cs:1510", $"Init Load");
            _clientContext.Load(file);
            try {
                //Storage.AddSystemLogEntry(Storage.LogCategory.Debug, "ScanProcessing.cs:219", $"Init ExecuteQuery");
                await _clientContext.ExecuteQueryAsync();
            } catch (Exception ex) {
                return Result.Error(ex);
            }

            string toFolderSiteRelativeUrl;
            string toFilename;
            var toDirectories = toPath.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (toDirectories.Last() == String.Empty) {
                // The destination is a folder with no filename
                // First we need to split the filename from the path
                //var fromFolderSiteRelativeUrl = string.Join("/", fromDirectories.Take(fromDirectories.Length - 1));
                toFilename = fromDirectories.Last();
                toFolderSiteRelativeUrl = toPath;
            } else {
                // The destination has a filename
                toFilename = toDirectories.Last();
                toFolderSiteRelativeUrl = string.Join("/", toDirectories.Take(toDirectories.Length - 1)) + "/";
            }

            if (createFoldersIfDoesntExist) {
                var toFolderExists = await AddFolderAsync(toFolderSiteRelativeUrl, false, true);
                if (toFolderExists.HasErrors) {
                    return toFolderExists;
                }
            } else {
                var toFolderExists = await FolderExistAsync(toFolderSiteRelativeUrl);
                if (toFolderExists.HasErrors) {
                    return Result.Error(toFolderExists.Errors);
                }
                if (toFolderExists.Value == false) {
                    return Result.Error("The destination folder does not exist.");
                }
            }

            if (conflictResolution == ConflictResolution.MakeFilenameUnique) {
                // Get the destination filename

                var filesInFolder = await GetFilesInFolderAsync(toFolderSiteRelativeUrl);
                if (filesInFolder.HasErrors) {
                    return Result.Error(filesInFolder.Errors);
                }
                var foldersInFolder = await GetFoldersInFolderAsync(toFolderSiteRelativeUrl);
                if (foldersInFolder.HasErrors) {
                    return Result.Error(foldersInFolder.Errors);
                }
                var existingNames = filesInFolder.Value.Union(foldersInFolder.Value);
                toFilename = CreateUniqueFilename(toFilename, existingNames);

            }

            toPath = $"{toFolderSiteRelativeUrl}{toFilename}";

            var toServerRelativeUrl = GetServerRelativeUrl(toPath);
            file.MoveTo(toServerRelativeUrl, conflictResolution == ConflictResolution.Overwrite ? MoveOperations.Overwrite : MoveOperations.None);
            try {
                _clientContext.ExecuteQuery();
            } catch (Microsoft.SharePoint.Client.ServerException ex) {
                return ex.ServerErrorCode == -2130575257
                    ? Result.Error("The file already exists.")
                    : Result.Error(ex.Message);
            } catch (Exception ex) {
                return Result.Error(ex.Message);
            }

            var verification1 = await FileExistAsync(toPath);
            if (!verification1.HasErrors) {
                return Result.Successful();
            }
            if (verification1.Value == false) {
                return Result.Error("The file was not moved.");
            }
            var verification2 = await FileExistAsync(fromPath);
            if (!verification2.HasErrors) {
                return Result.Successful();
            }
            return verification2.Value == true
                ? Result.Error("The file was moved but still exists in source folder.")
                : Result.Successful();
        }

        public async Task<Result<string>> SaveFileStreamAsync(string path, Stream stream, bool addFolderIfDoesntExist = true, bool ensureUniqueFilename = true) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(path)) {
                return Result.Error("The url is empty");
            }
            if (stream == null) {
                return Result.Error("The stream is null.");
            }
            path = NormalizeRelativeUrl(path);
            // First we need to split the filename from the path
            var directories = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (directories.Last() == String.Empty) {
                return Result.Error("The url provided does not have a filename");
            }
            var folderSiteRelativeUrl = string.Join("/", directories.Take(directories.Length - 1)) + "/";
            var filename = directories.Last();

            if (addFolderIfDoesntExist) {
                var folderExists = await AddFolderAsync(folderSiteRelativeUrl, false, true);
                if (folderExists.HasErrors) {
                    return folderExists;// Result.Error(folderExists.ErrorMessages, folderExists.Exceptions);
                }
            } else {
                var folderExists = await FolderExistAsync(folderSiteRelativeUrl);
                if (folderExists.HasErrors || folderExists.Value == false) {
                    return Result.Error("The folder does not exist.");
                }
            }
            // The folder exists.
            if (ensureUniqueFilename) {
                var filesInFolder = await GetFilesInFolderAsync(folderSiteRelativeUrl);
                if (filesInFolder.HasErrors) {
                    return Result.Error(filesInFolder.Errors);// Result.Error new ResponseResult<string>(filesInFolder.IsSuccessful, null, filesInFolder.ErrorMessages, filesInFolder.Exceptions);
                }
                var foldersInFolder = await GetFoldersInFolderAsync(folderSiteRelativeUrl);
                if (foldersInFolder.HasErrors) {
                    return Result.Error(foldersInFolder.Errors);
                }
                var existingNames = filesInFolder.Value.Union(foldersInFolder.Value);
                filename = CreateUniqueFilename(filename, existingNames);
                path = folderSiteRelativeUrl + filename;
            }

            // Now the filename is unique
            var folderServerRelativeUrl = GetServerRelativeUrl(folderSiteRelativeUrl);
            var folder = _clientContext.Web.GetFolderByServerRelativeUrl(folderServerRelativeUrl);
            folder.Files.Add(new FileCreationInformation() {
                ContentStream = stream,
                Url = filename // weird I know, but url is the filename
            });

            try {
                await _clientContext.ExecuteQueryAsync();
            } catch (Microsoft.SharePoint.Client.ServerException ex) {
                return Result.Error(ex);
            } catch (Exception ex) {
                return Result.Error(ex);
            }

            // Verify
            var verification = await FileExistAsync(path);
            if (verification.HasErrors) {
                return Result.Error(verification.Errors);
            }
            return verification.Value == false
                ? Result.Error("The file was not created.")
                : Result.Successful(path);
        }

        private static string NormalizeRelativeUrl(string relativeUrl) {
            relativeUrl = relativeUrl.Trim().TrimStart(System.IO.Path.DirectorySeparatorChar, System.IO.Path.AltDirectorySeparatorChar);
            return relativeUrl.Replace('\\', '/');
        }

        private string GetServerRelativeUrl(string siteRelativeUrl) {
            if (_clientContext == null) {
                throw new ArgumentNullException("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(siteRelativeUrl)) {
                throw new ArgumentNullException("The url is empty");
            }
            var url = System.IO.Path.Combine(_clientContext.Url, siteRelativeUrl.TrimStart(System.IO.Path.DirectorySeparatorChar, System.IO.Path.AltDirectorySeparatorChar)).Replace(System.IO.Path.DirectorySeparatorChar, '/');
            var uri = new Uri(url);
            return uri.ToString().Replace(uri.GetLeftPart(UriPartial.Authority), string.Empty);
        }

        protected virtual void Dispose(bool disposing) {
            if (!disposedValue) {
                if (disposing) {
                    _clientContext?.Dispose();
                }

                _clientContext = null;
                disposedValue = true;
            }
        }

        public void Dispose() {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public enum ConflictResolution {
            Overwrite,
            MakeFilenameUnique,
            Error
        }

        #region Sync Methods

        public Result AddFolder(string path, bool errorIfAlreadyExists = false, bool recursiveAdd = true) {
            if (_clientContext == null) {
                return Result.Error($"You must authenticate first. Call {nameof(Authenticate)}.");
            }
            if (string.IsNullOrWhiteSpace(path)) {
                return Result.Error("The url is empty");
            }
            path = NormalizeRelativeUrl(path);
            var directories = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (directories.Last() != String.Empty) {
                // We have reached the root.
                return Result.Error("The url must be a folder. Folder urls must end with a /");
            }
            directories = directories.Take(directories.Length - 1).ToArray();
            var folderExists = FolderExist(path);
            if (folderExists.HasErrors) {
                return Result.Error(folderExists.Errors);
            }
            if (folderExists.Value == true) {
                return errorIfAlreadyExists
                    ? Result.Error($"The folder '{path}' already exists.")
                    : Result.Successful();
            }
            // The folder doesnt exist.
            if (directories.Length == 1) {
                // We have reached the root.
                return Result.Error("The root folder does not exist.");
            }
            var parentSiteRelativeUrl = string.Join("/", directories.Take(directories.Length - 1)) + "/";
            var folderName = directories.Last();

            if (recursiveAdd) {
                var addParent = AddFolder(parentSiteRelativeUrl, false, true);
                if (addParent.HasErrors) {
                    return addParent;
                }
            } else {
                var parentFolderExists = FolderExist(parentSiteRelativeUrl);
                if (parentFolderExists.HasErrors) {
                    return Result.Error(parentFolderExists.Errors);
                }
                if (parentFolderExists.Value == false) {
                    return Result.Error("The parent folder does not exist.");
                }
            }

            // The parent folder exists here.
            // The folder needs to be created.
            var serverRelativeUrl = GetServerRelativeUrl(path);
            var folder = _clientContext.Web.Folders.Add(serverRelativeUrl);
            try {
                _clientContext.ExecuteQuery();
            } catch (Microsoft.SharePoint.Client.ServerException ex) {
                if (ex.ServerErrorCode == -2147024894) {
                    // serverRelativeUrl doesn't correspond to a folder
                    return Result.Error("The folder can not be created.");
                }
                return Result.Error(ex.Message);
            } catch (Exception ex) {
                //Console.WriteLine("Could not find folder.");
                return Result.Error(ex.Message);
            }

            var verification = FolderExist(path);
            if (verification.HasErrors) {
                return Result.Error(verification.Errors);
            }
            return verification.Value == false ? Result.Error("The folder was not created.") : Result.Successful();
        }

        public Result CopyFile(string fromPath, string toPath, ConflictResolution conflictResolution = ConflictResolution.Error, bool createFoldersIfDoesntExist = false) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(fromPath)) {
                return Result.Error("The from url is empty");
            }
            fromPath = NormalizeRelativeUrl(fromPath);
            toPath = NormalizeRelativeUrl(toPath);
            var fromDirectories = fromPath.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (fromDirectories.Last() == String.Empty) {
                // We have reached the root.
                return Result.Error("The url must be a file. File urls must not end with a /");
            }
            if (string.IsNullOrWhiteSpace(toPath)) {
                return Result.Error("The to url is empty");
            }
            var fromServerRelativeUrl = GetServerRelativeUrl(fromPath);
            var file = _clientContext.Web.GetFileByServerRelativeUrl(fromServerRelativeUrl);
            _clientContext.Load(file);
            try {
                _clientContext.ExecuteQuery();
            } catch (Exception) {

                throw;
            }

            string toFolderSiteRelativeUrl;
            string toFilename;
            var toDirectories = toPath.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (toDirectories.Last() == String.Empty) {
                // The destination is a folder with no filename
                // First we need to split the filename from the path
                //var fromFolderSiteRelativeUrl = string.Join("/", fromDirectories.Take(fromDirectories.Length - 1));
                toFilename = fromDirectories.Last();
                toFolderSiteRelativeUrl = toPath;
            } else {
                // The destination has a filename
                toFilename = toDirectories.Last();
                toFolderSiteRelativeUrl = string.Join("/", toDirectories.Take(toDirectories.Length - 1)) + "/";
            }

            if (createFoldersIfDoesntExist) {
                var toFolderExists = AddFolder(toFolderSiteRelativeUrl, false, true);
                if (toFolderExists.HasErrors) {
                    return toFolderExists;
                }
            } else {
                var toFolderExists = FolderExist(toFolderSiteRelativeUrl);
                if (toFolderExists.HasErrors) {
                    return Result.Error(toFolderExists.Errors);
                }
                if (toFolderExists.Value == false) {
                    return Result.Error("The destination folder does not exist.");
                }
            }

            if (conflictResolution == ConflictResolution.MakeFilenameUnique) {
                // Get the destination filename
                var filesInFolder = GetFilesInFolder(toFolderSiteRelativeUrl);
                if (filesInFolder.HasErrors) {
                    return Result.Error(filesInFolder.Errors);
                }
                var foldersInFolder = GetFoldersInFolder(toFolderSiteRelativeUrl);
                if (foldersInFolder.HasErrors) {
                    return Result.Error(foldersInFolder.Errors);
                }
                var existingNames = filesInFolder.Value.Union(foldersInFolder.Value);
                toFilename = CreateUniqueFilename(toFilename, existingNames);

            }

            toPath = $"{toFolderSiteRelativeUrl}{toFilename}";

            var toServerRelativeUrl = GetServerRelativeUrl(toPath);
            file.CopyTo(toServerRelativeUrl, conflictResolution == ConflictResolution.Overwrite);
            try {
                _clientContext.ExecuteQuery();
            } catch (Microsoft.SharePoint.Client.ServerException ex) {
                return ex.ServerErrorCode == -2130575257
                    ? Result.Error("The file already exists.")
                    : Result.Error(ex.Message);
            } catch (Exception ex) {
                return Result.Error(ex.Message);
            }

            var verification1 = FileExist(toPath);
            if (!verification1.HasErrors) {
                return Result.Successful();
            }
            if (verification1.Value == false) {
                return Result.Error("The file was not moved.");
            }

            return Result.Successful();
        }

        public Result DeleteFile(string path, bool errorIfFileDoesntExist = false) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(path)) {
                return Result.Error("The url is empty");
            }
            path = NormalizeRelativeUrl(path);
            var directories = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (directories.Last() == String.Empty) {
                return Result.Error("The url provided does not have a filename. Files do not end with /");
            }
            var serverRelativeUrl = GetServerRelativeUrl(path);

            if (errorIfFileDoesntExist) {
                var fileExist = FileExist(path);
                if (fileExist.HasErrors) {
                    return Result.Error(fileExist.Errors);
                }
                if (fileExist.Value == false) {
                    return Result.Error("The file does not exist.");
                }
            }

            var file = _clientContext.Web.GetFileByServerRelativeUrl(serverRelativeUrl);
            file.DeleteObject();
            try {
                _clientContext.ExecuteQuery();
            } catch (Exception) {

                throw;
            }
            var verification = FileExist(path);
            if (verification.HasErrors) {
                return Result.Error(verification.Errors);
            }
            return verification.Value == true ? Result.Error("The file was not deleted.") : Result.Successful();
        }

        public Result DeleteFolder(string path, bool errorIfFolderDoesntExist = false, bool errorIfFolderIsNotEmpty = true) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(path)) {
                return Result.Error("The url is empty");
            }
            path = NormalizeRelativeUrl(path);
            var directories = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (directories.Last() != String.Empty) {
                // We have reached the root.
                return Result.Error("The url must be a folder. Folder urls must end with a /");
            }
            //directories = directories.Take(directories.Length - 1).ToArray();
            var serverRelativeUrl = GetServerRelativeUrl(path);

            var folderExist = FolderExist(path);
            if (folderExist.HasErrors) {
                return Result.Error(folderExist.Errors);
            }
            if (errorIfFolderDoesntExist) {
                if (folderExist.Value == false) {
                    return Result.Error("The folder does not exist.");
                }
            } else {
                if (folderExist.Value == false) {
                    return Result.Successful();
                }
            }

            if (errorIfFolderIsNotEmpty) {
                var filesInFolder = GetFilesInFolder(path);
                if (filesInFolder.HasErrors) {
                    return Result.Error(filesInFolder.Errors);
                }
                if (filesInFolder.Value.Count > 0) {
                    return Result.Error($"There are {filesInFolder.Value.Count} files in this folder.");
                }
                var foldersInFolder = GetFoldersInFolder(path);
                if (foldersInFolder.HasErrors) {
                    return Result.Error(foldersInFolder.Errors);
                }
                if (foldersInFolder.Value.Count > 0) {
                    return Result.Error($"There are {foldersInFolder.Value.Count} folders in this folder.");
                }
            }

            var folder = _clientContext.Web.GetFolderByServerRelativeUrl(serverRelativeUrl);
            folder.DeleteObject();
            try {
                _clientContext.ExecuteQuery();
            } catch (Exception ex) {
                return Result.Error(ex.Message);
            }
            var verification = FolderExist(path);
            if (verification.HasErrors) {
                return Result.Error(verification.Errors);
            }
            return verification.Value == true ? Result.Error("The folder was not deleted.") : Result.Successful();
        }

        public Result<bool> FileExist(string path) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(path)) {
                return Result.Error("The url is empty");
            }
            path = NormalizeRelativeUrl(path);
            var directories = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (directories.Last() == String.Empty) {
                // We have reached the root.
                return Result.Error("The url must be a file. File urls must not end with a /");
            }
            var serverRelativeUrl = GetServerRelativeUrl(path);
            var file = _clientContext.Web.GetFileByServerRelativeUrl(serverRelativeUrl);
            _clientContext.Load(file, f => f.Exists);
            try {
                _clientContext.ExecuteQuery();

                return file.Exists ? Result.Successful(true) : Result.Successful(false);
            } catch (ServerUnauthorizedAccessException) {
                return Result.Error("You are not allowed to access this folder");
            } catch (Exception ex) {
                return Result.Error(ex.Message);
            }
        }

        public Result<bool> FolderExist(string path) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(path)) {
                return Result.Error("The url is empty");
            }
            path = NormalizeRelativeUrl(path);
            var directories = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (directories.Last() != String.Empty) {
                // We have reached the root.
                return Result.Error("The url must be a folder. Folder urls must end with a /");
            }
            var serverRelativeUrl = GetServerRelativeUrl(path);
            var folder = _clientContext.Web.GetFolderByServerRelativeUrl(serverRelativeUrl);
            _clientContext.Load(folder, f => f.Exists);
            try {
                _clientContext.ExecuteQuery();

                return folder.Exists ? Result.Successful(true) : Result.Successful(false);
            } catch (ServerUnauthorizedAccessException) {
                return Result.Error("You are not allowed to access this folder");
            } catch (Microsoft.SharePoint.Client.ServerException ex) {
                if (ex.ServerErrorCode == -2147024894) {
                    // serverRelativeUrl doesn't correspond to a folder
                    return Result.Successful(false);
                }
                return Result.Error(ex);
            } catch (Exception ex) {
                return Result.Error(ex);
            }
        }

        public Result<List<string>> GetFilesInFolder(string path) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(path)) {
                return Result.Error("The url is empty");
            }
            path = NormalizeRelativeUrl(path);
            var directories = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (directories.Last() != String.Empty) {
                // We have reached the root.
                return Result.Error("The url must be a folder. Folder urls must end with a /");
            }
            //directories = directories.Take(directories.Length - 1).ToArray();
            var folderServerRelativeUrl = GetServerRelativeUrl(path);
            FileCollection files = _clientContext.Web.GetFolderByServerRelativeUrl(folderServerRelativeUrl).Files;
            _clientContext.Load(files);
            try {
                _clientContext.ExecuteQuery();
            } catch (Exception ex) {
                return Result.Error(ex);
            }

            return Result.Successful(files.Select(e => e.Name).ToList());
        }

        public Result<Stream> GetFileStream(string path) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(path)) {
                return Result.Error("The url is empty");
            }
            path = NormalizeRelativeUrl(path);
            var directories = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (directories.Last() == String.Empty) {
                // We have reached the root.
                return Result.Error("The url must be a file. File urls must not end with a /");
            }
            var fileServerRelativeUrl = GetServerRelativeUrl(path);
            var file = _clientContext.Web.GetFileByServerRelativeUrl(fileServerRelativeUrl);
            var clientResult = file.OpenBinaryStream(); // has an options version
            _clientContext.Load(file);
            try {
                _clientContext.ExecuteQuery();
            } catch (Exception ex) {
                return Result.Error(ex);
            }

            return Result.Successful(clientResult.Value);
        }

        public Result<List<string>> GetFoldersInFolder(string path) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(path)) {
                return Result.Error("The url is empty");
            }
            path = NormalizeRelativeUrl(path);
            var directories = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (directories.Last() != String.Empty) {
                // We have reached the root.
                return Result.Error("The url must be a folder. Folder urls must end with a /");
            }
            //directories = directories.Take(directories.Length - 1).ToArray();
            var folderServerRelativeUrl = GetServerRelativeUrl(path);
            FolderCollection folders = _clientContext.Web.GetFolderByServerRelativeUrl(folderServerRelativeUrl).Folders;
            _clientContext.Load(folders);
            try {
                _clientContext.ExecuteQuery();
            } catch (Exception) {

                throw;
            }

            return Result.Successful(folders.Select(e => e.Name).ToList());
        }

        public Result MoveFile(string fromPath, string toPath, ConflictResolution conflictResolution = ConflictResolution.Error, bool createFoldersIfDoesntExist = false) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(fromPath)) {
                return Result.Error("The from url is empty");
            }
            fromPath = NormalizeRelativeUrl(fromPath);
            toPath = NormalizeRelativeUrl(toPath);
            var fromDirectories = fromPath.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (fromDirectories.Last() == String.Empty) {
                // We have reached the root.
                //Storage.AddSystemLogEntry(Storage.LogCategory.Debug, "ScanProcessing.cs:1500", "The url must be a file. File urls must not end with a /");
                return Result.Error("The url must be a file. File urls must not end with a /");
            }
            if (string.IsNullOrWhiteSpace(toPath)) {
                //Storage.AddSystemLogEntry(Storage.LogCategory.Debug, "ScanProcessing.cs:1504", "The to url is empty");
                return Result.Error("The to url is empty");
            }
            var fromServerRelativeUrl = GetServerRelativeUrl(fromPath);
            //Storage.AddSystemLogEntry(Storage.LogCategory.Debug, "ScanProcessing.cs:1508", "Init GetFileByServerRelativeUrl");
            var file = _clientContext.Web.GetFileByServerRelativeUrl(fromServerRelativeUrl);
            //Storage.AddSystemLogEntry(Storage.LogCategory.Debug, "ScanProcessing.cs:1510", $"Init Load");
            _clientContext.Load(file);
            try {
                //Storage.AddSystemLogEntry(Storage.LogCategory.Debug, "ScanProcessing.cs:219", $"Init ExecuteQuery");
                _clientContext.ExecuteQuery();
            } catch (Exception ex) {
                return Result.Error(ex);
            }

            string toFolderSiteRelativeUrl;
            string toFilename;
            var toDirectories = toPath.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (toDirectories.Last() == String.Empty) {
                // The destination is a folder with no filename
                // First we need to split the filename from the path
                //var fromFolderSiteRelativeUrl = string.Join("/", fromDirectories.Take(fromDirectories.Length - 1));
                toFilename = fromDirectories.Last();
                toFolderSiteRelativeUrl = toPath;
            } else {
                // The destination has a filename
                toFilename = toDirectories.Last();
                toFolderSiteRelativeUrl = string.Join("/", toDirectories.Take(toDirectories.Length - 1)) + "/";
            }

            if (createFoldersIfDoesntExist) {
                var toFolderExists = AddFolder(toFolderSiteRelativeUrl, false, true);
                if (toFolderExists.HasErrors) {
                    return toFolderExists;
                }
            } else {
                var toFolderExists = FolderExist(toFolderSiteRelativeUrl);
                if (toFolderExists.HasErrors) {
                    return Result.Error(toFolderExists.Errors);
                }
                if (toFolderExists.Value == false) {
                    return Result.Error("The destination folder does not exist.");
                }
            }

            if (conflictResolution == ConflictResolution.MakeFilenameUnique) {
                // Get the destination filename

                var filesInFolder = GetFilesInFolder(toFolderSiteRelativeUrl);
                if (filesInFolder.HasErrors) {
                    return Result.Error(filesInFolder.Errors);
                }
                var foldersInFolder = GetFoldersInFolder(toFolderSiteRelativeUrl);
                if (foldersInFolder.HasErrors) {
                    return Result.Error(foldersInFolder.Errors);
                }
                var existingNames = filesInFolder.Value.Union(foldersInFolder.Value);
                toFilename = CreateUniqueFilename(toFilename, existingNames);

            }

            toPath = $"{toFolderSiteRelativeUrl}{toFilename}";

            var toServerRelativeUrl = GetServerRelativeUrl(toPath);
            file.MoveTo(toServerRelativeUrl, conflictResolution == ConflictResolution.Overwrite ? MoveOperations.Overwrite : MoveOperations.None);
            try {
                _clientContext.ExecuteQuery();
            } catch (Microsoft.SharePoint.Client.ServerException ex) {
                return ex.ServerErrorCode == -2130575257
                    ? Result.Error("The file already exists.")
                    : Result.Error(ex.Message);
            } catch (Exception ex) {
                return Result.Error(ex.Message);
            }

            var verification1 = FileExist(toPath);
            if (!verification1.HasErrors) {
                return Result.Successful();
            }
            if (verification1.Value == false) {
                return Result.Error("The file was not moved.");
            }
            var verification2 = FileExist(fromPath);
            if (!verification2.HasErrors) {
                return Result.Successful();
            }
            return verification2.Value == true
                ? Result.Error("The file was moved but still exists in source folder.")
                : Result.Successful();
        }

        public Result<string> SaveFileStream(string path, Stream stream, bool addFolderIfDoesntExist = true, bool ensureUniqueFilename = true) {
            if (_clientContext == null) {
                return Result.Error("The SharePoint client context can not be null.");
            }
            if (string.IsNullOrWhiteSpace(path)) {
                return Result.Error("The url is empty");
            }
            if (stream == null) {
                return Result.Error("The stream is null.");
            }
            path = NormalizeRelativeUrl(path);
            // First we need to split the filename from the path
            var directories = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (directories.Last() == String.Empty) {
                return Result.Error("The url provided does not have a filename");
            }
            var folderSiteRelativeUrl = string.Join("/", directories.Take(directories.Length - 1)) + "/";
            var filename = directories.Last();

            if (addFolderIfDoesntExist) {
                var folderExists = AddFolder(folderSiteRelativeUrl, false, true);
                if (folderExists.HasErrors) {
                    return folderExists;// Result.Error(folderExists.ErrorMessages, folderExists.Exceptions);
                }
            } else {
                var folderExists = FolderExist(folderSiteRelativeUrl);
                if (folderExists.HasErrors || folderExists.Value == false) {
                    return Result.Error("The folder does not exist.");
                }
            }
            // The folder exists.
            if (ensureUniqueFilename) {
                var filesInFolder = GetFilesInFolder(folderSiteRelativeUrl);
                if (filesInFolder.HasErrors) {
                    return Result.Error(filesInFolder.Errors);// Result.Error new ResponseResult<string>(filesInFolder.IsSuccessful, null, filesInFolder.ErrorMessages, filesInFolder.Exceptions);
                }
                var foldersInFolder = GetFoldersInFolder(folderSiteRelativeUrl);
                if (foldersInFolder.HasErrors) {
                    return Result.Error(foldersInFolder.Errors);
                }
                var existingNames = filesInFolder.Value.Union(foldersInFolder.Value);
                filename = CreateUniqueFilename(filename, existingNames);
                path = folderSiteRelativeUrl + filename;
            }

            // Now the filename is unique
            var folderServerRelativeUrl = GetServerRelativeUrl(folderSiteRelativeUrl);
            var folder = _clientContext.Web.GetFolderByServerRelativeUrl(folderServerRelativeUrl);
            folder.Files.Add(new FileCreationInformation() {
                ContentStream = stream,
                Url = filename // weird I know, but url is the filename
            });

            try {
                _clientContext.ExecuteQuery();
            } catch (Microsoft.SharePoint.Client.ServerException ex) {
                return Result.Error(ex);
            } catch (Exception ex) {
                return Result.Error(ex);
            }

            // Verify
            var verification = FileExist(path);
            if (verification.HasErrors) {
                return Result.Error(verification.Errors);
            }
            return verification.Value == false
                ? Result.Error("The file was not created.")
                : Result.Successful(path);
        }

        #endregion
    }
}
using InterfaceFromADM.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using InterfaceFromADM.Helper.DBConfig;

namespace InterfaceFromADM.Helper.Util
{
    public class WebAPIUtil
    {
        public WebAPIUtil()
        {
            //Set default protocol to TLS 1.2
            ServicePointManager.SecurityProtocol = (SecurityProtocolType)3072;
        }


        public class TokenData
        {
            public string access_token { get; set; }

            public string token_type { get; set; }

            public string expires_in { get; set; }
        }

        public static T GetToken<T>(string url) where T : new()
        {
            using (var w = new WebClient())
            {
                var json_data = string.Empty;
                string json = string.Empty;

                try
                {
                    json_data = w.DownloadString(url);

                    JObject obj = JObject.Parse(json_data);

                    json = JsonConvert.SerializeObject(obj.SelectToken("token"));

                }
                catch (Exception) { }

                return !string.IsNullOrEmpty(json_data) ? JsonConvert.DeserializeObject<T>(json) : new T();
            }
        }

        public static List<T> Request<T>(Dictionary<string, string> data, string uri, string jsonConvert, string method) where T : new()
        {
            //get token
            //var postReq = "userName=" + CSTDSystemConfig.GetValue("ippcs_uname_ws");
            //postReq += "&password=" + CSTDSystemConfig.GetValue("ippcs_pwd_ws");
            //string getToken = CSTDSystemConfig.GetValue("ippcs_uri_token_ws") + postReq;

            //var TokenResponse = _download_serialized_json_data_token<TokenData>(getToken);

            var settings = new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                MissingMemberHandling = MissingMemberHandling.Ignore
            };

            string content = string.Empty;
            dynamic ajax_array = new Object();

            //handle error
            string Errresponse = string.Empty;
            CommonDBHelper Repo = CommonDBHelper.Instance;
            string url = Repo.getAPIUrl();

            Console.WriteLine("API URL :" + url);

            ServicePointManager.SecurityProtocol = (SecurityProtocolType)3072;
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);

            //set headers
            request.ContentType = "application/json";
            request.UserAgent = "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)";
            request.Method = method;

            //set token
            //request.Headers.Add(HttpRequestHeader.Authorization, "Bearer " + TokenResponse.access_token);

            request.Proxy = WebRequest.DefaultWebProxy;
            request.Credentials = System.Net.CredentialCache.DefaultCredentials; ;
            request.Proxy.Credentials = System.Net.CredentialCache.DefaultCredentials;

            try
            {
                if (data != null && data.Count > 0)
                {
                    //write data to be sent
                    string dataJson = JsonConvert.SerializeObject(data);
                    using (var streamWriter = new StreamWriter(request.GetRequestStream()))
                    {
                        streamWriter.Write(dataJson);
                    }
                }

                if (jsonConvert != null)
                {
                    //write data to be sent
                    using (var streamWriter = new StreamWriter(request.GetRequestStream()))
                    {
                        streamWriter.Write(jsonConvert);
                    }
                }

                //retrieve response
                var response = request.GetResponse() as HttpWebResponse;

                using (StreamReader reader = new StreamReader(response.GetResponseStream()))
                {
                    content = reader.ReadToEnd();
                }
                response.Close();

                ajax_array = JsonConvert.DeserializeObject<List<T>>(content, settings);
                //ajax_array.Message = null;

                return !string.IsNullOrEmpty(content) ? ajax_array : new List<T>();

            }
            catch (WebException ex)
            {
                using (var reader = new System.IO.StreamReader(ex.Response.GetResponseStream()))
                {
                    Errresponse = reader.ReadToEnd();

                }
                //throw ex;
                ajax_array = new List<T>();
                //ajax_array.Message = JsonConvert.DeserializeObject(Errresponse).ToString();

                return !string.IsNullOrEmpty(Errresponse) ? ajax_array : new List<T>();
            }
        }

        public static T _download_serialized_json_data_token<T>(string url) where T : new()
        {
            using (var w = new WebClient())
            {
                var json_data = string.Empty;
                string json = string.Empty;

                try
                {
                    json_data = w.DownloadString(url);
                    JObject obj = JObject.Parse(json_data);

                    json = JsonConvert.SerializeObject(obj.SelectToken("token"));

                }
                catch (Exception) { }

                return !string.IsNullOrEmpty(json_data) ? JsonConvert.DeserializeObject<T>(json) : new T();
            }
        }

        public static response RequestFile<T>(Dictionary<string, object> parameters, string uri, string jsonConvert, string method) where T : new()
        {
            string content = string.Empty;
            dynamic ajax_array = new Object();
            response objectRespond = new response();
            string Errresponse = string.Empty;
            // string url = ConfigurationManager.AppSettings["TPInterface"];

            CommonDBHelper Repo = CommonDBHelper.Instance;
            string url = Repo.getAPIUrl();
     
            Console.WriteLine("API URL :" + url);
            string boundary = "---------------------------" + DateTime.Now.Ticks.ToString("x");
            byte[] boundaryBytes = System.Text.Encoding.ASCII.GetBytes("\r\n--" + boundary + "\r\n");

            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
            request.ContentType = "multipart/form-data; boundary=" + boundary;
            request.UserAgent = "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)";
            request.Method = method;
            request.KeepAlive = true;
            request.Proxy = WebRequest.DefaultWebProxy;
            request.Credentials = System.Net.CredentialCache.DefaultCredentials;
            request.Proxy.Credentials = System.Net.CredentialCache.DefaultCredentials;

            try
            {
                Console.WriteLine("API Process Start");
                var settings = new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore,
                    MissingMemberHandling = MissingMemberHandling.Ignore
                };

                if (parameters != null && parameters.Count > 0)
                {

                    using (Stream requestStream = request.GetRequestStream())
                    {

                        foreach (KeyValuePair<string, object> pair in parameters)
                        {

                            requestStream.Write(boundaryBytes, 0, boundaryBytes.Length);
                            if (pair.Value is FormFile)
                            {
                                FormFile file = pair.Value as FormFile;
                                string header = "Content-Disposition: form-data; name=\"" + pair.Key + "\"; filename=\"" + file.Name + "\"\r\nContent-Type: " + file.ContentType + "\r\n\r\n";
                                byte[] bytes = System.Text.Encoding.UTF8.GetBytes(header);
                                requestStream.Write(bytes, 0, bytes.Length);
                                byte[] buffer = new byte[32768];
                                int bytesRead;
                                if (file.Stream == null)
                                {
                                    // upload from file
                                    using (FileStream fileStream = File.OpenRead(file.FilePath))
                                    {
                                        while ((bytesRead = fileStream.Read(buffer, 0, buffer.Length)) != 0)
                                            requestStream.Write(buffer, 0, bytesRead);
                                        fileStream.Close();
                                    }
                                }
                                else
                                {
                                    // upload from given stream
                                    while ((bytesRead = file.Stream.Read(buffer, 0, buffer.Length)) != 0)
                                        requestStream.Write(buffer, 0, bytesRead);
                                }
                            }
                            else
                            {
                                string data = "Content-Disposition: form-data; name=\"" + pair.Key + "\"\r\n\r\n" + pair.Value;
                                byte[] bytes = System.Text.Encoding.UTF8.GetBytes(data);
                                requestStream.Write(bytes, 0, bytes.Length);
                            }
                        }

                        byte[] trailer = System.Text.Encoding.ASCII.GetBytes("\r\n--" + boundary + "--\r\n");
                        requestStream.Write(trailer, 0, trailer.Length);
                        requestStream.Close();
                    }
                }

                using (WebResponse response = request.GetResponse())
                {
                    using (Stream responseStream = response.GetResponseStream())
                    using (StreamReader reader = new StreamReader(responseStream))
                        content = reader.ReadToEnd();
                }

                objectRespond = JsonConvert.DeserializeObject<response>(content);


                return objectRespond;

            }
            catch (WebException ex)
            {
                Console.WriteLine("API Process error : " + ex.Message);
                using (var reader = new System.IO.StreamReader(ex.Response.GetResponseStream()))
                {
                    Errresponse = reader.ReadToEnd();

                }
                //throw ex;
                objectRespond = JsonConvert.DeserializeObject<response>(Errresponse);
                //ajax_array.Message = JsonConvert.DeserializeObject(Errresponse).ToString();

                return objectRespond;
            }
        }

    }
}

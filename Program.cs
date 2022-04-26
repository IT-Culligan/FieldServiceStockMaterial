using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Net;
using System.Net.Mail;
using System.Text;
using Devart.Data.Salesforce;

namespace FieldServiceStockMaterial
{
    internal class Program
    {
        private static readonly DateTime Now = DateTime.Now;
        public static readonly String DirLog = AppDomain.CurrentDomain.BaseDirectory + @"Log\" + Now.Year + @"\" + Now.Month.ToString("00") + @"\";
        public static readonly String FileLog = Now.ToString("yyyyMMddHHmm") + ".log";

        static void Main()
        {
            String esitoPreparaLog = PreparaDirLog();
            if (!String.IsNullOrEmpty(esitoPreparaLog))
            {
                EmailErrore(esitoPreparaLog, "Preparazione log");
            }

            ScriviLog("----- Inizio -----", String.Empty);

            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            ServicePointManager.SecurityProtocol &= ~SecurityProtocolType.Ssl3;

            using (var connSql = new SqlConnection(GetConnectionString("Sql")))
            using (var connSF = new SalesforceConnection(GetConnectionString("SF")))
            {
                try
                {
                    connSql.Open();
                    connSF.Open();

                    try
                    {
                        String ssql = "SELECT Field_Service_Elaboration_Result__c, Location__c, Product_Code__c, Quantity__c ";
                        ssql += "FROM [BI_STG].[dbo].[vw_FieldServiceStockMaterial_SFDC_DIRECT]";

                        using (var comando = new SqlCommand(ssql, connSql))
                        using (var dataAdapter = new SqlDataAdapter(comando))
                        using (var ds = new DataSet())
                        {
                            dataAdapter.FillSchema(ds, SchemaType.Source);
                            dataAdapter.Fill(ds, "TabDati");

                            using (SalesforceLoader sfLoader = new SalesforceLoader("SAP_warehouse", connSF))
                            {
                                try
                                {
                                    sfLoader.Columns.Add("Field_Service_Elaboration_Result", SalesforceType.String);
                                    sfLoader.Columns.Add("Location", SalesforceType.String);
                                    sfLoader.Columns.Add("Product_Code", SalesforceType.String);
                                    sfLoader.Columns.Add("Quantity", SalesforceType.Double);

                                    sfLoader.Mode = SalesforceLoaderMode.Insert;
                                    sfLoader.BatchSize = Convert.ToInt32(ConfigurationManager.AppSettings["BatchSize"]);

                                    sfLoader.Open();
                                    Boolean open = true;
                                    var contaRecord = 0;

                                    foreach (DataRow r in ds.Tables["TabDati"].Rows)
                                    {
                                        sfLoader.SetValue("Field_Service_Elaboration_Result", r.Field<String>("Field_Service_Elaboration_Result__c"));
                                        sfLoader.SetValue("Location", r.Field<String>("Location__c"));
                                        sfLoader.SetValue("Product_Code", r.Field<String>("Product_Code__c"));
                                        sfLoader.SetValue("Quantity", r.Field<Decimal>("Quantity__c"));

                                        contaRecord++;
                                        sfLoader.NextRow();
                                    }

                                    if (open)
                                    {
                                        sfLoader.Close(contaRecord > 0);
                                    }
                                }
                                catch (SalesforceLoaderException ex)
                                {
                                    Int32 recordsFailed = ex.RecordsFailed;

                                    if (recordsFailed > 0)
                                    {
                                        var results = sfLoader.GetResults();
                                        var sb = new StringBuilder();

                                        foreach (var r in results)
                                        {
                                            if (!r.Success)
                                            {
                                                sb.AppendLine(r.Id + " - " + r.ErrorText);
                                            }
                                        }

                                        var log = sb.ToString();
                                        esitoPreparaLog = PreparaDirLog();

                                        if (esitoPreparaLog != String.Empty)
                                        {
                                            EmailErrore(esitoPreparaLog + "<br />" + log, "Errore log batch Field Service Stock Material");
                                            return;
                                        }

                                        ScriviLog("Errori insert dati", log);
                                        EmailErrore(log, "Errori insert Field Service Stock Material");
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        EmailErrore(ex.Message, "Generale");
                    }
                    finally
                    {
                        ChiudiConnessione(connSql);
                        ChiudiConnessione(connSF);
                        ScriviLog("----- Fine -----", String.Empty);
                    }
                }
                catch (Exception ex)
                {
                    EmailErrore(ex.Message, "Generale");
                }
            }
        }

        private static void EmailErrore(String errore, String origine)
        {
            //return;
            try
            {
                using (var smtp = new SmtpClient("CADRHR-01.culligan.it"))
                using (var message = new MailMessage())
                {
                    message.From = new MailAddress("no-reply@culligan.it", "Batch Field Service Stock Material");
                    var arrDestinatari = ConfigurationManager.AppSettings["DestinatariErrori"]
                        .Replace(" ", "")
                        .Split(new String[] { ";" }, StringSplitOptions.RemoveEmptyEntries);
                    foreach (var d in arrDestinatari)
                    {
                        message.To.Add(d);
                    }
                    message.Subject = "Errore Field Service Stock Material - " + origine;
                    message.Body = errore;
                    message.IsBodyHtml = true;

                    smtp.Send(message);
                }
            }
            catch
            {
                // Nulla
            }
        }

        private static String PreparaDirLog()
        {
            try
            {
                if (!Directory.Exists(DirLog))
                {
                    Directory.CreateDirectory(DirLog);
                }

                return String.Empty;
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
        }

        private static void ScriviLog(String origine, String cosa)
        {
            try
            {
                String log = DirLog + @"\" + FileLog;
                String textToWrite = DateTime.Now.ToString("HH:mm:ss") + " - " + origine + Environment.NewLine + cosa + Environment.NewLine;

                File.AppendAllText(log, textToWrite.Replace("<br />", ""));
            }
            catch (Exception ex)
            {
                EmailErrore(ex.Message, "Scrittura log - " + origine);
            }
        }

        private static String GetConnectionString(String quale)
        {

            if (quale == "SF")
            {
                String currDir = AppDomain.CurrentDomain.BaseDirectory;
                String dataCache = currDir + "Cache.db";
                String metadataCache = currDir + "Metadata.db";

                if (ConfigurationManager.AppSettings["IsSandbox"] == "1")
                {
                    return ConfigurationManager.ConnectionStrings["SalesforceTestConnectionString"].ConnectionString + ";Data Cache=" + dataCache + ";Metadata Cache=" + metadataCache;
                }
                else
                {
                    return ConfigurationManager.ConnectionStrings["SalesforceProdConnectionString"].ConnectionString + ";Data Cache=" + dataCache + ";Metadata Cache=" + metadataCache;
                }
            }
            else
            {
                return ConfigurationManager.ConnectionStrings["SqlConnectionString"].ConnectionString;
            }
        }

        private static void ChiudiConnessione(SqlConnection c)
        {
            if (c.State.HasFlag(ConnectionState.Open)) { c.Close(); }
        }

        private static void ChiudiConnessione(SalesforceConnection c)
        {
            if (c.State.HasFlag(ConnectionState.Open)) { c.Close(); }
        }
    }
}

/************************************************************************************************
 * Program History : 
 * 
 * Project Name     : IPPCS (Procurement Control System)
 * Client Name      : PT. TMMIN (Toyota Manufacturing Motor Indonesia)
 * Function Id      : 
 * Function Name    : 
 * Function Group   : 
 * Program Id       : 
 * Program Name     : 
 * Program Type     : Console Application
 * Description      : This Console is used for Common Batch IPPCS.
 * Environment      : .NET 4.0, ASP MVC 4.0
 * Author           : wot.Dena
 * Version          : 01.00.00
 * Creation Date    : 20/01/2021
 *                                                                                                          *
 * Update history		Re-fix date				Person in charge				Description					*
 *
 * Copyright(C) 2020 - . All Rights Reserved                                                                                              
 *************************************************************************************************/

using InterfaceFromADM.Helper.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
namespace InterfaceFromADM
{
    class Program
    {
        
        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine("Interface File From ADM to IPPCS is started");
                //string functionId = args[0];//"TransferPostingtoICS";
                string functionId = "InterfaceFromADM";
                Assembly assembly = typeof(Program).Assembly; // in the same assembly!

                Type type = assembly.GetType("InterfaceFromADM.AppCode." + functionId);
                BaseBatch batch = (BaseBatch)Activator.CreateInstance(type);
                batch.ExecuteBatch();
                Console.WriteLine("Interface File From ADM to IPPCS is ended");
                //TransferPostingIPPCStoICS.AppCode.TPIPPCStoICS baru = new AppCode.TPIPPCStoICS();
                //baru.ExecuteBatch();

                //TPIPPCStoICS1 TP = new TPIPPCStoICS1();
                //TP.ExecuteBatchTP();
                Thread.Sleep(1000);
            }
            catch (Exception AE)
            {
                Console.WriteLine(AE.Message);
                Console.WriteLine("Interface File From ADM to IPPCS is ended");
                Thread.Sleep(3000);
            }
        }
    }
}

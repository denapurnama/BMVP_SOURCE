using NPOI.HSSF.UserModel;
using NPOI.SS.UserModel;
using NPOI.SS.Util;
using NPOI.XSSF.UserModel;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Toyota.Common.Web.Platform;

namespace InterfaceFromADM.Helper.Report
{
    public static class NPOIHelper
    {
        public static void CopyImage(HSSFSheet sheet, HSSFPictureData picData, int row, int col)
        {

            var pictureIdx = sheet.Workbook.AddPicture(picData.Data, (NPOI.SS.UserModel.PictureType)picData.Format);
            var drawing = sheet.CreateDrawingPatriarch();
            ICreationHelper helper = sheet.Workbook.GetCreationHelper();
            var anchor = helper.CreateClientAnchor();
            anchor.Col1 = col;
            anchor.Row1 = row;
            var pict = drawing.CreatePicture(anchor, pictureIdx);
            pict.Resize();
        }
        public static void AddCellValue(HSSFSheet sheet, int row, int cell, object Value)
        {
            if (sheet.GetRow(row) == null)
            {
                sheet.CreateRow(row);
            }
            if (sheet.GetRow(row).GetCell(cell) == null)
            {
                sheet.GetRow(row).CreateCell(cell);
            }
            sheet.GetRow(row).GetCell(cell).SetCellValue(sheet.GetRow(row).GetCell(cell).ToString() + (Value == null ? "" : Value.ToString()));
        }
        public static void SetCellValue(HSSFSheet sheet, int row, int cell, object Value)
        {
            if (sheet.GetRow(row) == null)
            {
                sheet.CreateRow(row);
            }
            if (sheet.GetRow(row).GetCell(cell) == null)
            {
                sheet.GetRow(row).CreateCell(cell);
            }
            sheet.GetRow(row).GetCell(cell).SetCellValue(Value == null ? "" : Value.ToString());
        }
        public static void SetCellFormulla(HSSFSheet sheet, int row, int cell, object Value)
        {
            if (sheet.GetRow(row) == null)
            {
                sheet.CreateRow(row);
            }
            if (sheet.GetRow(row).GetCell(cell) == null)
            {
                sheet.GetRow(row).CreateCell(cell);
            }
            sheet.GetRow(row).GetCell(cell).SetCellFormula(Value.ToString());
        }
        public static void AddCellValueX(XSSFSheet sheet, int row, int cell, object Value)
        {
            if (sheet.GetRow(row) == null)
            {
                sheet.CreateRow(row);
            }
            if (sheet.GetRow(row).GetCell(cell) == null)
            {
                sheet.GetRow(row).CreateCell(cell);
            }
            sheet.GetRow(row).GetCell(cell).SetCellValue(sheet.GetRow(row).GetCell(cell).ToString() + (Value == null ? "" : Value.ToString()));
        }
        public static void SetCellValueX(XSSFSheet sheet, int row, int cell, object Value)
        {
            if (sheet.GetRow(row) == null)
            {
                sheet.CreateRow(row);
            }
            if (sheet.GetRow(row).GetCell(cell) == null)
            {
                sheet.GetRow(row).CreateCell(cell);
            }
            sheet.GetRow(row).GetCell(cell).SetCellValue(Value == null ? "" : Value.ToString());
        }
        public static void SetCellValueNumeric(HSSFSheet sheet, int row, int cell, object Value)
        {
            if (sheet.GetRow(row) == null)
            {
                sheet.CreateRow(row);
            }
            if (sheet.GetRow(row).GetCell(cell) == null)
            {
                sheet.GetRow(row).CreateCell(cell);
            }
            Double a = 0;
            sheet.GetRow(row).GetCell(cell).SetCellValue(Value == null ? a : Convert.ToDouble(Value));
        }
        public static void SetCellValueNumericNew(HSSFSheet sheet, int row, int cell, object Value)
        {
            if (sheet.GetRow(row) == null)
            {
                sheet.CreateRow(row);
            }
            if (sheet.GetRow(row).GetCell(cell) == null)
            {
                sheet.GetRow(row).CreateCell(cell);
            }
            Double a = 0;

            if (Value == null)
            {
                sheet.GetRow(row).GetCell(cell).SetCellValue("");
            }
            else
            {
                sheet.GetRow(row).GetCell(cell).SetCellValue(Convert.ToDouble(Value));
            }
        }

        public static void SetCellValueGlobal(HSSFSheet sheet, int row, int cell, object Value)
        {
            double n;
            bool isNumeric = double.TryParse(Convert.ToString(String.Format("{0:0.00}", Value)), out n);

            if (sheet.GetRow(row) == null)
            {
                sheet.CreateRow(row);
            }
            if (sheet.GetRow(row).GetCell(cell) == null)
            {
                sheet.GetRow(row).CreateCell(cell);
            }

            if (isNumeric)
            {
                Double a = 0;
                sheet.GetRow(row).GetCell(cell).SetCellValue(Value == null ? a : Convert.ToDouble(Value));
            }
            else
            {
                sheet.GetRow(row).GetCell(cell).SetCellValue(Value == null ? "" : Value.ToString());
            }

        }

        public static string CellName(int row, int column)
        {
            string cellName = "A1";
            try
            {
                string letter = "";
                string number = "";

                if (column <= 25)
                    letter = Convert.ToString((char)('A' + column));
                else if (column > 25 && column <= 51)
                {
                    int columnT = column % (26 * 1);
                    letter = "A";
                    string secondLetter = Convert.ToString((char)('A' + columnT));
                    letter += secondLetter;
                }
                else if (column > 51 && column <= 77)
                {
                    int columnT = column % (26 * 1);
                    letter = "B";
                    string secondLetter = Convert.ToString((char)('A' + columnT));
                    letter += secondLetter;
                }

                number = Convert.ToString(row + 1);

                cellName = letter + number;
            }
            catch
            {

            }
            return cellName;
        }

        /// <summary>
        /// HSSFRow Copy Command
        /// 
        /// Description:  Inserts a existing row into a new row, will automatically push down
        ///               any existing rows.  Copy is done cell by cell and supports, and the
        ///               command tries to copy all properties available (style, merged cells, values, etc...)
        /// </summary>
        /// <param name="workbook">Workbook containing the worksheet that will be changed</param>
        /// <param name="worksheet">WorkSheet containing rows to be copied</param>
        /// <param name="sourceRowNumber">Source Row Number</param>
        /// <param name="destinationRowNumber">Destination Row Number</param>
        public static void CopyRow(HSSFSheet worksheet, int sourceRowNumber, int destinationRowNumber)
        {
            // Get the source / new row
            HSSFRow newRow = (HSSFRow)worksheet.GetRow(destinationRowNumber);
            HSSFRow sourceRow = (HSSFRow)worksheet.GetRow(sourceRowNumber);

            // If the row exist in destination, push down all rows by 1 else create a new row
            if (newRow != null)
            {
                worksheet.ShiftRows(destinationRowNumber, worksheet.LastRowNum, 1);
            }
            else
            {
                newRow = (HSSFRow)worksheet.CreateRow(destinationRowNumber);
            }

            // Loop through source columns to add to new row
            for (int i = 0; i < sourceRow.LastCellNum; i++)
            {
                // Grab a copy of the old/new cell
                HSSFCell oldCell = (HSSFCell)sourceRow.GetCell(i);
                HSSFCell newCell = (HSSFCell)newRow.CreateCell(i);

                // If the old cell is null jump to next cell
                if (oldCell == null)
                {
                    newCell = null;
                    continue;
                }

                // Copy style from old cell and apply to new cell
                //HSSFCellStyle newCellStyle = (HSSFCellStyle)workbook.CreateCellStyle();
                //newCellStyle.CloneStyleFrom(oldCell.CellStyle); ;
                newCell.CellStyle = oldCell.CellStyle;

                // If there is a cell comment, copy
                if (newCell.CellComment != null) newCell.CellComment = oldCell.CellComment;

                // If there is a cell hyperlink, copy
                if (oldCell.Hyperlink != null) newCell.Hyperlink = oldCell.Hyperlink;

                // Set the cell data type
                newCell.SetCellType(oldCell.CellType);
                //Set the cell data value
                switch (oldCell.CellType)
                {
                    case CellType.Blank:
                        newCell.SetCellValue(oldCell.StringCellValue);
                        break;
                    case CellType.Boolean:
                        newCell.SetCellValue(oldCell.BooleanCellValue);
                        break;
                    case CellType.Error:
                        newCell.SetCellErrorValue(oldCell.ErrorCellValue);
                        break;
                    case CellType.Formula:
                        newCell.SetCellFormula(oldCell.CellFormula);
                        break;
                    case CellType.Numeric:
                        newCell.SetCellValue(oldCell.NumericCellValue);
                        break;
                    case CellType.String:
                        newCell.SetCellValue(oldCell.RichStringCellValue);
                        break;
                    case CellType.Unknown:
                        newCell.SetCellValue(oldCell.StringCellValue);
                        break;
                }
            }

            // If there are are any merged regions in the source row, copy to new row
            for (int i = 0; i < worksheet.NumMergedRegions; i++)
            {
                CellRangeAddress cellRangeAddress = worksheet.GetMergedRegion(i);
                if (cellRangeAddress.FirstRow == sourceRow.RowNum)
                {
                    CellRangeAddress newCellRangeAddress = new CellRangeAddress(newRow.RowNum,
                                                                                (newRow.RowNum +
                                                                                 (cellRangeAddress.LastRow -
                                                                                  cellRangeAddress.FirstRow)),
                                                                                cellRangeAddress.FirstColumn,
                                                                                cellRangeAddress.LastColumn);
                    worksheet.AddMergedRegion(newCellRangeAddress);
                }
            }

        }

        public static void copyCell(HSSFCell oldCell, HSSFCell newCell, bool copyStyle)
        {
            if (copyStyle)
            {
                newCell.CellStyle = oldCell.CellStyle;
            }

            switch (oldCell.CellType)
            {
                case CellType.String:
                    // newCell.setCellValue(oldCell.getStringCellValue());
                    newCell.SetCellValue(oldCell.RichStringCellValue);
                    break;

                case CellType.Numeric:
                    newCell.SetCellValue(oldCell.NumericCellValue);
                    break;

                case CellType.Blank:
                    newCell.SetCellValue("");
                    newCell.SetCellType(CellType.Blank);
                    break;

                case CellType.Boolean:
                    newCell.SetCellValue(oldCell.BooleanCellValue);
                    break;

                case CellType.Error:
                    newCell.SetCellValue(oldCell.ErrorCellValue);
                    break;

                case CellType.Formula:
                    break;

                default:
                    break;
            }
        }

        public static bool isNewMergedRegion(CellRangeAddress region, SortedList mergedRegions)
        {
            for (int i = 0; i < mergedRegions.Count; i++)
            {
                if (region.FirstRow >= ((CellRangeAddress)mergedRegions[i]).FirstRow &&
                    region.FirstColumn >= ((CellRangeAddress)mergedRegions[i]).FirstColumn &&
                    region.FirstRow <= ((CellRangeAddress)mergedRegions[i]).LastRow &&
                    region.FirstColumn <= ((CellRangeAddress)mergedRegions[i]).LastColumn)
                {
                    return false;
                }
            }
            return true;
        }

        public static CellRangeAddress getMergedRegion(HSSFSheet sheet, int rowNum, short cellNum)
        {
            for (int i = 0; i < sheet.NumMergedRegions; i++)
            {
                CellRangeAddress merged = sheet.GetMergedRegion(i);
                if (merged.FirstRow <= rowNum && rowNum <= merged.LastRow && merged.FirstColumn <= cellNum && cellNum <= merged.LastColumn)
                {
                    return merged;
                }
            }

            return null;
        }

        public static void copyCell(HSSFSheet srcSheet, HSSFSheet destSheet, int startRow, int endRow, short startCol, short endCol, int startRowDest, short startColDest)
        {
            try
            {
                HSSFRow[] srcRows = new HSSFRow[endRow - startRow + 1];
                HSSFRow[] destRows = new HSSFRow[endRow - startRow + 1];
                for (int i = startRow; i <= endRow; i++)
                {
                    HSSFRow srcRow = (HSSFRow)srcSheet.GetRow(i);
                    if (srcRow == null)
                    {
                        //Console.WriteLine(" row " + i + " in source sheet does not exist");
                        //return;
                    }
                    srcRows[i - startRow] = srcRow;

                    HSSFRow destRow = (HSSFRow)destSheet.GetRow(startRowDest + (i - startRow));
                    if (destRow == null)
                    {
                        destRow = (HSSFRow)destSheet.CreateRow(startRowDest + (i - startRow));
                    }

                    destRows[i - startRow] = destRow;
                }

                SortedList mergedRegions = new SortedList();

                for (int i = startRow; i <= endRow; i++)
                {
                    if (destRows[i - startRow] != null && srcRows[i - startRow] != null)
                    {
                        destRows[i - startRow].Height = srcRows[i - startRow].Height;
                    }
                }
                for (int i = startRow; i <= endRow; i++)
                {
                    int a = 1;

                    if (srcRows[i - startRow] == null)
                    {

                    }
                    else
                    {
                        for (short j = startCol; j <= endCol; j++)
                        {
                            HSSFCell oldCell = (HSSFCell)srcRows[i - startRow].GetCell(j);

                            int idxColDestInt = startColDest + (j - startCol);
                            short idxColDest = (short)idxColDestInt;
                            HSSFCell newCell = (HSSFCell)destRows[i - startRow].GetCell(idxColDest);

                            if (oldCell != null)
                            {
                                if (newCell == null)
                                {
                                    newCell = (HSSFCell)destRows[i - startRow].CreateCell(idxColDest);
                                }

                                copyCell(oldCell, newCell, true);
                                CellRangeAddress mergedRegion = getMergedRegion(srcSheet, srcRows[i - startRow].RowNum, (short)oldCell.ColumnIndex);
                                if (mergedRegion != null)
                                {
                                    int rowFrom = destRows[i - startRow].RowNum;

                                    short columnFrom = (short)(startColDest + (mergedRegion.FirstColumn - startCol));

                                    int rowTo = destRows[i - startRow].RowNum + (mergedRegion.LastRow - mergedRegion.FirstRow);

                                    short columnTo = (short)(startColDest + (mergedRegion.FirstColumn - startCol) + (mergedRegion.LastColumn - mergedRegion.FirstColumn));

                                    CellRangeAddress newMergedRegion = new CellRangeAddress(rowFrom, rowTo, columnFrom, columnTo);

                                    if (isNewMergedRegion(newMergedRegion, mergedRegions))
                                    {
                                        mergedRegions.Add(mergedRegions.Count, newMergedRegion);
                                        destSheet.AddMergedRegion(newMergedRegion);
                                    }
                                }
                            }
                        }
                    }
                }
                for (short j = startCol; j <= endCol; j++)
                {
                    int idxColDestInt = startColDest + (j - startCol);
                    short idxColDest = (short)idxColDestInt;
                    destSheet.SetColumnWidth(idxColDest, srcSheet.GetColumnWidth(j));
                }

            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                Console.Write(e.StackTrace);

            }

        }
    }
}

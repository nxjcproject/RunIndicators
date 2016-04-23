using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
namespace RunIndicators
{
    public class RunIndicatorsItems
    {
        public static DataTable GetRunIndicatorsItemsTable()
        {
            DataTable m_RunIndicatorsItemsTable = GetRunIndicatorsItemsTableStructure();
            m_RunIndicatorsItemsTable.Rows.Add("产量", "产量(t)", "t", "N01", "MaterialWeight");
            m_RunIndicatorsItemsTable.Rows.Add("台时产量", "台时产量(t/h)", "t/h", "N02", "MaterialWeight");
            m_RunIndicatorsItemsTable.Rows.Add("运转率", "运转率(%)", "%", "N03", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("可靠性", "可靠性(%)", "%", "N04", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("故障率", "故障率(%)", "%", "N05", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("运转时间", "运转时间(h)", "h", "N06", "EquipmentUtilization");

            m_RunIndicatorsItemsTable.Rows.Add("故障停机时间", "故障停机时间(h)", "h", "N07", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("工艺故障停机时间", "工艺故障停机时间(h)", "h", "N0701", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("机械故障停机时间", "机械故障停机时间(h)", "h", "N0702", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("电气故障停机时间", "电气故障停机时间(h)", "h", "N0703", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("环境停机时间", "环境停机时间(h)", "h", "N08", "EquipmentUtilization");

            m_RunIndicatorsItemsTable.Rows.Add("故障停机次数", "故障停机次数(次)", "次", "N09", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("工艺故障停机次数", "工艺故障故障停机次数(次)", "次", "N0901", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("机械故障停机次数", "机械故障停机次数(次)", "次", "N0902", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("电气故障停机次数", "电气故障停机次数(次)", "次", "N0903", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("环境停机次数", "环境停机次数(次)", "N10", "次", "EquipmentUtilization");


            m_RunIndicatorsItemsTable.Rows.Add("故障停机时间_8", "8小时故障停机时间(h)", "h", "N11", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("工艺故障停机时间_8", "8小时工艺故障故障停机时间(h)", "h", "N1101", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("机械故障停机时间_8", "8小时机械故障停机时间(h)", "h", "N1102", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("电气故障停机时间_8", "8小时电气故障停机时间(h)", "h", "N1103", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("环境停机时间_8", "8小时环境停机时间(h)", "h", "N12", "EquipmentUtilization");

            m_RunIndicatorsItemsTable.Rows.Add("故障停机次数_8", "8小时故障停机次数(次)", "次", "N13", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("工艺故障停机次数_8", "8小时工艺故障故障停机次数(次)", "次", "N1301", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("机械故障停机次数_8", "8小时机械故障停机次数(次)", "次", "N1302", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("电气故障停机次数_8", "8小时电气故障停机次数(次)", "次", "N1303", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("环境停机次数_8", "8小时环境停机次数(次)", "次", "N14", "EquipmentUtilization");


            m_RunIndicatorsItemsTable.Rows.Add("故障停机时间_24", "24小时故障停机时间(h)", "h", "N15", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("工艺故障停机时间_24", "24小时工艺故障故障停机时间(h)", "h", "N1501", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("机械故障停机时间_24", "24小时机械故障停机时间(h)", "h", "N1502", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("电气故障停机时间_24", "24小时电气故障停机时间(h)", "h", "N1503", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("环境停机时间_24", "24小时环境停机时间(h)", "h", "N16", "EquipmentUtilization");

            m_RunIndicatorsItemsTable.Rows.Add("故障停机次数_24", "24小时故障停机次数(次)", "次", "N17", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("工艺故障停机次数_24", "24小时工艺故障故障停机次数(次)", "次", "N1701", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("机械故障停机次数_24", "24小时机械故障停机次数(次)", "次", "N1702", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("电气故障停机次数_24", "24小时电气故障停机次数(次)", "次", "N1703", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("环境停机次数_24", "24小时环境停机次数(次)", "次", "N18", "EquipmentUtilization");
            return m_RunIndicatorsItemsTable;
        }
        private static DataTable GetRunIndicatorsItemsTableStructure()
        {
            DataTable m_RunIndicatorsItemsTable = new DataTable();
            m_RunIndicatorsItemsTable.Columns.Add("IndicatorId", typeof(string));
            m_RunIndicatorsItemsTable.Columns.Add("IndicatorName", typeof(string));
            m_RunIndicatorsItemsTable.Columns.Add("Unit", typeof(string));
            m_RunIndicatorsItemsTable.Columns.Add("LevelCode", typeof(string));
            m_RunIndicatorsItemsTable.Columns.Add("IndicatorType", typeof(string));
            return m_RunIndicatorsItemsTable;
        }
    }
}

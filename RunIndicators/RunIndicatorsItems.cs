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
            //m_RunIndicatorsItemsTable.Rows.Add("产量", "产量(t)", "t", "N01", "MaterialWeight");
            m_RunIndicatorsItemsTable.Rows.Add("台时产量", "台时产量(t/h)", "t/h", "N02", "MaterialWeight");
            m_RunIndicatorsItemsTable.Rows.Add("运转率", "运转率(%)", "%", "N03", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("可靠性", "可靠性(%)", "%", "N04", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("故障率", "故障率(%)", "%", "N05", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("运转时间", "运转时间(h)", "h", "N06", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("计划检修时间", "计划检修时间(h)", "h", "N07", "EquipmentUtilization");

            m_RunIndicatorsItemsTable.Rows.Add("故障停机时间", "故障停机时间(h)", "h", "N20", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("工艺故障停机时间", "工艺故障停机时间(h)", "h", "N2001", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("机械故障停机时间", "机械故障停机时间(h)", "h", "N2002", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("电气故障停机时间", "电气故障停机时间(h)", "h", "N2003", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("环境停机时间", "环境停机时间(h)", "h", "N21", "EquipmentUtilization");

            m_RunIndicatorsItemsTable.Rows.Add("故障停机次数", "故障停机次数(次)", "次", "N22", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("工艺故障停机次数", "工艺故障故障停机次数(次)", "次", "N2201", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("机械故障停机次数", "机械故障停机次数(次)", "次", "N2202", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("电气故障停机次数", "电气故障停机次数(次)", "次", "N2203", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("环境停机次数", "环境停机次数(次)", "N23", "次", "EquipmentUtilization");

            //m_RunIndicatorsItemsTable.Rows.Add("故障停机时间_8", "8小时故障停机时间(h)", "h", "N24", "EquipmentUtilization");
            //m_RunIndicatorsItemsTable.Rows.Add("工艺故障停机时间_8", "8小时工艺故障故障停机时间(h)", "h", "N2401", "EquipmentUtilization");
            //m_RunIndicatorsItemsTable.Rows.Add("机械故障停机时间_8", "8小时机械故障停机时间(h)", "h", "N2402", "EquipmentUtilization");
            //m_RunIndicatorsItemsTable.Rows.Add("电气故障停机时间_8", "8小时电气故障停机时间(h)", "h", "N2403", "EquipmentUtilization");
            //m_RunIndicatorsItemsTable.Rows.Add("环境停机时间_8", "8小时环境停机时间(h)", "h", "N25", "EquipmentUtilization");

            //m_RunIndicatorsItemsTable.Rows.Add("故障停机次数_8", "8小时故障停机次数(次)", "次", "N26", "EquipmentUtilization");
            //m_RunIndicatorsItemsTable.Rows.Add("工艺故障停机次数_8", "8小时工艺故障故障停机次数(次)", "次", "N2601", "EquipmentUtilization");
            //m_RunIndicatorsItemsTable.Rows.Add("机械故障停机次数_8", "8小时机械故障停机次数(次)", "次", "N2602", "EquipmentUtilization");
            //m_RunIndicatorsItemsTable.Rows.Add("电气故障停机次数_8", "8小时电气故障停机次数(次)", "次", "N2603", "EquipmentUtilization");
            //m_RunIndicatorsItemsTable.Rows.Add("环境停机次数_8", "8小时环境停机次数(次)", "次", "N27", "EquipmentUtilization");


            m_RunIndicatorsItemsTable.Rows.Add("故障停机时间_24", "24小时故障停机时间(h)", "h", "N28", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("工艺故障停机时间_24", "24小时工艺故障故障停机时间(h)", "h", "N2801", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("机械故障停机时间_24", "24小时机械故障停机时间(h)", "h", "N2802", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("电气故障停机时间_24", "24小时电气故障停机时间(h)", "h", "N2803", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("环境停机时间_24", "24小时环境停机时间(h)", "h", "N29", "EquipmentUtilization");

            m_RunIndicatorsItemsTable.Rows.Add("故障停机次数_24", "24小时故障停机次数(次)", "次", "N30", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("工艺故障停机次数_24", "24小时工艺故障故障停机次数(次)", "次", "N3001", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("机械故障停机次数_24", "24小时机械故障停机次数(次)", "次", "N3002", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("电气故障停机次数_24", "24小时电气故障停机次数(次)", "次", "N3003", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("环境停机次数_24", "24小时环境停机次数(次)", "次", "N31", "EquipmentUtilization");

            m_RunIndicatorsItemsTable.Rows.Add("故障停机时间_72", "72小时故障停机时间(h)", "h", "N32", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("工艺故障停机时间_72", "72小时工艺故障故障停机时间(h)", "h", "N3201", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("机械故障停机时间_72", "72小时机械故障停机时间(h)", "h", "N3202", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("电气故障停机时间_72", "72小时电气故障停机时间(h)", "h", "N3203", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("环境停机时间_72", "72小时环境停机时间(h)", "h", "N33", "EquipmentUtilization");

            m_RunIndicatorsItemsTable.Rows.Add("故障停机次数_72", "72小时故障停机次数(次)", "次", "N34", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("工艺故障停机次数_72", "72小时工艺故障故障停机次数(次)", "次", "N3401", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("机械故障停机次数_72", "72小时机械故障停机次数(次)", "次", "N3402", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("电气故障停机次数_72", "72小时电气故障停机次数(次)", "次", "N3403", "EquipmentUtilization");
            m_RunIndicatorsItemsTable.Rows.Add("环境停机次数_72", "72小时环境停机次数(次)", "次", "N35", "EquipmentUtilization");
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

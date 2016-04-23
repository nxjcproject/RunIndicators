using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using SqlServerDataAdapter;
namespace RunIndicators
{
    public class EquipmentRunIndicators
    {
        /// <summary>
        /// 设备可靠性计算方法
        /// </summary>
        /// <param name="myCalendarTime">日历时间</param>
        /// <param name="myRepairTime">检修时间</param>
        /// <param name="NormalStopTime">正常停机时间</param>
        /// <param name="myDownTime">故障停机</param>
        /// <returns>可靠性</returns>
        private static decimal GetReliability(decimal myCalendarTime, decimal myRepairTime, decimal NormalStopTime, decimal myDownTime)
        {
            if (myCalendarTime - myRepairTime - NormalStopTime != 0)
            {
                decimal m_Reliability = (myCalendarTime - myRepairTime - NormalStopTime - myDownTime) / (myCalendarTime - myRepairTime - NormalStopTime);
                return m_Reliability;
            }
            else
            {
                return 0;
            }
        }
        /// <summary>
        /// 设备运转率计算方法
        /// </summary>
        /// <param name="myCalendarTime">日历时间</param>
        /// <param name="myRepairTime">检修时间</param>
        /// <param name="NormalStopTime">正常停机时间</param>
        /// <param name="myDownTime">故障停机</param>
        /// <returns>运转率</returns>
        private static decimal GetRunningRate(decimal myCalendarTime, decimal myRepairTime, decimal NormalStopTime, decimal myDownTime)
        {
            decimal m_RunningRate = (myCalendarTime - myRepairTime - NormalStopTime - myDownTime) / myCalendarTime;
            return m_RunningRate;
        }
        /// <summary>
        /// 设备故障率计算方法
        /// </summary>
        /// <param name="myCalendarTime">日历时间</param>
        /// <param name="myRepairTime">检修时间</param>
        /// <param name="NormalStopTime">正常停机时间</param>
        /// <param name="myDownTime">故障停机</param>
        /// <returns>设备故障率</returns>
        private static decimal GetFailureRate(decimal myCalendarTime, decimal myRepairTime, decimal NormalStopTime, decimal myDownTime)
        {
            if (myCalendarTime - myRepairTime - NormalStopTime != 0)
            {
                decimal m_FailureRate = myDownTime / (myCalendarTime - myRepairTime - NormalStopTime);
                return m_FailureRate;
            }
            else
            {
                return 0;
            }
        }
        public static DataTable GetResultDataTable()
        {
            DataTable m_ResultDataTable = new DataTable();
            m_ResultDataTable.Columns.Add("EquipmentId",typeof(string));
            m_ResultDataTable.Columns.Add("January", typeof(decimal));
            m_ResultDataTable.Columns.Add("February", typeof(decimal));
            m_ResultDataTable.Columns.Add("March", typeof(decimal));
            m_ResultDataTable.Columns.Add("April", typeof(decimal));
            m_ResultDataTable.Columns.Add("May", typeof(decimal));
            m_ResultDataTable.Columns.Add("June", typeof(decimal));
            m_ResultDataTable.Columns.Add("July", typeof(decimal));
            m_ResultDataTable.Columns.Add("August", typeof(decimal));
            m_ResultDataTable.Columns.Add("September", typeof(decimal));
            m_ResultDataTable.Columns.Add("October", typeof(decimal));
            m_ResultDataTable.Columns.Add("November", typeof(decimal));
            m_ResultDataTable.Columns.Add("December", typeof(decimal));
            return m_ResultDataTable;
        }
        /// <summary>
        /// 按每月计算设备利用率
        /// </summary>
        /// <param name="myProductionQuotasId">生产指标ID</param>
        /// <param name="myOrganizationId">组织机构ID</param>
        /// <param name="myPlanYear">计划年份</param>
        /// <param name="myEquipmentCommonId">主要设备ID</param>
        /// <param name="myDataFactory">数据库连接类库</param>
        /// <returns>设备利用率表</returns>
        public static DataTable GetEquipmentUtilizationPerMonth(string myProductionQuotasId, string myOrganizationId, string myPlanYear, string myEquipmentCommonId, ISqlServerDataFactory myDataFactory)
        {
            string m_Sql = @"Select M.OrganizationID, M.EquipmentID as EquipmentId, M.ReasonID as ReasonId, convert(varchar(7),M.HaltTimeF,20) as HaltTimeMonthF, sum(M.HaltLong) as Value
                                from (SELECT A.MachineHaltLogID
                                      ,B.OrganizationID
                                      ,A.EquipmentID
	                                  ,A.HaltTime
	                                  ,A.RecoverTime
	                                  ,substring(A.ReasonID,1,3) as ReasonID
	                                  ,(case when convert(varchar(7),A.HaltTime,20) < D.PreMonth then  CONVERT(datetime,D.PreMonth + '-01 00:00:00') else A.HaltTime end) as HaltTimeF
	                                  ,(case when convert(varchar(7),A.RecoverTime,20) > D.PreMonth then dateadd(day,-1, dateadd(month, 1, CONVERT(datetime,D.PreMonth + '-01 23:59:59'))) else A.RecoverTime end) as RecoverTimeF
	                                  ,DATEDIFF (hour, (case when convert(varchar(7),A.HaltTime,20) < D.PreMonth then  CONVERT(datetime,D.PreMonth + '-01 00:00:00') else A.HaltTime end)
	                                  ,(case when convert(varchar(7),A.RecoverTime,20) > D.PreMonth then dateadd(day,-1, dateadd(month, 1, CONVERT(datetime,D.PreMonth + '-01 23:59:59'))) else A.RecoverTime end)) as HaltLong
                                  FROM shift_MachineHaltLog A, system_Organization B, system_Organization C, 
                                  (select convert(varchar(7),dateadd(month,number,@StartTime),120) as PreMonth
                                      from master..spt_values
                                      where type='P'
                                      and dateadd(month,number,@StartTime)<=@EndTime) D, equipment_EquipmentDetail E
                                  where A.HaltTime >= @StartTime
                                  and A.HaltTime <= @EndTime
                                  and (A.ReasonID like 'E01%' or A.ReasonID like 'E02%')
                                  and B.OrganizationID = @OrganizationID
                                  and C.LevelCode like B.LevelCode + '%'
                                  and A.OrganizationID = C.OrganizationID
                                  and convert(varchar(7),A.HaltTime,20) <= D.PreMonth and convert(varchar(7),RecoverTime,20) >= D.PreMonth
                                  and B.OrganizationID = E.OrganizationID
                                  and E.EquipmentCommonId = @EquipmentCommonId
                                  and A.EquipmentID = E.EquipmentID) M
                                  group by M.OrganizationID, M.EquipmentID, M.ReasonID, convert(varchar(7),M.HaltTimeF,20)
                                  order by M.OrganizationID, M.EquipmentID, convert(varchar(7),M.HaltTimeF,20), ReasonID";
            try
            {
                SqlParameter[] m_Parameters = { new SqlParameter("@OrganizationID", myOrganizationId)
                                              , new SqlParameter("@EquipmentCommonId", myEquipmentCommonId)
                                              , new SqlParameter("@StartTime", myPlanYear + "-01-01")
                                              , new SqlParameter("@EndTime", myPlanYear + "-12-31")};
                DataTable m_Result = myDataFactory.Query(m_Sql, m_Parameters);
                if (m_Result != null)
                {
                    DataTable m_ResultDataTable = GetResultDataTable();
                    string m_EquipmentId = "";
                    decimal m_NormalStopTime = 0.0m;   //正常停机(小时)
                    decimal m_DownTime = 0.0m;         //故障停机时间(小时)
                    decimal m_RepairTime = 0.0m;
                    decimal m_CalendarTime = 0.0m;
                    int m_MonthIndex = 0;
                    for (int i = 0; i < m_Result.Rows.Count; i++)
                    {
                        if (m_EquipmentId != m_Result.Rows[i]["EquipmentId"].ToString())      //如果是不同设备,需要另起一行
                        {
                            if (i != 0)           //如果不是第一次进入,则计算一次
                            {
                                if (myProductionQuotasId.Contains("运转率"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_MonthIndex] = GetRunningRate(m_CalendarTime, m_RepairTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("运转时间"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_MonthIndex] = m_CalendarTime - m_RepairTime - m_NormalStopTime - m_DownTime;
                                }
                                else if (myProductionQuotasId.Contains("台时产量"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_MonthIndex] = m_CalendarTime - m_RepairTime - m_NormalStopTime - m_DownTime;
                                }
                            }
                            DataRow m_NewRow = m_ResultDataTable.NewRow();
                            m_EquipmentId = m_Result.Rows[i]["EquipmentId"].ToString();
                            m_NewRow[0] = m_EquipmentId;

                            m_NormalStopTime = 0;
                            m_DownTime = 0;
                            m_RepairTime = 0;

                            DateTime m_MonthStart = DateTime.Parse(m_Result.Rows[i]["HaltTimeMonthF"].ToString() + "-01");
                            m_MonthIndex = m_MonthStart.Month;
                            m_CalendarTime = (m_MonthStart.AddMonths(1) - m_MonthStart).Hours;
                            if (m_Result.Rows[i]["ReasonId"].ToString() == "E01")
                            {
                                m_NormalStopTime = decimal.Parse(m_Result.Rows[i]["Value"].ToString());
                            }
                            else if (m_Result.Rows[i]["ReasonId"].ToString() == "E02")
                            {
                                m_DownTime = decimal.Parse(m_Result.Rows[i]["Value"].ToString());
                            }
                            m_Result.Rows.Add(m_NewRow);
                        }
                        else
                        {
                            if (DateTime.Parse(m_Result.Rows[i]["HaltTimeMonthF"].ToString() + "-01").Month != m_MonthIndex)   //如果是相同设备不同月份,需要计算,填入相应的月份中
                            {
                                if (myProductionQuotasId.Contains("运转率"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_MonthIndex] = GetRunningRate(m_CalendarTime, m_RepairTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("运转时间"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_MonthIndex] = m_CalendarTime - m_RepairTime - m_NormalStopTime - m_DownTime;
                                }
                                DateTime m_MonthStart = DateTime.Parse(m_Result.Rows[i]["HaltTimeMonthF"].ToString() + "-01");
                                m_MonthIndex = m_MonthStart.Month;
                                m_CalendarTime = (m_MonthStart.AddMonths(1) - m_MonthStart).Hours;

                                m_NormalStopTime = 0;
                                m_DownTime = 0;
                                m_RepairTime = 0;
                            }
                            if (m_Result.Rows[i]["ReasonId"].ToString() == "E01")
                            {
                                m_NormalStopTime = decimal.Parse(m_Result.Rows[i]["Value"].ToString());
                            }
                            else if (m_Result.Rows[i]["ReasonId"].ToString() == "E02")
                            {
                                m_DownTime = decimal.Parse(m_Result.Rows[i]["Value"].ToString());
                            }
                        }
                        if (i == m_Result.Rows.Count - 1)
                        {
                            if (myProductionQuotasId.Contains("运转率"))
                            {
                                m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_MonthIndex] = GetRunningRate(m_CalendarTime, m_RepairTime, m_NormalStopTime, m_DownTime);
                            }
                            else if (myProductionQuotasId.Contains("运转时间"))
                            {
                                m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_MonthIndex] = m_CalendarTime - m_RepairTime - m_NormalStopTime - m_DownTime;
                            }
                            else if (myProductionQuotasId.Contains("台时产量"))
                            {
                                m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_MonthIndex] = m_CalendarTime - m_RepairTime - m_NormalStopTime - m_DownTime;
                            }
                        }

                    }
                    return m_ResultDataTable;
                }
                else
                {
                    return null;
                }          
            }
            catch (Exception e)
            {
                return null;
            }
        }
        /// <summary>
        /// 台时产量
        /// </summary>
        /// <param name="myProductionQuotasId">生产指标ID</param>
        /// <param name="myOrganizationId">组织机构ID</param>
        /// <param name="myPlanYear">计划年份</param>
        /// <param name="myEquipmentCommonId">主要设备ID</param>
        /// <param name="myDataFactory">数据库连接类库</param>
        /// <returns></returns>
        public static DataTable GetMachineHourCapacityPerMonth(string myProductionQuotasId, string myOrganizationId, string myPlanYear, string myEquipmentCommonId, ISqlServerDataFactory myDataFactory)
        {
            DataTable m_WeightTable = RunIndicators.MaterialWeightResult.GetMaterialWeightResultByDenominatorPerMonth(myProductionQuotasId, myOrganizationId, myPlanYear, myEquipmentCommonId, myDataFactory);
            DataTable m_RunTimeTable = RunIndicators.EquipmentRunIndicators.GetEquipmentUtilizationPerMonth(myProductionQuotasId, myOrganizationId, myPlanYear, myEquipmentCommonId, myDataFactory);
            if (m_WeightTable != null && m_RunTimeTable != null)
            {
                bool m_ContainRunTimeRow = false;
                for (int i = 0; i < m_WeightTable.Rows.Count; i++)
                {
                    m_ContainRunTimeRow = false;
                    for (int j = 0; j < m_RunTimeTable.Rows.Count; j++)
                    {
                        if (m_WeightTable.Rows[i]["EquipmentId"].ToString() == m_RunTimeTable.Rows[i]["EquipmentId"].ToString())
                        {
                            for (int w = 1; w <= 12; w++)
                            {
                                decimal m_m_RunTimeTemp = (decimal)m_RunTimeTable.Rows[i][w];
                                if (m_m_RunTimeTemp > 0)
                                {
                                    m_WeightTable.Rows[i][w] = (decimal)m_WeightTable.Rows[i][w] / m_m_RunTimeTemp;
                                }
                                else
                                {
                                    m_WeightTable.Rows[i][w] = 0.0m;
                                }
                            }
                            m_ContainRunTimeRow = true;
                            break;
                        }
                    }
                    if (m_ContainRunTimeRow == false)          //如果运行时间没有找到,则整行数据全都为0
                    {
                        for (int w = 1; w <= 12; w++)
                        {
                            m_WeightTable.Rows[i][w] = 0.0m;
                        }
                    }
                }
            }
            return m_WeightTable;
        }
        public static DataTable GetEquipmentUtilization(string myProductionQuotasId, string myEquipmentCommonId, string myOrganizationId, ISqlServerDataFactory myDataFactory)
        {
            return null;
        }
    }
}

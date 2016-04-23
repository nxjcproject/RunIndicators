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
        private const string RunTimeQuotasId = "运转时间";
        #region 可靠性、运转率等基本运算单元
        /// <summary>
        /// 设备可靠性计算方法
        /// </summary>
        /// <param name="myCalendarTime">日历时间</param>
        /// <param name="myRepairTime">检修时间</param>
        /// <param name="NormalStopTime">正常停机时间</param>
        /// <param name="myDownTime">故障停机</param>
        /// <returns>可靠性</returns>
        private static decimal GetReliability(decimal myCalendarTime, decimal myRepairTime, decimal myEnvironmentTime, decimal myNormalStopTime, decimal myDownTime)
        {
            //(日历时间 - 检修时间 - 正常停机时间 - 环境停机时间 - 故障停机时间) / (日历时间 - 检修时间 - 故障停机时间)
            //即:运行时间/(运行时间 + 故障停机)
            if (myCalendarTime - myRepairTime - myNormalStopTime - myEnvironmentTime != 0)
            {
                decimal m_Reliability = (myCalendarTime - myRepairTime - myNormalStopTime - myEnvironmentTime - myDownTime) / (myCalendarTime - myRepairTime - myNormalStopTime - myEnvironmentTime);
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
        private static decimal GetRunningRate(decimal myCalendarTime, decimal myRepairTime, decimal myEnvironmentTime, decimal NormalStopTime, decimal myDownTime)
        {
            if (myCalendarTime != 0)
            {
                decimal m_RunningRate = (myCalendarTime - myRepairTime - NormalStopTime - myDownTime) / myCalendarTime;
                return m_RunningRate;
            }
            else
            {
                return 0.0m;
            }
        }
        /// <summary>
        /// 设备故障率计算方法
        /// </summary>
        /// <param name="myCalendarTime">日历时间</param>
        /// <param name="myRepairTime">检修时间</param>
        /// <param name="NormalStopTime">正常停机时间</param>
        /// <param name="myDownTime">故障停机</param>
        /// <returns>设备故障率</returns>
        private static decimal GetFailureRate(decimal myCalendarTime, decimal myRepairTime, decimal myEnvironmentTime, decimal NormalStopTime, decimal myDownTime)
        {
            if (myCalendarTime - myRepairTime - NormalStopTime != 0)
            {
                decimal m_FailureRate = myDownTime / (myCalendarTime - myRepairTime - NormalStopTime - myEnvironmentTime);
                return m_FailureRate;
            }
            else
            {
                return 0;
            }
        }
        private static decimal GetRunTime(decimal myCalendarTime, decimal myRepairTime, decimal myEnvironmentTime, decimal NormalStopTime, decimal myDownTime)
        {
            return myCalendarTime - myRepairTime - NormalStopTime - myDownTime;
        }
        #endregion
        #region 按月查询构造结果表结构
        public static DataTable GetResultDataTable()
        {
            DataTable m_ResultDataTable = new DataTable();
            m_ResultDataTable.Columns.Add("EquipmentId", typeof(string));
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
        public static DataTable GetResultDataTable(string myStartTime, string myEndTime)
        {
            DateTime m_StartTime = DateTime.Parse(myStartTime);
            DateTime m_EndTime = DateTime.Parse(myEndTime);
            DataTable m_ResultDataTable = new DataTable();
            m_ResultDataTable.Columns.Add("EquipmentId", typeof(string));
            m_ResultDataTable.Columns.Add("EquipmentName", typeof(string));
            while (m_StartTime < m_EndTime)
            {
                m_ResultDataTable.Columns.Add(m_StartTime.ToString("yyyy-MM"), typeof(decimal));
                m_StartTime = m_StartTime.AddMonths(1);
            }
            return m_ResultDataTable;
        }
        #endregion
        ///////////////////////////////////计算除台时产量的所有指标////////////////////////////
        #region 一年内按月查询(英文月份标识)
        /// <summary>
        /// 按每月计算设备利用率(按全年计算),该计算方法按照EquipmentCommonId进行计算，得到该生产区域的所有属于EquipmentCommonId的具体设备
        /// </summary>
        /// <param name="myProductionQuotasId">生产指标ID</param>
        /// <param name="myOrganizationId">组织机构ID</param>
        /// <param name="myPlanYear">计划年份</param>
        /// <param name="myEquipmentCommonId">主要设备CommonID</param>
        /// <param name="myDataFactory">数据库连接类库</param>
        /// <returns>设备利用率表</returns>
        public static DataTable GetEquipmentUtilizationPerMonth(string myProductionQuotasId, string myOrganizationId, string myPlanYear, string myEquipmentCommonId, ISqlServerDataFactory myDataFactory)
        {
            string m_Sql = @"Select P.OrganizationID, P.EquipmentId, P.PreMonth as HaltTimeMonthF, (case when Q.ReasonStatisticsTypeId is null then '' else Q.ReasonStatisticsTypeId end) as ReasonStatisticsTypeId, 
                                sum(case when P.HaltLong is null then 0 else P.HaltLong end) as Value from 
	                            (Select N.MachineHaltLogID,
		                            M.OrganizationID,
		                            M.EquipmentId,
		                            N.HaltTime,
		                            N.RecoverTime,
		                            M.PreMonth,
		                            N.ReasonID,
		                            (case when convert(varchar(7),N.HaltTime,20) < M.PreMonth then  CONVERT(datetime,M.PreMonth + '-01 00:00:00') else N.HaltTime end) as HaltTimeF
									     ,(case when convert(varchar(7),N.RecoverTime,20) > M.PreMonth then dateadd(day,-1, dateadd(month, 1, CONVERT(datetime,M.PreMonth + '-01 23:59:59'))) else N.RecoverTime end) as RecoverTimeF
										 ,convert(decimal(18,4), DATEDIFF (second, (case when convert(varchar(7),N.HaltTime,20) < M.PreMonth then  CONVERT(datetime,M.PreMonth + '-01 00:00:00') else N.HaltTime end)
										 ,(case when convert(varchar(7),N.RecoverTime,20) > M.PreMonth then dateadd(day,-1, dateadd(month, 1, CONVERT(datetime,M.PreMonth + '-01 23:59:59'))) else N.RecoverTime end)) / 3600.00) as HaltLong

		                             from
		                            (Select A.EquipmentId, B.PreMonth as PreMonth, F.OrganizationID from 
		                                equipment_EquipmentDetail A,
			                            (select convert(varchar(7),dateadd(month,number,'{0}'),120) as PreMonth
													                            from master..spt_values
													                            where type='P'
													                            and dateadd(month,number,'{0}')<='{1}') B

			                            ,system_Organization E, system_Organization F
			                            where F.OrganizationID = '{2}'
                                        and E.LevelCode like F.LevelCode + '%'
                                        and E.LevelType = 'Factory'
                                        and A.OrganizationID = E.OrganizationID
			                            and A.EquipmentCommonId = '{3}') M
	                                left join 
		                                (Select C.MachineHaltLogID, C.EquipmentId, C.HaltTime, C.RecoverTime, C.ReasonID from shift_MachineHaltLog C
			                                where ((C.HaltTime >= '{0} 00:00:00'
			                                    and C.HaltTime <= '{1} 23:59:59')
                                            or (C.RecoverTime >= '{0} 00:00:00'
			                                    and C.RecoverTime <= '{1} 23:59:59'))
                                           ) N on M.EquipmentId = N.EquipmentId and convert(varchar(7),N.HaltTime,20) <= M.PreMonth and convert(varchar(7),N.RecoverTime,20) >= M.PreMonth) P
                               left join system_MachineHaltReason Q on P.ReasonID = Q.MachineHaltReasonID
                            group by P.OrganizationID, P.EquipmentId, P.PreMonth, Q.ReasonStatisticsTypeId
                            order by  P.OrganizationID, P.EquipmentId, P.PreMonth, Q.ReasonStatisticsTypeId";
            try
            {
                m_Sql = string.Format(m_Sql, myPlanYear + "-01-01", myPlanYear + DateTime.Now.ToString("-MM-dd"), myOrganizationId, myEquipmentCommonId);
                DataTable m_Result = myDataFactory.Query(m_Sql);
                if (m_Result != null)
                {
                    DataTable m_ResultDataTable = GetResultDataTable();
                    string m_EquipmentId = "";
                    decimal m_EnvironmentTime = 0.0m;   //环境停机(小时)
                    decimal m_DownTime = 0.0m;          //故障停机时间(小时)
                    decimal m_RepairTime = 0.0m;        //计划检修时间(小时)
                    decimal m_NormalStopTime = 0.0m;        //正常停机(小时)
                    decimal m_CalendarTime = 0.0m;      //日历时间(小时)
                    int m_MonthIndex = 0;
                    for (int i = 0; i < m_Result.Rows.Count; i++)
                    {
                        if (m_EquipmentId != m_Result.Rows[i]["EquipmentId"].ToString())      //如果是不同设备,需要另起一行
                        {
                            if (i != 0)           //如果不是第一次进入,则计算一次
                            {
                                if (myProductionQuotasId.Contains("运转率"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_MonthIndex] = GetRunningRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("运转时间"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_MonthIndex] = GetRunTime(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                            }
                            DataRow m_NewRow = m_ResultDataTable.NewRow();
                            m_EquipmentId = m_Result.Rows[i]["EquipmentId"].ToString();
                            m_NewRow[0] = m_EquipmentId;

                            m_EnvironmentTime = 0;
                            m_DownTime = 0;
                            m_RepairTime = 0;
                            m_NormalStopTime = 0;

                            DateTime m_MonthStart = DateTime.Parse(m_Result.Rows[i]["HaltTimeMonthF"].ToString() + "-01");
                            m_MonthIndex = m_MonthStart.Month;
                            m_CalendarTime = (m_MonthStart.AddMonths(1) - m_MonthStart).Hours + (m_MonthStart.AddMonths(1) - m_MonthStart).Days * 24;
                            if (m_Result.Rows[i]["ReasonStatisticsTypeId"].ToString() == "EnvironmentTime")
                            {
                                m_EnvironmentTime = m_EnvironmentTime + decimal.Parse(m_Result.Rows[i]["Value"].ToString());
                            }
                            else if (m_Result.Rows[i]["ReasonStatisticsTypeId"].ToString() != "ProcessDowntime" ||
                                     m_Result.Rows[i]["ReasonStatisticsTypeId"].ToString() != "MechanicalDowntime" ||
                                     m_Result.Rows[i]["ReasonStatisticsTypeId"].ToString() != "ElectricalDowntime")
                            {
                                m_DownTime = m_DownTime + decimal.Parse(m_Result.Rows[i]["Value"].ToString());
                            }
                            else if (m_Result.Rows[i]["ReasonStatisticsTypeId"].ToString() != "RepairTime")
                            {
                                m_RepairTime = m_RepairTime + decimal.Parse(m_Result.Rows[i]["Value"].ToString());
                            }
                            else if (m_Result.Rows[i]["ReasonStatisticsTypeId"].ToString() != "NormalStopTime")
                            {
                                m_NormalStopTime = m_NormalStopTime + decimal.Parse(m_Result.Rows[i]["Value"].ToString());
                            }
                            m_ResultDataTable.Rows.Add(m_NewRow);
                        }
                        else
                        {
                            if (DateTime.Parse(m_Result.Rows[i]["HaltTimeMonthF"].ToString() + "-01").Month != m_MonthIndex)   //如果是相同设备不同月份,需要计算,填入相应的月份中
                            {
                                if (myProductionQuotasId.Contains("运转率"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_MonthIndex] = GetRunningRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("运转时间"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_MonthIndex] = GetRunTime(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                DateTime m_MonthStart = DateTime.Parse(m_Result.Rows[i]["HaltTimeMonthF"].ToString() + "-01");
                                m_MonthIndex = m_MonthStart.Month;
                                m_CalendarTime = (m_MonthStart.AddMonths(1) - m_MonthStart).Hours + (m_MonthStart.AddMonths(1) - m_MonthStart).Days * 24;

                                m_EnvironmentTime = 0;
                                m_DownTime = 0;
                                m_RepairTime = 0;
                                m_NormalStopTime = 0;
                            }
                            if (m_Result.Rows[i]["ReasonStatisticsTypeId"].ToString() == "EnvironmentTime")
                            {
                                m_EnvironmentTime = m_EnvironmentTime + decimal.Parse(m_Result.Rows[i]["Value"].ToString());
                            }
                            else if (m_Result.Rows[i]["ReasonStatisticsTypeId"].ToString() != "ProcessDowntime" ||
                                     m_Result.Rows[i]["ReasonStatisticsTypeId"].ToString() != "MechanicalDowntime" ||
                                     m_Result.Rows[i]["ReasonStatisticsTypeId"].ToString() != "ElectricalDowntime")
                            {
                                m_DownTime = m_DownTime + decimal.Parse(m_Result.Rows[i]["Value"].ToString());
                            }
                            else if (m_Result.Rows[i]["ReasonStatisticsTypeId"].ToString() != "RepairTime")
                            {
                                m_RepairTime = m_RepairTime + decimal.Parse(m_Result.Rows[i]["Value"].ToString());
                            }
                            else if (m_Result.Rows[i]["ReasonStatisticsTypeId"].ToString() != "NormalStopTime")
                            {
                                m_NormalStopTime = m_NormalStopTime + decimal.Parse(m_Result.Rows[i]["Value"].ToString());
                            }
                        }
                        if (i == m_Result.Rows.Count - 1)
                        {
                            if (myProductionQuotasId.Contains("运转率"))
                            {
                                m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_MonthIndex] = GetRunningRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                            }
                            else if (myProductionQuotasId.Contains("运转时间"))
                            {
                                m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_MonthIndex] = GetRunTime(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
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
        /// 全年按每月计算台时产量。该计算方法按照EquipmentCommonId进行计算，得到该生产区域的所有属于EquipmentCommonId的具体设备
        /// </summary>
        /// <param name="myProductionQuotasId">生产指标ID</param>
        /// <param name="myOrganizationId">组织机构ID</param>
        /// <param name="myPlanYear">计划年份</param>
        /// <param name="myEquipmentCommonId">主要设备ID</param>
        /// <param name="myDataFactory">数据库连接类库</param>
        /// <returns></returns>
        public static DataTable GetMachineHourCapacityPerMonth(string myProductionQuotasId, string myOrganizationId, string myPlanYear, string myEquipmentCommonId, ISqlServerDataFactory myDataFactory)
        {
            DataTable m_WeightTable = RunIndicators.MaterialWeightResult.GetMaterialWeightResultPerMonth(myProductionQuotasId, myOrganizationId, myPlanYear, myEquipmentCommonId, myDataFactory);
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
        #endregion
        #region 时间范围内按月查询（标准年月标识）
        
        /// <summary>
        /// 按每月计算设备利用率(按按开始时间和结束之间计算),该计算方法按照EqupmentId计算具体设备每月的指标
        /// </summary>
        /// <param name="myProductionQuotasId">生产指标ID</param>
        /// <param name="myOrganizationId">组织机构ID</param>
        /// <param name="myEquipmentId">设备ID</param>
        /// <param name="myStartTime">开始时间</param>
        /// <param name="myEndTime">结束时间</param>
        /// <param name="myDataFactory">数据库适配器</param>
        /// <returns></returns>
        public static DataTable GetEquipmentUtilizationPerMonth(string myProductionQuotasId, string myOrganizationId, string myEquipmentId, string myStartTime, string myEndTime, ISqlServerDataFactory myDataFactory)
        {
            if (myProductionQuotasId.Contains("台时产量"))
            {
                DataTable m_ResultDataTable = GetEquipmentHourCapacityPerMonth(myProductionQuotasId, myOrganizationId, myEquipmentId, myStartTime, myEndTime, myDataFactory);
                return m_ResultDataTable;
            }
            else
            {
                DataTable m_HaltLogDataTable = GetEquipmentHaltLogDataPerMonth(myOrganizationId, myEquipmentId, myStartTime, myEndTime, myDataFactory);
                if (m_HaltLogDataTable != null)
                {
                    DataTable m_ResultDataTable = GetResultDataTable(myStartTime, myEndTime);
                    string m_EquipmentId = "";
                    decimal m_EnvironmentTime = 0.0m;   //正常停机(小时)
                    decimal m_DownTime = 0.0m;         //故障停机时间(小时)
                    decimal m_RepairTime = 0.0m;
                    decimal m_CalendarTime = 0.0m;
                    decimal m_NormalStopTime = 0.0m;
                    string m_ColumnName = "";
                    for (int i = 0; i < m_HaltLogDataTable.Rows.Count; i++)
                    {
                        if (m_EquipmentId != m_HaltLogDataTable.Rows[i]["EquipmentId"].ToString())      //如果是不同设备,需要另起一行
                        {
                            if (i != 0)           //如果不是第一次进入,则计算一次
                            {
                                if (myProductionQuotasId.Contains("运转率"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetRunningRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("故障率"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetFailureRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("可靠性"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetReliability(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("运转时间"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetRunTime(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("计划检修时间"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = m_RepairTime;
                                }
                            }
                            DataRow m_NewRow = m_ResultDataTable.NewRow();
                            m_EquipmentId = m_HaltLogDataTable.Rows[i]["EquipmentId"].ToString();
                            m_NewRow[0] = m_EquipmentId;
                            m_NewRow[1] = m_HaltLogDataTable.Rows[i]["EquipmentName"].ToString(); 
                            m_EnvironmentTime = 0;
                            m_DownTime = 0;
                            m_RepairTime = 0;
                            m_NormalStopTime = 0;

                            DateTime m_MonthStart = DateTime.Parse(m_HaltLogDataTable.Rows[i]["HaltTimeMonthF"].ToString() + "-01");
                            m_ColumnName = m_MonthStart.ToString("yyyy-MM");
                            m_CalendarTime = (m_MonthStart.AddMonths(1) - m_MonthStart).Hours + (m_MonthStart.AddMonths(1) - m_MonthStart).Days * 24;
                            if (m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() == "EnvironmentTime")
                            {
                                m_EnvironmentTime = m_EnvironmentTime + decimal.Parse(m_HaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                            else if (m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "ProcessDowntime" ||
                                     m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "MechanicalDowntime" ||
                                     m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "ElectricalDowntime")
                            {
                                m_DownTime = m_DownTime + decimal.Parse(m_HaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                            else if (m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "RepairTime")
                            {
                                m_RepairTime = m_RepairTime + decimal.Parse(m_HaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                            else if (m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "NormalStopTime")
                            {
                                m_NormalStopTime = m_NormalStopTime + decimal.Parse(m_HaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                            m_ResultDataTable.Rows.Add(m_NewRow);
                        }
                        else
                        {
                            if (DateTime.Parse(m_HaltLogDataTable.Rows[i]["HaltTimeMonthF"].ToString() + "-01").ToString("yyyy-MM") != m_ColumnName)   //如果是相同设备不同月份,需要计算,填入相应的月份中
                            {
                                if (myProductionQuotasId.Contains("运转率"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetRunningRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("故障率"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetFailureRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("可靠性"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetReliability(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("运转时间"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetRunTime(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("计划检修时间"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = m_RepairTime;
                                }
                                DateTime m_MonthStart = DateTime.Parse(m_HaltLogDataTable.Rows[i]["HaltTimeMonthF"].ToString() + "-01");
                                m_ColumnName = m_MonthStart.ToString("yyyy-MM");
                                m_CalendarTime = (m_MonthStart.AddMonths(1) - m_MonthStart).Hours + (m_MonthStart.AddMonths(1) - m_MonthStart).Days * 24;
                                m_EnvironmentTime = 0;
                                m_DownTime = 0;
                                m_RepairTime = 0;
                                m_NormalStopTime = 0;

                            }
                            if (m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() == "EnvironmentTime")
                            {
                                m_EnvironmentTime = m_EnvironmentTime + decimal.Parse(m_HaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                            else if (m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "ProcessDowntime" ||
                                     m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "MechanicalDowntime" ||
                                     m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "ElectricalDowntime")
                            {
                                m_DownTime = m_DownTime + decimal.Parse(m_HaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                            else if (m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "RepairTime")
                            {
                                m_RepairTime = m_RepairTime + decimal.Parse(m_HaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                            else if (m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "NormalStopTime")
                            {
                                m_NormalStopTime = m_NormalStopTime + decimal.Parse(m_HaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                        }
                        if (i == m_HaltLogDataTable.Rows.Count - 1)
                        {
                            if (myProductionQuotasId.Contains("运转率"))
                            {
                                m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetRunningRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                            }
                            else if (myProductionQuotasId.Contains("故障率"))
                            {
                                m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetFailureRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                            }
                            else if (myProductionQuotasId.Contains("可靠性"))
                            {
                                m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetReliability(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                            }
                            else if (myProductionQuotasId.Contains("运转时间"))
                            {
                                m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetRunTime(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                            }
                            else if (myProductionQuotasId.Contains("计划检修时间"))
                            {
                                m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = m_RepairTime;
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
        }
        public static DataTable GetEquipmentHaltLogDataPerMonth(string myOrganizationId, string myEquipmentId, string myStartTime, string myEndTime, ISqlServerDataFactory myDataFactory)
        {
            string m_Sql = @"Select P.OrganizationID, P.EquipmentId, P.EquipmentName, P.PreMonth as HaltTimeMonthF, (case when Q.ReasonStatisticsTypeId is null then '' else Q.ReasonStatisticsTypeId end) as ReasonStatisticsTypeId, 
                                sum(case when P.HaltLong is null then 0 else P.HaltLong end) as Value from 
	                            (Select N.MachineHaltLogID,
		                            M.OrganizationID,
		                            M.EquipmentId,
                                    M.EquipmentName,
		                            N.HaltTime,
		                            N.RecoverTime,
		                            M.PreMonth,
		                            N.ReasonID,
		                            (case when convert(varchar(7),N.HaltTime,20) < M.PreMonth then  CONVERT(datetime,M.PreMonth + '-01 00:00:00') else N.HaltTime end) as HaltTimeF
									    ,(case when convert(varchar(7),N.RecoverTime,20) > M.PreMonth then dateadd(day,-1, dateadd(month, 1, CONVERT(datetime,M.PreMonth + '-01 23:59:59'))) else N.RecoverTime end) as RecoverTimeF
										 ,convert(decimal(18,4), DATEDIFF (second, (case when convert(varchar(7),N.HaltTime,20) < M.PreMonth then  CONVERT(datetime,M.PreMonth + '-01 00:00:00') else N.HaltTime end)
											                              ,(case when convert(varchar(7),N.RecoverTime,20) > M.PreMonth then dateadd(day,-1, dateadd(month, 1, CONVERT(datetime,M.PreMonth + '-01 23:59:59'))) else N.RecoverTime end)) / 3600.00) as HaltLong

		                             from
		                            (Select A.EquipmentId, B.PreMonth as PreMonth, F.OrganizationID, A.EquipmentName from 
		                                equipment_EquipmentDetail A,
			                            (select convert(varchar(7),dateadd(month,number,'{0}'),120) as PreMonth
													                            from master..spt_values
													                            where type='P'
													                            and dateadd(month,number,'{0}')<='{1}') B
                                   ,system_Organization E, system_Organization F
			                            where F.OrganizationID = '{2}'
                                        and E.LevelCode like F.LevelCode + '%'
                                        and E.LevelType = 'Factory'
                                        and A.OrganizationID = E.OrganizationID
			                            and A.EquipmentId = '{3}') M
	                                left join 
		                                (Select C.MachineHaltLogID, C.EquipmentId, C.HaltTime, C.RecoverTime, C.ReasonID from shift_MachineHaltLog C
			                                where ((C.HaltTime >= '{0} 00:00:00'
			                                    and C.HaltTime <= '{1} 23:59:59')
                                            or (C.RecoverTime >= '{0} 00:00:00'
			                                    and C.RecoverTime <= '{1} 23:59:59'))
                                           ) N on M.EquipmentId = N.EquipmentId and convert(varchar(7),N.HaltTime,20) <= M.PreMonth and convert(varchar(7),N.RecoverTime,20) >= M.PreMonth) P
                               left join system_MachineHaltReason Q on P.ReasonID = Q.MachineHaltReasonID
                            group by P.OrganizationID, P.EquipmentId, P.EquipmentName, P.PreMonth, Q.ReasonStatisticsTypeId
                            order by  P.OrganizationID, P.EquipmentId, P.PreMonth, Q.ReasonStatisticsTypeId";
            try
            {
                m_Sql = string.Format(m_Sql, myStartTime, myEndTime, myOrganizationId, myEquipmentId);
                DataTable m_Result = myDataFactory.Query(m_Sql);
                return m_Result;
            }
            catch (Exception e)
            {
                return null;
            }
        }
        public static DataTable GetEquipmentHourCapacityPerMonth(string myProductionQuotasId, string myOrganizationId, string myEquipmentId, string myStartTime, string myEndTime, ISqlServerDataFactory myDataFactory)
        {
            DataTable m_EquipmentUtilizationTable = GetEquipmentUtilizationPerMonth(RunTimeQuotasId, myOrganizationId, myEquipmentId, myStartTime, myEndTime, myDataFactory);
            DataTable m_WeightTable = RunIndicators.MaterialWeightResult.GetMaterialWeightResultPerMonthS(myOrganizationId, myEquipmentId, myStartTime, myEndTime, myDataFactory);
            if (m_WeightTable != null && m_EquipmentUtilizationTable != null)
            {
                bool m_ContainRunTimeRow = false;
                for (int i = 0; i < m_WeightTable.Rows.Count; i++)
                {
                    m_ContainRunTimeRow = false;
                    for (int j = 0; j < m_EquipmentUtilizationTable.Rows.Count; j++)
                    {
                        if (m_WeightTable.Rows[i]["EquipmentId"].ToString() == m_EquipmentUtilizationTable.Rows[i]["EquipmentId"].ToString())
                        {
                            for (int w = 1; w < m_WeightTable.Columns.Count; w++)
                            {
                                decimal m_RunTimeTemp = (decimal)m_EquipmentUtilizationTable.Rows[i][w];
                                if (m_RunTimeTemp > 0)
                                {
                                    m_WeightTable.Rows[i][w] = (decimal)m_WeightTable.Rows[i][w] / m_RunTimeTemp;
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
        
        /// <summary>
        /// 按每月计算设备利用率(按按开始时间和结束之间计算),该计算方法按照EquipmentCommonId计算该类设备每月的指标（多种CommonId）
        /// </summary>
        /// <param name="myProductionQuotasId">生产指标</param>
        /// <param name="myEquipmentCommonId">主要设备CommonID</param>
        /// <param name="myOrganizationId">组织机构ID</param>
        /// <param name="myStartTime">开始时间</param>
        /// <param name="myEndTime">结束时间</param>
        /// <param name="myDataFactory">数据库适配器</param>
        /// <returns></returns>
        public static DataTable GetEquipmentCommonUtilizationPerMonth(string myProductionQuotasId, string myEquipmentCommonIdList, string myOrganizationId, string myStartTime, string myEndTime, ISqlServerDataFactory myDataFactory)
        {
            if (myProductionQuotasId == "台时产量")
            {
                DataTable m_ResultDataTable = GetEquipmentCommonHourCapacityPerMonth(RunTimeQuotasId, myOrganizationId, myEquipmentCommonIdList, myStartTime, myEndTime, myDataFactory);
                return m_ResultDataTable;
            }
            else
            {
                DataTable m_HaltLogDataTable = GetEquipmentHaltLogDataMPerMonth(myEquipmentCommonIdList, myOrganizationId, myStartTime, myEndTime, myDataFactory);
                if (m_HaltLogDataTable != null)
                {
                    DataTable m_ResultDataTable = GetResultDataTable(myStartTime, myEndTime);
                    string m_EquipmentCommonId = "";
                    decimal m_EnvironmentTime = 0.0m;   //正常停机(小时)
                    decimal m_DownTime = 0.0m;         //故障停机时间(小时)
                    decimal m_RepairTime = 0.0m;
                    decimal m_CalendarTime = 0.0m;
                    decimal m_NormalStopTime = 0.0m;
                    string m_ColumnName = "";
                    for (int i = 0; i < m_HaltLogDataTable.Rows.Count; i++)
                    {
                        if (m_EquipmentCommonId != m_HaltLogDataTable.Rows[i]["EquipmentCommonId"].ToString())      //如果是不同设备,需要另起一行
                        {
                            if (i != 0)           //如果不是第一次进入,则计算一次
                            {
                                if (myProductionQuotasId.Contains("运转率"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetRunningRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("故障率"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetFailureRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("可靠性"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetReliability(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("运转时间"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetRunTime(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("计划检修时间"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = m_RepairTime;
                                }
                            }
                            DataRow m_NewRow = m_ResultDataTable.NewRow();
                            m_EquipmentCommonId = m_HaltLogDataTable.Rows[i]["EquipmentCommonId"].ToString();
                            m_NewRow[0] = m_EquipmentCommonId;
                            m_NewRow[1] = m_HaltLogDataTable.Rows[i]["EquipmentCommonName"].ToString();
                            m_EnvironmentTime = 0;
                            m_DownTime = 0;
                            m_RepairTime = 0;
                            m_NormalStopTime = 0;
                            m_CalendarTime = 0;

                            DateTime m_MonthStart = DateTime.Parse(m_HaltLogDataTable.Rows[i]["HaltTimeMonthF"].ToString() + "-01");
                            m_ColumnName = m_MonthStart.ToString("yyyy-MM");
                            m_CalendarTime = m_CalendarTime + (m_MonthStart.AddMonths(1) - m_MonthStart).Hours + (m_MonthStart.AddMonths(1) - m_MonthStart).Days * 24;
                            if (m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() == "EnvironmentTime")
                            {
                                m_EnvironmentTime = m_EnvironmentTime + decimal.Parse(m_HaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                            else if (m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "ProcessDowntime" ||
                                     m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "MechanicalDowntime" ||
                                     m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "ElectricalDowntime")
                            {
                                m_DownTime = m_DownTime + decimal.Parse(m_HaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                            else if (m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "RepairTime")
                            {
                                m_RepairTime = m_RepairTime + decimal.Parse(m_HaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                            else if (m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "NormalStopTime")
                            {
                                m_NormalStopTime = m_NormalStopTime + decimal.Parse(m_HaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                            m_ResultDataTable.Rows.Add(m_NewRow);
                        }
                        else
                        {
                            if (DateTime.Parse(m_HaltLogDataTable.Rows[i]["HaltTimeMonthF"].ToString() + "-01").ToString("yyyy-MM") != m_ColumnName)   //如果是相同设备不同月份,需要计算,填入相应的月份中
                            {
                                if (myProductionQuotasId.Contains("运转率"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetRunningRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("故障率"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetFailureRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("可靠性"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetReliability(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("运转时间"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetRunTime(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                                }
                                else if (myProductionQuotasId.Contains("计划检修时间"))
                                {
                                    m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = m_RepairTime;
                                }
                                m_CalendarTime = 0;
                                m_EnvironmentTime = 0;
                                m_DownTime = 0;
                                m_RepairTime = 0;
                                m_NormalStopTime = 0;
                            }
                            DateTime m_MonthStart = DateTime.Parse(m_HaltLogDataTable.Rows[i]["HaltTimeMonthF"].ToString() + "-01");
                            m_ColumnName = m_MonthStart.ToString("yyyy-MM");
                            m_CalendarTime = m_CalendarTime + (m_MonthStart.AddMonths(1) - m_MonthStart).Hours + (m_MonthStart.AddMonths(1) - m_MonthStart).Days * 24;
                            if (m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() == "EnvironmentTime")
                            {
                                m_EnvironmentTime = m_EnvironmentTime + decimal.Parse(m_HaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                            else if (m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "ProcessDowntime" ||
                                     m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "MechanicalDowntime" ||
                                     m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "ElectricalDowntime")
                            {
                                m_DownTime = m_DownTime + decimal.Parse(m_HaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                            else if (m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "RepairTime")
                            {
                                m_RepairTime = m_RepairTime + decimal.Parse(m_HaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                            else if (m_HaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "NormalStopTime")
                            {
                                m_NormalStopTime = m_NormalStopTime + decimal.Parse(m_HaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                        }
                        if (i == m_HaltLogDataTable.Rows.Count - 1)
                        {
                            if (myProductionQuotasId.Contains("运转率"))
                            {
                                m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetRunningRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                            }
                            else if (myProductionQuotasId.Contains("故障率"))
                            {
                                m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetFailureRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                            }
                            else if (myProductionQuotasId.Contains("可靠性"))
                            {
                                m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetReliability(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                            }
                            else if (myProductionQuotasId.Contains("运转时间"))
                            {
                                m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = GetRunTime(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                            }
                            else if (myProductionQuotasId.Contains("计划检修时间"))
                            {
                                m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_ColumnName] = m_RepairTime;
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
        }
        public static DataTable GetEquipmentHaltLogDataMPerMonth(string myEquipmentCommonIdList, string myOrganizationId, string myStartTime, string myEndTime, ISqlServerDataFactory myDataFactory)
        {
            string[] m_EquipmentCommonIdList = myEquipmentCommonIdList.Split(',');
            string m_Condition = "";
            for (int i = 0; i < m_EquipmentCommonIdList.Length; i++)
            {
                if (i == 0)
                {
                    m_Condition = "'" + m_EquipmentCommonIdList[i] + "'";
                }
                else
                {
                    m_Condition = m_Condition + ",'" + m_EquipmentCommonIdList[i] + "'";
                }
            }
            string m_Sql = @"Select P.OrganizationID, P.EquipmentId, P.EquipmentCommonId, P.EquipmentCommonName, P.PreMonth as HaltTimeMonthF, (case when Q.ReasonStatisticsTypeId is null then '' else Q.ReasonStatisticsTypeId end) as ReasonStatisticsTypeId, 
                                sum(case when P.HaltLong is null then 0 else P.HaltLong end) as Value  from 
	                            (Select N.MachineHaltLogID,
		                            M.OrganizationID,
		                            M.EquipmentId,
                                    M.EquipmentCommonId, 
                                    M.EquipmentCommonName,
		                            N.HaltTime,
		                            N.RecoverTime,
		                            M.PreMonth,
		                            N.ReasonID,
		                            (case when convert(varchar(7),N.HaltTime,20) < M.PreMonth then  CONVERT(datetime,M.PreMonth + '-01 00:00:00') else N.HaltTime end) as HaltTimeF
											                              ,(case when convert(varchar(7),N.RecoverTime,20) > M.PreMonth then dateadd(day,-1, dateadd(month, 1, CONVERT(datetime,M.PreMonth + '-01 23:59:59'))) else N.RecoverTime end) as RecoverTimeF
											                              ,convert(decimal(18,4), DATEDIFF (second, (case when convert(varchar(7),N.HaltTime,20) < M.PreMonth then  CONVERT(datetime,M.PreMonth + '-01 00:00:00') else N.HaltTime end)
											                              ,(case when convert(varchar(7),N.RecoverTime,20) > M.PreMonth then dateadd(day,-1, dateadd(month, 1, CONVERT(datetime,M.PreMonth + '-01 23:59:59'))) else N.RecoverTime end)) / 3600.00) as HaltLong
		                             from
		                            (Select A.EquipmentId, B.PreMonth as PreMonth, F.OrganizationID, A.EquipmentCommonId, C.Name as EquipmentCommonName from 
		                                equipment_EquipmentDetail A,
			                            (select convert(varchar(7),dateadd(month,number,'{0}'),120) as PreMonth
													                            from master..spt_values
													                            where type='P'
													                            and dateadd(month,number,'{0}')<='{1}') B, equipment_EquipmentCommonInfo C
			                            ,system_Organization E, system_Organization F
			                            where F.OrganizationID = '{2}'
                                        and E.LevelCode like F.LevelCode + '%'
                                        and E.LevelType = 'Factory'
                                        and A.OrganizationID = E.OrganizationID
			                            and A.EquipmentCommonId in ({3})
                                        and A.EquipmentCommonId = C.EquipmentCommonId) M
	                                left join 
		                                (Select D.MachineHaltLogID, D.EquipmentId, D.HaltTime, D.RecoverTime, D.ReasonID from shift_MachineHaltLog D
			                                where ((D.HaltTime >= '{0} 00:00:00'
			                                    and D.HaltTime <= '{1} 23:59:59')
                                            or (D.RecoverTime >= '{0} 00:00:00'
			                                    and D.RecoverTime <= '{1} 23:59:59'))
                                    ) N on M.EquipmentId = N.EquipmentId and convert(varchar(7),N.HaltTime,20) <= M.PreMonth and convert(varchar(7),N.RecoverTime,20) >= M.PreMonth) P
                               left join system_MachineHaltReason Q on P.ReasonID = Q.MachineHaltReasonID
                            group by P.OrganizationID, P.EquipmentId, P.EquipmentCommonId, P.EquipmentCommonName, P.PreMonth, Q.ReasonStatisticsTypeId
                            order by  P.OrganizationID, P.EquipmentCommonId, P.PreMonth, P.EquipmentId, Q.ReasonStatisticsTypeId";

            m_Sql = string.Format(m_Sql, myStartTime, myEndTime, myOrganizationId, m_Condition);
            try
            {
                DataTable m_Result = myDataFactory.Query(m_Sql);
                return m_Result;
            }
            catch
            {
                return null;
            }
        }
        public static DataTable GetEquipmentCommonHourCapacityPerMonth(string myProductionQuotasId, string myOrganizationId, string myEquipmentCommonIdList, string myStartTime, string myEndTime, ISqlServerDataFactory myDataFactory)
        {
            DataTable m_EquipmentUtilizationTable = GetEquipmentCommonUtilizationPerMonth(RunTimeQuotasId, myEquipmentCommonIdList, myOrganizationId, myStartTime, myEndTime, myDataFactory);
            DataTable m_WeightTable = RunIndicators.MaterialWeightResult.GetMaterialWeightResultPerMonthByEquipmentCommon(myOrganizationId, myEquipmentCommonIdList, myStartTime, myEndTime, myDataFactory);
            if (m_WeightTable != null && m_EquipmentUtilizationTable != null)
            {
                bool m_ContainRunTimeRow = false;
                for (int i = 0; i < m_WeightTable.Rows.Count; i++)
                {
                    m_ContainRunTimeRow = false;
                    for (int j = 0; j < m_EquipmentUtilizationTable.Rows.Count; j++)
                    {
                        if (m_WeightTable.Rows[i]["EquipmentId"].ToString() == m_EquipmentUtilizationTable.Rows[i]["EquipmentId"].ToString())
                        {
                            for (int w = 1; w <= 12; w++)
                            {
                                decimal m_m_RunTimeTemp = (decimal)m_EquipmentUtilizationTable.Rows[i][w];
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
        #endregion

        #region 某个时间范围内汇总查询
        public static decimal GetEquipmentUtilization(string myProductionQuotasId, string myEquipmentId, string myOrganizationId, string myStartTime, string myEndTime, ISqlServerDataFactory myDataFactory)
        {
            if (myProductionQuotasId == "台时产量")
            {
                decimal m_ResultDataValue = GetEquipmentHourCapacity(myOrganizationId, myEquipmentId, myStartTime, myEndTime, myDataFactory);
                return m_ResultDataValue;
            }
            else
            {
                DataTable m_EquipmentHaltLogDataTable = GetEquipmentHaltLogData(myEquipmentId, myOrganizationId, myStartTime, myEndTime, myDataFactory);
                if (m_EquipmentHaltLogDataTable != null && m_EquipmentHaltLogDataTable.Rows.Count > 0)
                    {
                        DateTime m_StartTime = DateTime.Parse(myStartTime);
                        DateTime m_EndTime = DateTime.Parse(myEndTime);
                        decimal m_EnvironmentTime = 0.0m;   //正常停机(小时)
                        decimal m_DownTime = 0.0m;         //故障停机时间(小时)
                        decimal m_RepairTime = 0.0m;
                        decimal m_CalendarTime = 0.0m;
                        decimal m_NormalStopTime = 0.0m;
                        for (int i = 0; i < m_EquipmentHaltLogDataTable.Rows.Count; i++)
                        {
                            m_CalendarTime = m_CalendarTime + (m_EndTime - m_StartTime).Hours + (m_EndTime - m_StartTime).Days * 24;
                            if (m_EquipmentHaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() == "EnvironmentTime")
                            {
                                m_EnvironmentTime = m_EnvironmentTime + decimal.Parse(m_EquipmentHaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                            else if (m_EquipmentHaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "ProcessDowntime" ||
                                     m_EquipmentHaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "MechanicalDowntime" ||
                                     m_EquipmentHaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "ElectricalDowntime")
                            {
                                m_DownTime = m_DownTime + decimal.Parse(m_EquipmentHaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                            else if (m_EquipmentHaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "RepairTime")
                            {
                                m_RepairTime = m_RepairTime + decimal.Parse(m_EquipmentHaltLogDataTable.Rows[i]["Value"].ToString());
                            }
                            else if (m_EquipmentHaltLogDataTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "NormalStopTime")
                            {
                                m_NormalStopTime = m_NormalStopTime + decimal.Parse(m_EquipmentHaltLogDataTable.Rows[i]["Value"].ToString());
                            }

                        }
                        if (myProductionQuotasId.Contains("运转率"))
                        {
                            return GetRunningRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                        }
                        else if (myProductionQuotasId.Contains("故障率"))
                        {
                            return GetFailureRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                        }
                        else if (myProductionQuotasId.Contains("可靠性"))
                        {
                            return GetReliability(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                        }
                        else if (myProductionQuotasId.Contains("运转时间"))
                        {
                            return GetRunTime(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                        }
                        else if (myProductionQuotasId.Contains("计划检修时间"))
                        {
                            return m_RepairTime;
                        }
                        else
                        {
                            return 0.0m;
                        }
                    }
                    else
                    {
                        return 0.0m;
                    }
               
            }
        }
        public static decimal GetEquipmentUtilization(string myProductionQuotasId, string myEquipmentId, string myOrganizationId, string myStartTime, string myEndTime, DataTable myHaltLogTable, ISqlServerDataFactory myDataFactory)
        {
            if (myProductionQuotasId == "台时产量")
            {
                decimal m_ResultDataValue = GetEquipmentHourCapacity(myOrganizationId, myEquipmentId, myStartTime, myEndTime, myHaltLogTable, myDataFactory);
                return m_ResultDataValue;
            }
            else
            {
                if (myHaltLogTable != null && myHaltLogTable.Rows.Count > 0)
                {
                    DateTime m_StartTime = DateTime.Parse(myStartTime);
                    DateTime m_EndTime = DateTime.Parse(myEndTime);
                    decimal m_EnvironmentTime = 0.0m;   //正常停机(小时)
                    decimal m_DownTime = 0.0m;         //故障停机时间(小时)
                    decimal m_RepairTime = 0.0m;
                    decimal m_CalendarTime = 0.0m;
                    decimal m_NormalStopTime = 0.0m;
                    for (int i = 0; i < myHaltLogTable.Rows.Count; i++)
                    {
                        m_CalendarTime = m_CalendarTime + (m_EndTime - m_StartTime).Hours + (m_EndTime - m_StartTime).Days * 24;
                        if (myHaltLogTable.Rows[i]["ReasonStatisticsTypeId"].ToString() == "EnvironmentTime")
                        {
                            m_EnvironmentTime = m_EnvironmentTime + decimal.Parse(myHaltLogTable.Rows[i]["Value"].ToString());
                        }
                        else if (myHaltLogTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "ProcessDowntime" ||
                                 myHaltLogTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "MechanicalDowntime" ||
                                 myHaltLogTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "ElectricalDowntime")
                        {
                            m_DownTime = m_DownTime + decimal.Parse(myHaltLogTable.Rows[i]["Value"].ToString());
                        }
                        else if (myHaltLogTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "RepairTime")
                        {
                            m_RepairTime = m_RepairTime + decimal.Parse(myHaltLogTable.Rows[i]["Value"].ToString());
                        }
                        else if (myHaltLogTable.Rows[i]["ReasonStatisticsTypeId"].ToString() != "NormalStopTime")
                        {
                            m_NormalStopTime = m_NormalStopTime + decimal.Parse(myHaltLogTable.Rows[i]["Value"].ToString());
                        }

                    }
                    if (myProductionQuotasId.Contains("运转率"))
                    {
                        return GetRunningRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                    }
                    else if (myProductionQuotasId.Contains("故障率"))
                    {
                        return GetFailureRate(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                    }
                    else if (myProductionQuotasId.Contains("可靠性"))
                    {
                        return GetReliability(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                    }
                    else if (myProductionQuotasId.Contains("运转时间"))
                    {
                        return GetRunTime(m_CalendarTime, m_RepairTime, m_EnvironmentTime, m_NormalStopTime, m_DownTime);
                    }
                    else if (myProductionQuotasId.Contains("计划检修时间"))
                    {
                        return m_RepairTime;
                    }
                    else
                    {
                        return 0.0m;
                    }
                }
                else
                {
                    return 0.0m;
                }

            }
        }
        public static DataTable GetEquipmentHaltLogData(string myEquipmentId, string myOrganizationId, string myStartTime, string myEndTime, ISqlServerDataFactory myDataFactory)
        {
            string m_Sql = @"Select M.OrganizationID, M.EquipmentID as EquipmentId, M.ReasonStatisticsTypeId as ReasonStatisticsTypeId,  sum(case when M.HaltLongF is null then 0 else M.HaltLongF end) as Value
                                from (SELECT A.MachineHaltLogID
                                      ,B.OrganizationID
                                      ,A.EquipmentID
	                                  ,A.HaltTime
	                                  ,A.RecoverTime
	                                  ,(case when D.ReasonStatisticsTypeId is null then '' else D.ReasonStatisticsTypeId end) as ReasonStatisticsTypeId
	                                  ,A.HaltTime as HaltTimeF
	                                  ,(case when A.RecoverTime is not null and A.RecoverTime < '{1} 23:59:59' then A.RecoverTime else '{1} 23:59:59' end) as RecoverTimeF
	                                  ,convert(decimal(18,4), DATEDIFF (second, A.HaltTime, (case when A.RecoverTime is not null and A.RecoverTime < '{1} 23:59:59' then A.RecoverTime else '{1} 23:59:59' end)) / 3600.00) as HaltLongF
                                  FROM shift_MachineHaltLog A
                                  left join system_MachineHaltReason D on A.ReasonID = D.MachineHaltReasonID, 
                                  system_Organization B, system_Organization C
                                  where ((A.HaltTime >= '{0} 00:00:00'
			                                    and A.HaltTime <= '{1} 23:59:59')
                                            or (A.RecoverTime >= '{0} 00:00:00'
			                                    and A.RecoverTime <= '{1} 23:59:59'))
                                  and B.OrganizationID = '{2}'
                                  and C.LevelCode like B.LevelCode + '%'
                                  and A.OrganizationID = C.OrganizationID
                                  and convert(varchar(64),A.EquipmentID) = '{3}') M
                                  group by M.OrganizationID, M.EquipmentID, M.ReasonStatisticsTypeId
                                  order by M.OrganizationID, M.EquipmentID, M.ReasonStatisticsTypeId";
            try
            {
                m_Sql = string.Format(m_Sql, myStartTime, myEndTime, myOrganizationId, myEquipmentId);
                DataTable m_Result = myDataFactory.Query(m_Sql);
                return m_Result;
            }
            catch (Exception e)
            {
                return null;
            }
        }
        public static decimal GetEquipmentHourCapacity(string myOrganizationId, string myEquipmentId, string myStartTime, string myEndTime, ISqlServerDataFactory myDataFactory)
        {
            decimal m_RuntimeTemp = GetEquipmentUtilization(RunTimeQuotasId, myEquipmentId, myOrganizationId, myStartTime, myEndTime, myDataFactory);
            decimal m_EquipmentMaterialWeight = MaterialWeightResult.GetMaterialWeightResultByEquipment(myOrganizationId, myEquipmentId, myStartTime, myEndTime, myDataFactory);
            if (m_RuntimeTemp > 0)
            {
                return m_EquipmentMaterialWeight / m_RuntimeTemp;
            }
            else
            {
                return 0.0m;
            }
        }
        public static decimal GetEquipmentHourCapacity(string myOrganizationId, string myEquipmentId, string myStartTime, string myEndTime, DataTable myHaltLogTable, ISqlServerDataFactory myDataFactory)
        {
            decimal m_RuntimeTemp = GetEquipmentUtilization(RunTimeQuotasId, myEquipmentId, myOrganizationId, myStartTime, myEndTime, myHaltLogTable, myDataFactory);
            decimal m_EquipmentMaterialWeight = MaterialWeightResult.GetMaterialWeightResultByEquipment(myOrganizationId, myEquipmentId, myStartTime, myEndTime, myDataFactory);
            if (m_RuntimeTemp > 0)
            {
                return m_EquipmentMaterialWeight / m_RuntimeTemp;
            }
            else
            {
                return 0.0m;
            }
        }

        public static DataTable GetEquipmentUtilizationByCommonId(string[] myProductionQuotasIdList, string myEquipmentCommonId, string myOrganizationId, string myStartTime, string myEndTime, ISqlServerDataFactory myDataFactory)
        {
            DataTable m_EquipmentUtilizationResultTable = new DataTable();
            m_EquipmentUtilizationResultTable.Columns.Add("EquipmentId", typeof(string));
            m_EquipmentUtilizationResultTable.Columns.Add("EquipmentName", typeof(string));
            for (int i = 0; i < myProductionQuotasIdList.Length; i++)
            {
                m_EquipmentUtilizationResultTable.Columns.Add(myProductionQuotasIdList[i], typeof(decimal));
            }
            DataTable m_EquipmentHaltLogTable = GetEquipmentHaltLogByCommonId(myEquipmentCommonId, myOrganizationId, myStartTime, myEndTime, myDataFactory);
            if (myProductionQuotasIdList != null && m_EquipmentHaltLogTable != null)
            {
                for (int i = 0; i < m_EquipmentHaltLogTable.Rows.Count; i++)
                {
                    DataRow m_NewRowTemp = m_EquipmentUtilizationResultTable.NewRow();
                    m_NewRowTemp["EquipmentId"] = m_EquipmentHaltLogTable.Rows[i]["EquipmentId"];
                    m_NewRowTemp["EquipmentName"] = m_EquipmentHaltLogTable.Rows[i]["EquipmentName"];
                    DataTable m_RowTable = m_EquipmentHaltLogTable.Clone();
                    m_RowTable.Rows.Add(m_EquipmentHaltLogTable.Rows[i].ItemArray);
                    for (int j = 0; j < myProductionQuotasIdList.Length; j++)
                    {
                        decimal m_UtilizationValue = GetEquipmentUtilization(myProductionQuotasIdList[j], m_EquipmentHaltLogTable.Rows[i]["EquipmentId"].ToString(), myOrganizationId, myStartTime, myEndTime, m_RowTable, myDataFactory);
                        m_NewRowTemp[myProductionQuotasIdList[j]] = m_UtilizationValue;
                    }
                    m_EquipmentUtilizationResultTable.Rows.Add(m_NewRowTemp);
                }
            }
            return m_EquipmentUtilizationResultTable;
        }
        public static DataTable GetEquipmentHaltLogByCommonId(string myEquipmentCommonId, string myOrganizationId, string myStartTime, string myEndTime, ISqlServerDataFactory myDataFactory)
        {
            string m_Sql = @"Select  M.OrganizationID, M.EquipmentId, M.EquipmentName, (case when N.ReasonStatisticsTypeId is null then '' else N.ReasonStatisticsTypeId end) as ReasonStatisticsTypeId,  sum(case when N.HaltLongF is null then 0 else N.HaltLongF end) as Value from
                                (Select B.EquipmentCommonId, B.Name, A.EquipmentId, A.EquipmentName, K.OrganizationID from equipment_EquipmentDetail A, equipment_EquipmentCommonInfo B, system_Organization J, system_Organization K 
                                where A.EquipmentCommonId = '{3}'
                                                                  and A.EquipmentCommonId = B.EquipmentCommonId
								                                  and K.OrganizationID = '{2}'
                                                                  and J.LevelCode like K.LevelCode + '%'
                                                                  and J.LevelType = 'Factory'
                                                                  and A.OrganizationID = J.OrganizationID) M
                                    left join 
	                                (Select C.MachineHaltLogID, D.OrganizationID, C.HaltTime, C.RecoverTime, C.EquipmentID
	                                                                  ,F.ReasonStatisticsTypeId
	                                                                  ,C.HaltTime as HaltTimeF
	                                                                  ,(case when C.RecoverTime is not null and C.RecoverTime < '{1} 23:59:59' then C.RecoverTime else '{1} 23:59:59' end) as RecoverTimeF
	                                                                  ,convert(decimal(18,4), DATEDIFF (second, C.HaltTime, (case when C.RecoverTime is not null and C.RecoverTime < '{1} 23:59:59' then C.RecoverTime else '{1} 23:59:59' end)) / 3600.0000) as HaltLongF from shift_MachineHaltLog C
	                                    left join system_MachineHaltReason F on C.ReasonID = F.MachineHaltReasonID, 
		                                 system_Organization D, system_Organization E
                                     where ((C.HaltTime >= '{0} 00:00:00'
			                                    and C.HaltTime <= '{1} 23:59:59')
                                            or (C.RecoverTime >= '{0} 00:00:00'
			                                    and C.RecoverTime <= '{1} 23:59:59'))
                                    and D.OrganizationID = '{2}'
                                    and E.LevelCode like D.LevelCode + '%'
                                    and C.OrganizationID = E.OrganizationID) N on M.EquipmentID = N.EquipmentID and M.OrganizationID = N.OrganizationID
								    group by M.OrganizationID, M.EquipmentId, M.EquipmentName, N.ReasonStatisticsTypeId
                                    order by M.OrganizationID, M.EquipmentId, N.ReasonStatisticsTypeId";
            try
            {
                m_Sql = string.Format(m_Sql, myStartTime, myEndTime, myOrganizationId, myEquipmentCommonId);
                DataTable m_Result = myDataFactory.Query(m_Sql);
                return m_Result;
            }
            catch (Exception e)
            {
                return null;
            }
        }
        
        
        public static DataTable GetEquipmentCommonUtilization(string[] myProductionQuotasIdList, string[] myEquipmentCommonIdList, string myOrganizationId, string myStartTime, string myEndTime, ISqlServerDataFactory myDataFactory)
        {
            string m_Condition = "";
            for (int i = 0; i < myEquipmentCommonIdList.Length; i++)
            {
                if (i == 0)
                {
                    m_Condition = "'" + myEquipmentCommonIdList[i] + "'";
                }
                else
                {
                    m_Condition = m_Condition + ",'" + myEquipmentCommonIdList[i] + "'";
                }
            }
            DataTable m_EquipmentCommonUtilizationResultTable = new DataTable();
            m_EquipmentCommonUtilizationResultTable.Columns.Add("EquipmentCommonId", typeof(string));
            m_EquipmentCommonUtilizationResultTable.Columns.Add("Name", typeof(string));
            for (int i = 0; i < myProductionQuotasIdList.Length; i++)
            {
                m_EquipmentCommonUtilizationResultTable.Columns.Add(myProductionQuotasIdList[i], typeof(decimal));
            }
            DataTable m_EquipmentCommonHaltLogTable = GetEquipmentCommonHaltLogData(myEquipmentCommonIdList, myOrganizationId, myStartTime, myEndTime, myDataFactory);
            if (myProductionQuotasIdList != null && m_EquipmentCommonHaltLogTable != null)
            {
                for (int i = 0; i < myEquipmentCommonIdList.Length; i++)
                {
                    DataRow[] m_EquipmentDataRows = m_EquipmentCommonHaltLogTable.Select(string.Format("EquipmentCommonId = '{0}'", myEquipmentCommonIdList[i]));
                    if (m_EquipmentDataRows != null && m_EquipmentDataRows.Length > 0)
                    {
                        DataRow m_NewRowTemp = m_EquipmentCommonUtilizationResultTable.NewRow();
                        m_NewRowTemp["EquipmentCommonId"] = myEquipmentCommonIdList[i];
                        m_NewRowTemp["Name"] = m_EquipmentDataRows[0]["Name"];
                        for (int j = 0; j < myProductionQuotasIdList.Length; j++)
                        {
                            decimal m_UtilizationValue = GetEquipmentUtilization(myProductionQuotasIdList[j], m_EquipmentCommonHaltLogTable.Rows[i]["EquipmentCommonId"].ToString(), myOrganizationId, myStartTime, myEndTime, m_EquipmentDataRows.CopyToDataTable(), myDataFactory);
                            m_NewRowTemp[myProductionQuotasIdList[j]] = m_UtilizationValue;
                        }
                        m_EquipmentCommonUtilizationResultTable.Rows.Add(m_NewRowTemp);
                    }
                }
            }
            return m_EquipmentCommonUtilizationResultTable;
        }
        public static DataTable GetEquipmentCommonHaltLogData(string[] myEquipmentCommonIdList, string myOrganizationId, string myStartTime, string myEndTime, ISqlServerDataFactory myDataFactory)
        {
            string m_Condition = "";
            for (int i = 0; i < myEquipmentCommonIdList.Length; i++)
            {
                if (i == 0)
                {
                    m_Condition = "'" + myEquipmentCommonIdList[i] + "'";
                }
                else
                {
                    m_Condition = m_Condition + ",'" + myEquipmentCommonIdList[i] + "'";
                }
            }
            string m_Sql = @"Select  M.OrganizationID, M.EquipmentId, M.EquipmentCommonId as EquipmentCommonId, M.Name, (case when N.ReasonStatisticsTypeId is null then '' else N.ReasonStatisticsTypeId end) as ReasonStatisticsTypeId,  sum(case when N.HaltLongF is null then 0 else N.HaltLongF end) as Value from
                                (Select B.EquipmentCommonId, B.Name, A.EquipmentId, K.OrganizationID from equipment_EquipmentDetail A, equipment_EquipmentCommonInfo B, system_Organization J, system_Organization K 
                                where A.EquipmentCommonId in ({3})
                                                                  and A.EquipmentCommonId = B.EquipmentCommonId
								                                  and K.OrganizationID = '{2}'
                                                                  and J.LevelCode like K.LevelCode + '%'
                                                                  and J.LevelType = 'Factory'
                                                                  and A.OrganizationID = J.OrganizationID) M
                                    left join 
	                                (Select C.MachineHaltLogID, D.OrganizationID, C.HaltTime, C.RecoverTime, C.EquipmentID
	                                                                  ,F.ReasonStatisticsTypeId
	                                                                  ,C.HaltTime as HaltTimeF
	                                                                  ,(case when C.RecoverTime is not null and C.RecoverTime < '{1} 23:59:59' then C.RecoverTime else '{1} 23:59:59' end) as RecoverTimeF
	                                                                  ,convert(decimal(18,4), DATEDIFF (second, C.HaltTime, (case when C.RecoverTime is not null and C.RecoverTime < '{1} 23:59:59' then C.RecoverTime else '{1} 23:59:59' end)) / 3600.0000) as HaltLongF from shift_MachineHaltLog C
	                                    left join system_MachineHaltReason F on C.ReasonID = F.MachineHaltReasonID, 
		                                 system_Organization D, system_Organization E
                                     where ((C.HaltTime >= '{0} 00:00:00'
			                                    and C.HaltTime <= '{1} 23:59:59')
                                            or (C.RecoverTime >= '{0} 00:00:00'
			                                    and C.RecoverTime <= '{1} 23:59:59'))
                                    and D.OrganizationID = '{2}'
                                    and E.LevelCode like D.LevelCode + '%'
                                    and C.OrganizationID = E.OrganizationID) N on M.EquipmentID = N.EquipmentID and M.OrganizationID = N.OrganizationID
								    group by M.OrganizationID, M.EquipmentCommonId, M.Name, M.EquipmentId, N.ReasonStatisticsTypeId
                                    order by M.OrganizationID, M.EquipmentCommonId, M.EquipmentId, N.ReasonStatisticsTypeId";
            try
            {
                m_Sql = string.Format(m_Sql, myStartTime, myEndTime, myOrganizationId, m_Condition);
                DataTable m_Result = myDataFactory.Query(m_Sql);
                return m_Result;
            }
            catch (Exception e)
            {
                return null;
            }
        }
        #endregion
    }
}
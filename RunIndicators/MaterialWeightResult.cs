using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using SqlServerDataAdapter;
namespace RunIndicators
{
    public class MaterialWeightResult
    {
        private const string MaterialWeight = "MaterialWeight";
        private const string EquipmentUtilization = "EquipmentUtilization";
        public static DataTable GetMaterialWeightResultPerMonth(string myProductionQuotasId, string myOrganizationId, string myPlanYear, string myEquipmentCommonId, ISqlServerDataFactory myDataFactory)
        {
            string m_MonthStartTime = myPlanYear + "-01";
            string m_MonthEndTime;
            string m_DayStartTime;
            string m_DayEndTime;
            if (Int32.Parse(myPlanYear) < DateTime.Now.Year)     //如果选取的时间小于今年           
            {
                m_MonthEndTime =  myPlanYear + "-12";
                m_DayStartTime = DateTime.Parse(myPlanYear + "-12-31").AddDays(-2).ToString("yyyy-MM-dd");          //设置开始时间晚于结束时间,使day汇总没有数据
                m_DayEndTime = myPlanYear + "-12-31"; 
            }
            else
            {
                m_MonthEndTime = DateTime.Now.AddMonths(-1).ToString("yyyy-MM");           //如果当前就是1月份的话,按month查的汇总就没有数据
                m_DayStartTime = DateTime.Now.ToString("yyyy-MM-01");
                m_DayEndTime = DateTime.Now.ToString("yyyy-MM-dd");
            }
            string m_Sql = @"Select M.VariableName,M.EquipmentId, M.QuotasID, M.TimeStamp, M.Value + N.Value as Value from
                             (Select B.EquipmentName + A.QuotasName as VariableName, B.EquipmentId, A.QuotasID, C.TimeStamp, sum(D.TotalPeakValleyFlatB) as Value
                                from plan_ProductionPlan_Template A, equipment_EquipmentDetail B, tz_Balance C, balance_Production D, system_Organization E, system_Organization F
                                where F.OrganizationID = @OrganizationId
                                and E.LevelCode like F.LevelCode + '%'
                                and E.LevelType = 'Factory'
                                and B.OrganizationID = E.OrganizationID
                                and B.EquipmentCommonId = @EquipmentCommonId
                                and A.EquipmentCommonId = B.EquipmentCommonId
                                and A.QuotasID = @QuotasID
                                and C.StaticsCycle = 'month'
                                and C.TimeStamp >= @MonthStartTime
                                and C.TimeStamp <= @MonthEndTime
                                and B.OrganizationID = C.OrganizationID
                                and C.BalanceId = D.KeyId
                                and D.VariableId = B.EquipmentId
                                and B.ValueType = 'MaterialWeight'
                                group by F.OrganizationID, B.EquipmentId, A.QuotasID, B.EquipmentName + A.QuotasName, C.TimeStamp) M,
                             (Select B.EquipmentName + A.QuotasName as VariableName, B.EquipmentId, A.QuotasID, CONVERT(varchar(7), C.TimeStamp, 20) as TimeStamp, sum(D.TotalPeakValleyFlatB) as Value
                                from plan_ProductionPlan_Template A, equipment_EquipmentDetail B, tz_Balance C, balance_Production D, system_Organization E, system_Organization F
                                where F.OrganizationID = @OrganizationId
                                and E.LevelCode like F.LevelCode + '%'
                                and E.LevelType = 'Factory'
                                and B.OrganizationID = E.OrganizationID
                                and B.EquipmentCommonId = @EquipmentCommonId
                                and A.EquipmentCommonId = B.EquipmentCommonId
                                and A.QuotasID = @QuotasID
                                and C.StaticsCycle = 'day'
                                and C.TimeStamp >= @DayStartTime
                                and C.TimeStamp <= @DayEndTime
                                and B.OrganizationID = C.OrganizationID
                                and C.BalanceId = D.KeyId
                                and D.VariableId = B.EquipmentId
                                and B.ValueType = 'MaterialWeight'
                                group by F.OrganizationID, B.EquipmentId, A.QuotasID, B.EquipmentName + A.QuotasName, CONVERT(varchar(7), C.TimeStamp, 20)) N
                                where M.OrganizationID = N.OrganizationID and M.EquipmentId = N.EquipmentId and M.QuotasID = N.QuotasID and M.TimeStamp = N.TimeStamp
                                order by M.EquipmentId, M.QuotasID, M.TimeStamp";
            try
            {
                SqlParameter[] m_Parameters = { new SqlParameter("@OrganizationID", myOrganizationId)
                                              , new SqlParameter("@EquipmentCommonId", myEquipmentCommonId)
                                              , new SqlParameter("@QuotasID", myProductionQuotasId)
                                              , new SqlParameter("@MonthStartTime", m_MonthStartTime)
                                              , new SqlParameter("@MonthEndTime", m_MonthEndTime)
                                              , new SqlParameter("@DayStartTime", m_DayStartTime)
                                              , new SqlParameter("@DayEndTime", m_DayEndTime)};
                DataTable m_Result = myDataFactory.Query(m_Sql, m_Parameters);
                if (m_Result != null)
                {
                    DataTable m_ResultDataTable = EquipmentRunIndicators.GetResultDataTable();
                    string m_EquipmentId = "";
                    for (int i = 0; i < m_Result.Rows.Count; i++)
                    {
                        if (m_EquipmentId != m_Result.Rows[i]["EquipmentId"].ToString())
                        {
                            DataRow m_NewRow = m_ResultDataTable.NewRow();
                            m_NewRow[0] = m_EquipmentId;
                            int m_MonthIndex = DateTime.Parse(m_Result.Rows[i]["TimeStamp"].ToString() + "-01").Month;
                            m_NewRow[m_MonthIndex] = decimal.Parse(m_Result.Rows[i]["Value"].ToString());
                            m_Result.Rows.Add(m_NewRow);
                        }
                        else
                        {
                            int m_MonthIndex = DateTime.Parse(m_Result.Rows[i]["TimeStamp"].ToString() + "-01").Month;
                            m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][m_MonthIndex] = decimal.Parse(m_Result.Rows[i]["Value"].ToString());
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
        public static DataTable GetMaterialWeightResultPerMonthS(string myOrganizationId, string myEquipmentId, string myStartTime, string myEndTime,ISqlServerDataFactory myDataFactory)
        {
            string m_MonthStartTime = myStartTime;
            string m_MonthEndTime;
            string m_DayStartTime;
            string m_DayEndTime;
            if (DateTime.Parse(myEndTime+ "-01") < DateTime.Now)     //如果选取的时间小于这个月         
            {
                m_MonthEndTime = myEndTime;
                m_DayStartTime = "2016-01-31";          //设置开始时间晚于结束时间,使day汇总没有数据
                m_DayEndTime = "2016-01-20";
            }
            else
            {
                m_MonthEndTime = DateTime.Now.AddMonths(-1).ToString("yyyy-MM");           //如果当前就是1月份的话,按month查的汇总就没有数据
                m_DayStartTime = DateTime.Now.ToString("yyyy-MM-01");
                m_DayEndTime = DateTime.Now.ToString("yyyy-MM-dd");
            }
            string m_Sql = @"Select M.VariableName,M.EquipmentId, M.TimeStamp, M.Value + N.Value as Value from
                             (Select B.EquipmentName as VariableName, B.EquipmentId, C.TimeStamp, sum(D.TotalPeakValleyFlatB) as Value
                                from equipment_EquipmentDetail B, tz_Balance C, balance_Production D, system_Organization E, system_Organization F
                                where F.OrganizationID = @OrganizationId
                                and E.LevelCode like F.LevelCode + '%'
                                and E.LevelType = 'Factory'
                                and B.OrganizationID = E.OrganizationID
                                and B.EquipmentId = @EquipmentId
                                and C.StaticsCycle = 'month'
                                and C.TimeStamp >= @MonthStartTime
                                and C.TimeStamp <= @MonthEndTime
                                and B.OrganizationID = C.OrganizationID
                                and C.BalanceId = D.KeyId
                                and D.VariableId = B.EquipmentId
                                and D.ValueType = 'MaterialWeight'
                                group by F.OrganizationID, B.EquipmentId, B.EquipmentName, C.TimeStamp) M,
                             (Select B.EquipmentName as VariableName, B.EquipmentId, CONVERT(varchar(7), C.TimeStamp, 20) as TimeStamp, sum(D.TotalPeakValleyFlatB) as Value
                                from equipment_EquipmentDetail B, tz_Balance C, balance_Production D, system_Organization E, system_Organization F
                                where F.OrganizationID = @OrganizationId
                                and E.LevelCode like F.LevelCode + '%'
                                and E.LevelType = 'Factory'
                                and B.OrganizationID = E.OrganizationID
                                and B.EquipmentId = @EquipmentId
                                and C.StaticsCycle = 'day'
                                and C.TimeStamp >= @DayStartTime
                                and C.TimeStamp <= @DayEndTime
                                and B.OrganizationID = C.OrganizationID
                                and C.BalanceId = D.KeyId
                                and D.VariableId = B.EquipmentId
                                and D.ValueType = 'MaterialWeight'
                                group by F.OrganizationID, B.EquipmentId, B.EquipmentName, CONVERT(varchar(7), C.TimeStamp, 20)) N
                                where M.OrganizationID = N.OrganizationID and M.EquipmentId = N.EquipmentId and M.TimeStamp = N.TimeStamp
                                order by M.EquipmentId, M.TimeStamp";
            try
            {
                SqlParameter[] m_Parameters = { new SqlParameter("@OrganizationID", myOrganizationId)
                                              , new SqlParameter("@EquipmentId", myEquipmentId)
                                              , new SqlParameter("@MonthStartTime", m_MonthStartTime)
                                              , new SqlParameter("@MonthEndTime", m_MonthEndTime)
                                              , new SqlParameter("@DayStartTime", m_DayStartTime)
                                              , new SqlParameter("@DayEndTime", m_DayEndTime)};
                DataTable m_Result = myDataFactory.Query(m_Sql, m_Parameters);
                if (m_Result != null)
                {
                    DataTable m_ResultDataTable = EquipmentRunIndicators.GetResultDataTable(myStartTime, myEndTime);
                    string m_EquipmentId = "";
                    for (int i = 0; i < m_Result.Rows.Count; i++)
                    {
                        if (m_EquipmentId != m_Result.Rows[i]["EquipmentId"].ToString())
                        {
                            DataRow m_NewRow = m_ResultDataTable.NewRow();
                            m_NewRow[0] = m_EquipmentId;
                            m_NewRow[DateTime.Parse(m_Result.Rows[i]["TimeStamp"].ToString() + "-01").ToString("yyyy-MM")] = decimal.Parse(m_Result.Rows[i]["Value"].ToString());
                            m_Result.Rows.Add(m_NewRow);
                        }
                        else
                        {
                            m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][DateTime.Parse(m_Result.Rows[i]["TimeStamp"].ToString() + "-01").ToString("yyyy-MM")] = decimal.Parse(m_Result.Rows[i]["Value"].ToString());
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
        public static DataTable GetMaterialWeightResultPerMonthByEquipmentCommon(string myOrganizationId, string myEquipmentCommonIdList, string myStartTime, string myEndTime, ISqlServerDataFactory myDataFactory)
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
            string m_MonthStartTime = myStartTime;
            string m_MonthEndTime;
            string m_DayStartTime;
            string m_DayEndTime;
            if (DateTime.Parse(myEndTime + "-01") < DateTime.Now)     //如果选取的时间小于这个月         
            {
                m_MonthEndTime = myEndTime;
                m_DayStartTime = "2016-01-31";          //设置开始时间晚于结束时间,使day汇总没有数据
                m_DayEndTime = "2016-01-20";
            }
            else
            {
                m_MonthEndTime = DateTime.Now.AddMonths(-1).ToString("yyyy-MM");           //如果当前就是1月份的话,按month查的汇总就没有数据
                m_DayStartTime = DateTime.Now.ToString("yyyy-MM-01");
                m_DayEndTime = DateTime.Now.ToString("yyyy-MM-dd");
            }
            string m_Sql = @"Select M.EquipmentCommonId, M.TimeStamp, M.Value + N.Value as Value from
                             (Select B.EquipmentCommonId, C.TimeStamp, sum(D.TotalPeakValleyFlatB) as Value
                                from equipment_EquipmentDetail B, tz_Balance C, balance_Production D, system_Organization E, system_Organization F
                                where F.OrganizationID = @OrganizationId
                                and E.LevelCode like F.LevelCode + '%'
                                and E.LevelType = 'Factory'
                                and B.OrganizationID = E.OrganizationID
                                and B.EquipmentCommonId in (@EquipmentCommonId)
                                and C.StaticsCycle = 'month'
                                and C.TimeStamp >= @MonthStartTime
                                and C.TimeStamp <= @MonthEndTime
                                and B.OrganizationID = C.OrganizationID
                                and C.BalanceId = D.KeyId
                                and D.VariableId = B.EquipmentId
                                and D.ValueType = 'MaterialWeight'
                                group by F.OrganizationID, B.EquipmentCommonId, C.TimeStamp) M,
                             (Select B.EquipmentCommonId, CONVERT(varchar(7), C.TimeStamp, 20) as TimeStamp, sum(D.TotalPeakValleyFlatB) as Value
                                from equipment_EquipmentDetail B, tz_Balance C, balance_Production D, system_Organization E, system_Organization F
                                where F.OrganizationID = @OrganizationId
                                and E.LevelCode like F.LevelCode + '%'
                                and E.LevelType = 'Factory'
                                and B.OrganizationID = E.OrganizationID
                                and B.EquipmentCommonId in (@EquipmentCommonId)
                                and C.StaticsCycle = 'day'
                                and C.TimeStamp >= @DayStartTime
                                and C.TimeStamp <= @DayEndTime
                                and B.OrganizationID = C.OrganizationID
                                and C.BalanceId = D.KeyId
                                and D.VariableId = B.EquipmentId
                                and D.ValueType = 'MaterialWeight'
                                group by F.OrganizationID, B.EquipmentCommonId, CONVERT(varchar(7), C.TimeStamp, 20)) N
                                where M.OrganizationID = N.OrganizationID and M.EquipmentId = N.EquipmentId and M.TimeStamp = N.TimeStamp
                                order by M.EquipmentId, M.TimeStamp";
            try
            {
                SqlParameter[] m_Parameters = { new SqlParameter("@OrganizationID", myOrganizationId)
                                              , new SqlParameter("@EquipmentCommonId", m_Condition)
                                              , new SqlParameter("@MonthStartTime", m_MonthStartTime)
                                              , new SqlParameter("@MonthEndTime", m_MonthEndTime)
                                              , new SqlParameter("@DayStartTime", m_DayStartTime)
                                              , new SqlParameter("@DayEndTime", m_DayEndTime)};
                DataTable m_Result = myDataFactory.Query(m_Sql, m_Parameters);
                if (m_Result != null)
                {
                    DataTable m_ResultDataTable = EquipmentRunIndicators.GetResultDataTable(myStartTime, myEndTime);
                    string m_EquipmentId = "";
                    for (int i = 0; i < m_Result.Rows.Count; i++)
                    {
                        if (m_EquipmentId != m_Result.Rows[i]["EquipmentId"].ToString())
                        {
                            DataRow m_NewRow = m_ResultDataTable.NewRow();
                            m_NewRow[0] = m_EquipmentId;
                            m_NewRow[DateTime.Parse(m_Result.Rows[i]["TimeStamp"].ToString() + "-01").ToString("yyyy-MM")] = decimal.Parse(m_Result.Rows[i]["Value"].ToString());
                            m_Result.Rows.Add(m_NewRow);
                        }
                        else
                        {
                            m_ResultDataTable.Rows[m_ResultDataTable.Rows.Count - 1][DateTime.Parse(m_Result.Rows[i]["TimeStamp"].ToString() + "-01").ToString("yyyy-MM")] = decimal.Parse(m_Result.Rows[i]["Value"].ToString());
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
        public static decimal GetMaterialWeightResultByEquipment(string myOrganizationId, string myEquipmentId, string myStartTime, string myEndTime, ISqlServerDataFactory myDataFactory)
        {
            string m_MonthStartTime = myStartTime;
            string m_MonthEndTime;
            string m_DayStartTime;
            string m_DayEndTime;
            if (DateTime.Parse(myEndTime + "-01") < DateTime.Now)     //如果选取的时间小于这个月         
            {
                m_MonthEndTime = myEndTime;
                m_DayStartTime = "2016-01-31";          //设置开始时间晚于结束时间,使day汇总没有数据
                m_DayEndTime = "2016-01-20";
            }
            else
            {
                m_MonthEndTime = DateTime.Now.AddMonths(-1).ToString("yyyy-MM");           //如果当前就是1月份的话,按month查的汇总就没有数据
                m_DayStartTime = DateTime.Now.ToString("yyyy-MM-01");
                m_DayEndTime = DateTime.Now.ToString("yyyy-MM-dd");
            }
            string m_Sql = @"Select M.EquipmentId, M.TimeStamp, M.Value + N.Value as Value from
                             (Select B.EquipmentId, C.TimeStamp, sum(D.TotalPeakValleyFlatB) as Value
                                from equipment_EquipmentDetail B, tz_Balance C, balance_Production D, system_Organization E, system_Organization F
                                where F.OrganizationID = @OrganizationId
                                and E.LevelCode like F.LevelCode + '%'
                                and E.LevelType = 'Factory'
                                and B.OrganizationID = E.OrganizationID
                                and B.EquipmentId in @EquipmentId
                                and C.StaticsCycle = 'month'
                                and C.TimeStamp >= @MonthStartTime
                                and C.TimeStamp <= @MonthEndTime
                                and B.OrganizationID = C.OrganizationID
                                and C.BalanceId = D.KeyId
                                and D.VariableId = B.EquipmentId
                                and D.ValueType = 'MaterialWeight'
                                group by F.OrganizationID, B.EquipmentId, C.TimeStamp) M,
                             (Select B.EquipmentId, CONVERT(varchar(7), C.TimeStamp, 20) as TimeStamp, sum(D.TotalPeakValleyFlatB) as Value
                                from equipment_EquipmentDetail B, tz_Balance C, balance_Production D, system_Organization E, system_Organization F
                                where F.OrganizationID = @OrganizationId
                                and E.LevelCode like F.LevelCode + '%'
                                and E.LevelType = 'Factory'
                                and B.OrganizationID = E.OrganizationID
                                and B.EquipmentId in @EquipmentId
                                and C.StaticsCycle = 'day'
                                and C.TimeStamp >= @DayStartTime
                                and C.TimeStamp <= @DayEndTime
                                and B.OrganizationID = C.OrganizationID
                                and C.BalanceId = D.KeyId
                                and D.VariableId = B.EquipmentId
                                and D.ValueType = 'MaterialWeight'
                                group by F.OrganizationID, B.EquipmentId, CONVERT(varchar(7), C.TimeStamp, 20)) N
                                where M.OrganizationID = N.OrganizationID and M.EquipmentId = N.EquipmentId and M.TimeStamp = N.TimeStamp
                                order by M.EquipmentId, M.TimeStamp";
            try
            {
                SqlParameter[] m_Parameters = { new SqlParameter("@OrganizationID", myOrganizationId)
                                              , new SqlParameter("@myEquipmentId", myEquipmentId)
                                              , new SqlParameter("@MonthStartTime", m_MonthStartTime)
                                              , new SqlParameter("@MonthEndTime", m_MonthEndTime)
                                              , new SqlParameter("@DayStartTime", m_DayStartTime)
                                              , new SqlParameter("@DayEndTime", m_DayEndTime)};
                DataTable m_Result = myDataFactory.Query(m_Sql, m_Parameters);
                if (m_Result != null && m_Result.Rows.Count > 0)
                {
                    decimal m_MaterialWeight = (decimal)m_Result.Rows[0]["Value"];
                    return m_MaterialWeight;
                }
                else
                {
                    return 0.0m;
                }
            }
            catch (Exception e)
            {
                return 0.0m;
            }
        }
    }
}

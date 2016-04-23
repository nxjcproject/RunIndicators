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
            string m_Sql = @"Select B.EquipmentName + A.QuotasName as VariableName, B.EquipmentId, A.QuotasID, C.TimeStamp, sum(D.TotalPeakValleyFlatB) as Value
                                from plan_ProductionPlan_Template A, equipment_EquipmentDetail B, tz_Balance C, balance_Production D
                                where B.OrganizationID = @OrganizationId
                                and B.EquipmentCommonId = @EquipmentCommonId
                                and A.EquipmentCommonId = B.EquipmentCommonId
                                and A.QuotasID = @QuotasID
                                and C.StaticsCycle = 'month'
                                and C.TimeStamp >= @StartTime
                                and C.TimeStamp <= @EndTime
                                and B.OrganizationID = C.OrganizationID
                                and C.BalanceId = D.KeyId
                                and D.VariableId = B.EquipmentId + A.QuotasID
                                group by B.OrganizationID, B.EquipmentId, A.QuotasID, B.EquipmentName + A.QuotasName, C.TimeStamp
                                order by B.EquipmentId, A.QuotasID, C.TimeStamp";
            try
            {
                SqlParameter[] m_Parameters = { new SqlParameter("@OrganizationID", myOrganizationId)
                                              , new SqlParameter("@EquipmentCommonId", myEquipmentCommonId)
                                              , new SqlParameter("@QuotasID", myProductionQuotasId)
                                              , new SqlParameter("@StartTime", myPlanYear + "-01")
                                              , new SqlParameter("@EndTime", myPlanYear + "-12")};
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
        public static DataTable GetMaterialWeightResultByDenominatorPerMonth(string myProductionQuotasId, string myOrganizationId, string myPlanYear, string myEquipmentCommonId, ISqlServerDataFactory myDataFactory)
        {
            string m_Sql = @"Select B.EquipmentName + A.QuotasName as VariableName, B.EquipmentId, A.QuotasID, C.TimeStamp, sum(D.TotalPeakValleyFlatB) as Value
                                from plan_ProductionPlan_Template A, equipment_EquipmentDetail B, tz_Balance C, balance_Production D, system_MasterMachineDescription E
                                where B.OrganizationID = @OrganizationId
                                and B.EquipmentCommonId = @EquipmentCommonId
                                and A.EquipmentCommonId = B.EquipmentCommonId
                                and C.StaticsCycle = 'month'
                                and C.TimeStamp >= @StartTime
                                and C.TimeStamp <= @EndTime
                                and B.OrganizationID = C.OrganizationID
                                and C.BalanceId = D.KeyId
                                and D.VariableId = B.EquipmentId + A.QuotasID
                                and B.EquipmentId = E.ID
                                group by B.OrganizationID, B.EquipmentId, A.QuotasID, B.EquipmentName + A.QuotasName, C.TimeStamp
                                order by B.EquipmentId, A.QuotasID, C.TimeStamp";
            try
            {
                SqlParameter[] m_Parameters = { new SqlParameter("@OrganizationID", myOrganizationId)
                                              , new SqlParameter("@EquipmentCommonId", myEquipmentCommonId)
                                              , new SqlParameter("@QuotasID", myProductionQuotasId)
                                              , new SqlParameter("@StartTime", myPlanYear + "-01")
                                              , new SqlParameter("@EndTime", myPlanYear + "-12")};
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
    }
}

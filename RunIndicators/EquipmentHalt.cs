using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using SqlServerDataAdapter;
namespace RunIndicators
{
    public class EquipmentHalt
    {
        private const string ProcessDowntime = "ProcessDowntime";
        private const string MechanicalDowntime = "MechanicalDowntime";
        private const string ElectricalDowntime = "ElectricalDowntime";
        private const string EnvironmentDowntime = "EnvironmentDowntime";

        public static DataTable GetEquipmentHaltDetail(string myEquipmentCommonId, string myOrganizationId, string myStatisticalRange, string myStartTime, string myEndTime, ISqlServerDataFactory myDataFactory)
        {
            string m_Sql = @"SELECT A.EquipmentID as EquipmentId, A.EquipmentName, (case when P.ReasonStatisticsTypeId is null then '' else P.ReasonStatisticsTypeId end) as ReasonStatisticsTypeId, 
                              (case when P.DowntimeCount is null then 0 else P.DowntimeCount end) as DowntimeCount, (case when P.DowntimeTime is null then 0 else P.DowntimeTime end) as DowntimeTime
                              FROM equipment_EquipmentDetail A
							  left join 
							  (Select M.OrganizationID, M.EquipmentID, M.ReasonStatisticsTypeId, count(M.OrganizationID) as DowntimeCount, sum(M.HaltLongF) as DowntimeTime from
									(Select C.MachineHaltLogID, D.OrganizationID, C.HaltTime, C.RecoverTime, C.EquipmentID
												,F.ReasonStatisticsTypeId
												,C.HaltTime as HaltTimeF
												,(case when C.RecoverTime is not null and C.RecoverTime < '{1} 23:59:59' then C.RecoverTime else '{1} 23:59:59' end) as RecoverTimeF
												,convert(decimal(18,4), DATEDIFF (second, C.HaltTime, (case when C.RecoverTime is not null and C.RecoverTime < '{1} 23:59:59' then C.RecoverTime else '{1} 23:59:59' end)) / 3600.00) as HaltLongF 
										from shift_MachineHaltLog C
										   left join system_MachineHaltReason F on C.ReasonID = F.MachineHaltReasonID, 
										   system_Organization D, system_Organization E, system_Organization J, system_Organization K
										where ((C.HaltTime >= '{0} 00:00:00' and C.HaltTime <= '{1} 23:59:59')
										   or (C.RecoverTime >= '{0} 00:00:00' and C.RecoverTime <= '{1} 23:59:59'))
                                        and K.OrganizationID = '{2}'
                                        and J.LevelCode like K.LevelCode + '%'
                                        and J.LevelType = 'Factory' 
										and D.OrganizationID = J.OrganizationID
										and E.LevelCode like D.LevelCode + '%'
										and C.OrganizationID = E.OrganizationID) M
									where M.HaltLongF > {4}
									group by M.OrganizationID, M.EquipmentID, M.ReasonStatisticsTypeId) P on P.EquipmentID = A.EquipmentID and P.OrganizationID = A.OrganizationId
							  , equipment_EquipmentCommonInfo B, system_Organization X, system_Organization Y
                              where A.EquipmentCommonId = B.EquipmentCommonId
                              and X.OrganizationId = '{2}'
                              and Y.LevelCode like X.LevelCode + '%'
                              and Y.LevelType = 'Factory'
                              and A.OrganizationId = Y.OrganizationId
                              and B.EquipmentCommonId = '{3}'
                              order by A.DisplayIndex, A.EquipmentName, P.ReasonStatisticsTypeId";
            m_Sql = string.Format(m_Sql, myStartTime, myEndTime, myOrganizationId, myEquipmentCommonId, myStatisticalRange);
            try
            {
                DataTable m_EquipmentHaltDetailTable = myDataFactory.Query(m_Sql);
                DataTable m_EquipmentHaltDetailResultTable = new DataTable();
                m_EquipmentHaltDetailResultTable.Columns.Add("EquipmentId", typeof(string));
                m_EquipmentHaltDetailResultTable.Columns.Add("EquipmentName", typeof(string));
                m_EquipmentHaltDetailResultTable.Columns.Add("DowntimeCount", typeof(int));
                m_EquipmentHaltDetailResultTable.Columns.Add("ProcessDowntimeCount", typeof(int));
                m_EquipmentHaltDetailResultTable.Columns.Add("MechanicalDowntimeCount", typeof(int));
                m_EquipmentHaltDetailResultTable.Columns.Add("ElectricalDowntimeCount", typeof(int));
                m_EquipmentHaltDetailResultTable.Columns.Add("EnvironmentDowntimeCount", typeof(int));
                m_EquipmentHaltDetailResultTable.Columns.Add("DowntimeTime", typeof(decimal));
                m_EquipmentHaltDetailResultTable.Columns.Add("ProcessDowntimeTime", typeof(decimal));
                m_EquipmentHaltDetailResultTable.Columns.Add("MechanicalDowntimeTime", typeof(decimal));
                m_EquipmentHaltDetailResultTable.Columns.Add("ElectricalDowntimeTime", typeof(decimal));
                m_EquipmentHaltDetailResultTable.Columns.Add("EnvironmentDowntimeTime", typeof(decimal));
                if (m_EquipmentHaltDetailTable != null)
                {
                    string m_EquipmentId = "";
                    int m_DowntimeCount = 0;
                    decimal m_DowntimeTime = 0.0m;
                    for (int i = 0; i < m_EquipmentHaltDetailTable.Rows.Count; i++)
                    {
                        if (i == 0)
                        {
                            m_EquipmentId = m_EquipmentHaltDetailTable.Rows[i]["EquipmentId"].ToString();
                            m_EquipmentHaltDetailResultTable.Rows.Add(m_EquipmentHaltDetailResultTable.NewRow());
                            m_EquipmentHaltDetailResultTable.Rows[m_EquipmentHaltDetailResultTable.Rows.Count - 1]["EquipmentId"] = m_EquipmentId;
                            m_EquipmentHaltDetailResultTable.Rows[m_EquipmentHaltDetailResultTable.Rows.Count - 1]["EquipmentName"] = m_EquipmentHaltDetailTable.Rows[i]["EquipmentName"];
                            for (int j = 0; j < 10; j++)
                            {
                                m_EquipmentHaltDetailResultTable.Rows[m_EquipmentHaltDetailResultTable.Rows.Count - 1][j + 2] = 0;
                            }

                            //////////////////////
                            m_DowntimeCount = (int)m_EquipmentHaltDetailTable.Rows[i]["DowntimeCount"];
                            m_DowntimeTime = (decimal)m_EquipmentHaltDetailTable.Rows[i]["DowntimeTime"];
                        }
                        else if (i != 0 && m_EquipmentId != m_EquipmentHaltDetailTable.Rows[i]["EquipmentId"].ToString())
                        {
                            m_EquipmentHaltDetailResultTable.Rows[m_EquipmentHaltDetailResultTable.Rows.Count - 1]["DowntimeCount"] = m_DowntimeCount;
                            m_EquipmentHaltDetailResultTable.Rows[m_EquipmentHaltDetailResultTable.Rows.Count - 1]["DowntimeTime"] = m_DowntimeTime;

                            //////////////////////////////////////
                            m_EquipmentId = m_EquipmentHaltDetailTable.Rows[i]["EquipmentId"].ToString();
                            m_EquipmentHaltDetailResultTable.Rows.Add(m_EquipmentHaltDetailResultTable.NewRow());
                            m_EquipmentHaltDetailResultTable.Rows[m_EquipmentHaltDetailResultTable.Rows.Count - 1]["EquipmentId"] = m_EquipmentId;
                            m_EquipmentHaltDetailResultTable.Rows[m_EquipmentHaltDetailResultTable.Rows.Count - 1]["EquipmentName"] = m_EquipmentHaltDetailTable.Rows[i]["EquipmentName"];
                            for (int j = 0; j < 10; j++)
                            {
                                m_EquipmentHaltDetailResultTable.Rows[m_EquipmentHaltDetailResultTable.Rows.Count - 1][j + 2] = 0;
                            }

                            //////////////////////////////////////
                            m_DowntimeCount = (int)m_EquipmentHaltDetailTable.Rows[i]["DowntimeCount"];
                            m_DowntimeTime = (decimal)m_EquipmentHaltDetailTable.Rows[i]["DowntimeTime"];
                        }
                        else
                        {
                            m_DowntimeCount = m_DowntimeCount + (int)m_EquipmentHaltDetailTable.Rows[i]["DowntimeCount"];
                            m_DowntimeTime = m_DowntimeTime + (decimal)m_EquipmentHaltDetailTable.Rows[i]["DowntimeTime"];
                            if(m_EquipmentHaltDetailTable.Rows[i]["ReasonStatisticsTypeId"].ToString() == ProcessDowntime)
                            {
                                m_EquipmentHaltDetailResultTable.Rows[m_EquipmentHaltDetailResultTable.Rows.Count - 1]["ProcessDowntimeCount"] = m_DowntimeCount;
                                m_EquipmentHaltDetailResultTable.Rows[m_EquipmentHaltDetailResultTable.Rows.Count - 1]["ProcessDowntimeTime"] = m_DowntimeTime;
                            }
                            else if(m_EquipmentHaltDetailTable.Rows[i]["ReasonStatisticsTypeId"].ToString() == MechanicalDowntime)
                            {
                                m_EquipmentHaltDetailResultTable.Rows[m_EquipmentHaltDetailResultTable.Rows.Count - 1]["MechanicalDowntimeCount"] = m_DowntimeCount;
                                m_EquipmentHaltDetailResultTable.Rows[m_EquipmentHaltDetailResultTable.Rows.Count - 1]["MechanicalDowntimeTime"] = m_DowntimeTime;
                            }
                            else if(m_EquipmentHaltDetailTable.Rows[i]["ReasonStatisticsTypeId"].ToString() == ElectricalDowntime)
                            {
                                m_EquipmentHaltDetailResultTable.Rows[m_EquipmentHaltDetailResultTable.Rows.Count - 1]["ElectricalDowntimeCount"] = m_DowntimeCount;
                                m_EquipmentHaltDetailResultTable.Rows[m_EquipmentHaltDetailResultTable.Rows.Count - 1]["ElectricalDowntimeTime"] = m_DowntimeTime;
                            }
                            else if(m_EquipmentHaltDetailTable.Rows[i]["ReasonStatisticsTypeId"].ToString() == EnvironmentDowntime)
                            {
                                m_EquipmentHaltDetailResultTable.Rows[m_EquipmentHaltDetailResultTable.Rows.Count - 1]["EnvironmentDowntimeCount"] = m_DowntimeCount;
                                m_EquipmentHaltDetailResultTable.Rows[m_EquipmentHaltDetailResultTable.Rows.Count - 1]["EnvironmentDowntimeTime"] = m_DowntimeTime;
                            }
                        }
                        if (i == m_EquipmentHaltDetailTable.Rows.Count - 1)
                        {
                            m_EquipmentHaltDetailResultTable.Rows[m_EquipmentHaltDetailResultTable.Rows.Count - 1]["DowntimeCount"] = m_DowntimeCount;
                            m_EquipmentHaltDetailResultTable.Rows[m_EquipmentHaltDetailResultTable.Rows.Count - 1]["DowntimeTime"] = m_DowntimeTime;
                        }
                    }
                }
                return m_EquipmentHaltDetailResultTable;
            }
            catch
            {
                return null;
            }
        }
        public static DataTable GetEquipmentHalt(string[] myEquipmentCommonIdList, string myFactoryOrganizationId, string myStartTime, string myEndTime, string myStatisticalRange, ISqlServerDataFactory myDataFactory)
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
            string m_Sql = @"SELECT B.EquipmentCommonId as EquipmentCommonId, B.Name as EquipmentName,
                              sum((case when P.DowntimeCount is null then 0 else P.DowntimeCount end)) as DowntimeCount, sum((case when P.DowntimeTime is null then 0 else P.DowntimeTime end)) as DowntimeTime
                              FROM equipment_EquipmentDetail A
							  left join 
							  (Select M.OrganizationID, M.EquipmentID, M.ReasonStatisticsTypeId, count(M.OrganizationID) as DowntimeCount, sum(M.HaltLongF) as DowntimeTime from
									(Select C.MachineHaltLogID, D.OrganizationID, C.HaltTime, C.RecoverTime, C.EquipmentID
												,F.ReasonStatisticsTypeId
												,C.HaltTime as HaltTimeF
												,(case when C.RecoverTime is not null and C.RecoverTime < '{1} 23:59:59' then C.RecoverTime else '{1} 23:59:59' end) as RecoverTimeF
												,convert(decimal(18,4), DATEDIFF (second, C.HaltTime, (case when C.RecoverTime is not null and C.RecoverTime < '{1} 23:59:59' then C.RecoverTime else '{1} 23:59:59' end)) / 3600.00) as HaltLongF 
										from shift_MachineHaltLog C
										   left join system_MachineHaltReason F on C.ReasonID = F.MachineHaltReasonID, 
										   system_Organization D, system_Organization E, system_Organization J, system_Organization K
										where ((C.HaltTime >= '{0} 00:00:00' and C.HaltTime <= '{1} 23:59:59')
										   or (C.RecoverTime >= '{0} 00:00:00' and C.RecoverTime <= '{1} 23:59:59'))
										and K.OrganizationID = '{2}'
                                        and J.LevelCode like K.LevelCode + '%'
                                        and J.LevelType = 'Factory' 
										and D.OrganizationID = J.OrganizationID
										and E.LevelCode like D.LevelCode + '%'
										and C.OrganizationID = E.OrganizationID) M
									where M.HaltLongF > {4}
									group by M.OrganizationID, M.EquipmentID, M.ReasonStatisticsTypeId) P on P.EquipmentID = A.EquipmentID and P.OrganizationID = A.OrganizationId
							  , equipment_EquipmentCommonInfo B, system_Organization X, system_Organization Y
                              where A.EquipmentCommonId = B.EquipmentCommonId
                              and X.OrganizationId = '{2}'
                              and Y.LevelCode like X.LevelCode + '%'
                              and Y.LevelType = 'Factory'
                              and A.OrganizationId = Y.OrganizationId
                              and B.EquipmentCommonId in ({3})
                              group by B.EquipmentCommonId,B.Name
                              order by B.EquipmentCommonId";
            m_Sql = string.Format(m_Sql, myStartTime, myEndTime, myFactoryOrganizationId, m_Condition, myStatisticalRange);
            try
            {
                DataTable m_EquipmentHaltTable = myDataFactory.Query(m_Sql);
                //DataTable m_m_EquipmentHaltResultTable = new DataTable();
                //m_EquipmentHaltTable.Columns.Add("EquipmentCommonId", typeof(string));
                //m_EquipmentHaltTable.Columns.Add("Name", typeof(string));
                //m_EquipmentHaltTable.Columns.Add("ReasonStatisticsTypeId", typeof(string));
                //m_EquipmentHaltTable.Columns.Add("DowntimeCount", typeof(int));
                //m_EquipmentHaltTable.Columns.Add("DowntimeTime", typeof(decimal));
                //if (m_EquipmentHaltTable != null)
                //{

                //}
                return m_EquipmentHaltTable;
            }
            catch
            {
                return null;
            }
        }
    }
}

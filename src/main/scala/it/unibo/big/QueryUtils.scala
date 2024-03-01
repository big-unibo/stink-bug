package it.unibo.big

object QueryUtils {
  val fullMonitoredTaksQuery: String = {
    // for each week, for each trap a flag if is inspected y/n - monitored y/n, count of BMSB types
    // date of task assignment and date of task completion
    s"""
       |select
       |	to_char( COALESCE(working.timestamp_assignment, monitoring.timestamp_assignment), 'DD-MM-YYYY') as taskDate,
       |	to_char( COALESCE(working.timestamp_completed, monitoring.timestamp_completed), 'DD-MM-YYYY') as rilevationDate,
       |	COALESCE(working.name,monitoring.name) as name,
       |	COALESCE(working.togo_id,monitoring.togo_id) as togo_id,
       |	COALESCE(working.gid,monitoring.gid) as gid,
       |	COALESCE(working.ms_id,monitoring.ms_id) as ms_id,
       |	COALESCE(is_working,true) as is_working,
       |	COALESCE(monitored,false) as monitored,
       |	monitoring."Adulti", monitoring."Giovani II - III (small)", monitoring."Giovani IV - V (large)"
       |
       |FROM
       |
       |(select togo.timestamp_assignment, togo.timestamp_completed, togo.togo_id, geo.name, geo.gid, geo.ms_id, false as is_working
       | from task_on_geo_object togo
       | join traps geo on geo.gid = togo.gid
       | join given_answer ans on ans.togo_id = togo.togo_id
       | where (ans.answer_id = 'BMSB.PASSNEW.Q2.A2' and geo.ms_id in (9, 12)) or (ans.answer_id IN ('BMSB.PASS.Q1.A3', 'BMSB.PASS.Q1.A4', 'BMSB.PASS.Q1.A5') and geo.ms_id = 6)
       |) working
       |
       |FULL OUTER JOIN
       |
       |((select togo.timestamp_assignment, togo.timestamp_completed, togo.togo_id, geo.name, geo.gid, geo.ms_id, true as monitored,
       |sum(case when q.question_id in ('BMSB.PASS.Q10', 'BMSB.PASS.Q11', 'BMSB.PASS.Q4', 'BMSB.PASSNEW.Q3')
       |	THEN cast(ga.text as integer)
       |	ELSE 0 end) "Adulti",
       |sum(case when q.question_id in ('BMSB.PASS.Q14', 'BMSB.PASS.Q15', 'BMSB.PASS.Q7', 'BMSB.PASSNEW.Q4')
       |	THEN cast(ga.text as integer)
       |	ELSE 0 end) "Giovani II - III (small)",
       |sum(case when q.question_id in ('BMSB.PASS.Q16', 'BMSB.PASS.Q17', 'BMSB.PASS.Q8', 'BMSB.PASSNEW.Q5')
       |	THEN cast(ga.text as integer)
       |	ELSE 0 end) "Giovani IV - V (large)"
       |
       |from task_on_geo_object togo
       |JOIN given_answer ga ON togo.togo_id = ga.togo_id
       |JOIN answer a ON ga.answer_id = a.answer_id JOIN question q ON q.question_id = a.question_id
       |join traps geo on geo.gid = togo.gid
       |where ((togo.task_id = 6 and geo.ms_id in (9, 12)) or (togo.task_id = 3 and geo.ms_id = 6) )
       |and timestamp_completed is not null
       |and q.question_id in ('BMSB.PASS.Q4','BMSB.PASS.Q7','BMSB.PASS.Q8',
       |    'BMSB.PASS.Q10','BMSB.PASS.Q11','BMSB.PASS.Q14','BMSB.PASS.Q15',
       |	'BMSB.PASS.Q16','BMSB.PASS.Q17', 'BMSB.PASSNEW.Q3', 'BMSB.PASSNEW.Q4', 'BMSB.PASSNEW.Q5')
       |group by togo.timestamp_assignment, togo.timestamp_completed, togo.togo_id, geo.name, geo.gid, geo.ms_id
       |)
       |union
       |(
       |select togo_dates.timestamp_assignment, null as togo_id, null as timestamp_completed, inst.name, inst.gid, inst.ms_id, false as monitored, null as "Adulti", null as "Giovani II - III (small)", null as "Giovani IV - V (large)"
       |from
       |
       |(select distinct togo.timestamp_assignment, ms_id as ms_id
       |from task_on_geo_object togo join traps geo on geo.gid = togo.gid
       |where task_id = 6 or task_id = 3
       |) togo_dates,
       |
       |(select togo.timestamp_completed, geo.name, geo.gid, geo.ms_id
       |	from traps geo, task_on_geo_object togo
       |	where ((togo.task_id = 5 and geo.ms_id in (9, 12)) or (togo.task_id = 2 and geo.ms_id = 6) )
       |	and togo.gid = geo.gid
       |	and geo.geom is not null) inst
       |
       |where timestamp_assignment not in (
       |	select timestamp_assignment
       |	from task_on_geo_object togo
       |	join traps geo on geo.gid = togo.gid
       |	where geo.gid = inst.gid
       |	and ((togo.task_id = 6 and geo.ms_id in (9, 12)) or (togo.task_id = 3 and geo.ms_id = 6) )
       |	and timestamp_completed is not null
       |	and togo.timestamp_assignment > inst.timestamp_completed
       |)
       |and timestamp_assignment > inst.timestamp_completed
       |and togo_dates.ms_id = inst.ms_id
       |)) monitoring
       |
       |ON monitoring.gid = working.gid and monitoring.timestamp_assignment = working.timestamp_assignment
       |order by gid, COALESCE(working.timestamp_assignment, monitoring.timestamp_assignment)
       |""".stripMargin
  }

  val notMonitoredTraps: String =
    s"""
       |select togo_dates.timestamp_assignment, null as togo_id, null as timestamp_completed, inst.name, inst.gid, inst.ms_id, false as monitored, null as "Adulti", null as "Giovani II - III (small)", null as "Giovani IV - V (large)"
       |from
       |
       |(select distinct togo.timestamp_assignment, ms_id as ms_id
       |from task_on_geo_object togo join traps geo on geo.gid = togo.gid
       |where task_id = 6 or task_id = 3
       |) togo_dates,
       |
       |(select togo.timestamp_completed, geo.name, geo.gid, geo.ms_id
       |	from traps geo, task_on_geo_object togo
       |	where ((togo.task_id = 5 and geo.ms_id in (9, 12)) or (togo.task_id = 2 and geo.ms_id = 6) )
       |	and togo.gid = geo.gid
       |	and geo.geom is not null) inst
       |
       |where timestamp_assignment not in (
       |	select timestamp_assignment
       |	from task_on_geo_object togo
       |	join traps geo on geo.gid = togo.gid
       |	where geo.gid = inst.gid
       |	and ((togo.task_id = 6 and geo.ms_id in (9, 12)) or (togo.task_id = 3 and geo.ms_id = 6) )
       |	and timestamp_completed is not null
       |	and togo.timestamp_assignment > inst.timestamp_completed
       |)
       |and timestamp_assignment > inst.timestamp_completed
       |and togo_dates.ms_id = inst.ms_id""".stripMargin

}

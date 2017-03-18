with cte_patients
as
(
	select 
	  '@cdmSourceName' as cdm_source_name, 
	  count(distinct subject_id) as person_count 
	from @resultsDatabaseSchema.cohort
	where cohort_definition_id = @cohortDefinitionId
		{@yearStart != ''}?{and year(cohort_start_date) >= @yearStart}
	  {@yearEnd != ''}?{and year(cohort_start_date) <= @yearEnd}
),
cte_total
as
(
	select 
	  '@cdmSourceName' as cdm_source_name, 
	  sum(cast(datediff(d, observation_period_start_date, observation_period_end_date) / 365.25 as bigint)) as person_time
	from @cdmDatabaseSchema.observation_period
	where 1=1
	  {@yearStart != ''}?{and year(observation_period_start_date) >= @yearStart}
	  {@yearEnd != ''}?{and year(observation_period_end_date) <= @yearEnd}
)
select A.cdm_source_name, ((1.0 * A.person_count) / (1.0 * B.person_time)) * 100000 as incidence_per_100k_years
from cte_patients A
join cte_total B on A.cdm_source_name = B.cdm_source_name;

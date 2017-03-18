with cte_patients
as
(
	select 
		year(A.cohort_start_date) as index_year,
		C.concept_name as gender, 
		floor((year(A.cohort_start_date) - B.year_of_birth) / 10) as age_decile, 
		count(distinct A.subject_id) as person_count
		from @resultsDatabaseSchema.cohort A
	join @cdmDatabaseSchema.person B on A.subject_id = B.person_id
	join @cdmDatabaseSchema.concept C on B.gender_concept_id = C.concept_id
	where A.cohort_definition_id = @cohortDefinitionId
	group by year(A.cohort_start_date), C.concept_name, floor((year(A.cohort_start_date) - B.year_of_birth) / 10)
),
cte_denominator
as
(
	select 
	  Y.index_year, 
	  C.concept_name as gender, 
	  floor((Y.index_year - P.year_of_birth) / 10) as age_decile,
	  count(distinct O.person_id) as total_count
	from
	(
		select distinct year(observation_period_start_date) as index_year
		from @cdmDatabaseSchema.observation_period
	) Y
	left join @cdmDatabaseSchema.observation_period O on Y.index_year >= year(O.observation_period_start_date) and 
		Y.index_year <= year(O.observation_period_end_date)
	join @cdmDatabaseSchema.person P on O.person_id = P.person_id
	join @cdmDatabaseSchema.concept C on P.gender_concept_id = C.concept_id
	group by Y.index_year, C.concept_name, floor((Y.index_year - P.year_of_birth) / 10)
)
select 
  A.index_year, 
  A.gender,
  A.age_decile,
  B.person_count,
  A.total_count,
  (1.0*sum(B.person_count)) / (1.0*A.total_count) as incidence
from cte_denominator A
left join cte_patients B
  on A.index_year = B.index_year 
  and A.gender = B.gender 
  and A.age_decile = B.age_decile
group by A.index_year, A.gender, A.age_decile, B.person_count, A.total_count
order by 1,2;


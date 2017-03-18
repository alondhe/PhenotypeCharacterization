IF OBJECT_ID('tempdb..#cohort', 'U') IS NOT NULL
drop table #cohort;


--all cohorts:  new users of drugs, newly diagnosed, new procedures,  1yr washout
create table #cohort 
-- HACK @ctasHashSubjectId
as
select de1.person_id as subject_id, de1.cohort_definition_id, de1.cohort_start_date, de1.cohort_end_date
from
(select 
  person_id, 
  drug_concept_id as cohort_definition_id, 
  drug_era_start_date as cohort_start_date, 
  drug_era_end_date as cohort_end_date, row_number() 
    over (partition by person_id, drug_concept_id order by drug_era_start_date asc) rn1
  from @cdmDatabaseSchema.drug_era
  where drug_concept_id > 0
) de1
inner join @cdmDatabaseSchema.observation_period op1 on de1.person_id = op1.person_id
and de1.cohort_start_date >= dateadd(dd,365,op1.observation_period_start_date) 
and de1.cohort_start_date <= op1.observation_period_end_date
and de1.rn1 = 1
;

IF OBJECT_ID('tempdb..#incident_outcomes', 'U') IS NOT NULL
	drop table #incident_outcomes;


create table #incident_outcomes 
-- HACK @ctasHashPersonId
as
select 
  co1.person_id, 
  @cohortDefinitionId as condition_concept_id, 
  min(co1.cohort_start_date) as condition_start_date
from
(
  select subject_id as person_id, cohort_start_date
  from @resultsDatabaseSchema.cohort
  where cohort_definition_id = @cohortDefinitionId
) co1
group by co1.person_id
;



IF OBJECT_ID('tempdb..#drug_outcome_summary', 'U') IS NOT NULL
drop table #drug_outcome_summary;



create table #drug_outcome_summary 
-- HACK @ctasTempPdw 
as
select do1.cohort_definition_id,
	do1.condition_concept_id,
	d1.num_persons - do1.num_persons_prior_outcome as num_persons_pp_risk,
	do1.num_persons_post_pp,
	d1.pt_pp_post - do1.pt_pp_censor_prior - do1.pt_pp_censor_post as pt_pp,
	do1.num_persons_post_itt,
	d1.pt_itt_post - do1.pt_itt_censor_prior - do1.pt_itt_censor_post as pt_itt
from
(
	select c1.cohort_definition_id,
		count(c1.subject_id) as num_persons,
		sum(datediff(dd,c1.cohort_start_date, c1.cohort_end_date)/365.25) as pt_pp_post,
		sum(datediff(dd,c1.cohort_start_date, op1.observation_period_end_date)/365.25) as pt_itt_post
	from #cohort c1
		inner join @cdmDatabaseSchema.observation_period op1
		on c1.subject_id = op1.person_id
		and c1.cohort_start_date >= op1.observation_period_start_date
		and c1.cohort_start_date <= op1.observation_period_end_date
	group by c1.cohort_definition_id
) d1
inner join
(
select c1.cohort_definition_id, io1.condition_concept_id,
	sum(case when condition_start_date <= c1.cohort_start_date then 1 else 0 end) as num_persons_prior_outcome,
	sum(case when condition_start_date <= c1.cohort_start_date then datediff(dd,c1.cohort_start_date,op1.observation_period_end_date) else 0 end/365.25) as pt_itt_censor_prior,
	sum(case when condition_start_date <= c1.cohort_start_date then datediff(dd,c1.cohort_start_date,c1.cohort_end_date) else 0 end/365.25) as pt_pp_censor_prior,
	sum(case when condition_start_date > c1.cohort_start_date and condition_start_date <= op1.observation_period_end_date then 1 else 0 end) as num_persons_post_itt,
	sum(case when condition_start_date > c1.cohort_start_date and condition_start_date <= op1.observation_period_end_date then datediff(dd,condition_start_date,op1.observation_period_end_date) else 0 end/365.25) as pt_itt_censor_post,
	sum(case when condition_start_date > c1.cohort_start_date and condition_start_date <= c1.cohort_end_date then 1 else 0 end) as num_persons_post_pp,
	sum(case when condition_start_date > c1.cohort_start_date and condition_start_date <= c1.cohort_end_date then datediff(dd,condition_start_date,c1.cohort_end_date) else 0 end/365.25) as pt_pp_censor_post
from
#cohort c1
inner join @cdmDatabaseSchema.observation_period op1
	on c1.subject_id = op1.person_id
	and c1.cohort_start_date >= op1.observation_period_start_date
	and c1.cohort_start_date <= op1.observation_period_end_date
inner join #incident_outcomes io1
on c1.subject_id = io1.person_id
group by c1.cohort_definition_id, io1.condition_concept_id
) do1
on d1.cohort_definition_id = do1.cohort_definition_id
;





create table #hackathon_cohort_outcome_summary 
-- HACK @ctasTempPdw 
as
select dos1.cohort_definition_id,
	c1.concept_name as drug_cohort_definition_name,
	dos1.condition_concept_id,
	@cohortDefinitionId as phenotype_cohort_definition_name,
	dos1.num_persons_pp_risk,
	dos1.num_persons_post_pp,
	dos1.pt_pp,
	1000.0*dos1.num_persons_post_pp / case when dos1.num_persons_pp_risk > 0 then dos1.num_persons_pp_risk else 1 end as ip_pp_1000pp,
	1000.0*dos1.num_persons_post_pp / case when dos1.pt_pp > 0 then dos1.pt_pp else 0.001 end as ir_pp_1000pp,
	dos1.num_persons_post_itt,
	dos1.pt_itt,
	1000.0*dos1.num_persons_post_itt / case when dos1.num_persons_pp_risk > 0 then dos1.num_persons_pp_risk else 1 end as ip_itt_1000pp,
	1000.0*dos1.num_persons_post_itt / case when dos1.pt_itt > 0 then dos1.pt_itt else 0.001 end as ir_itt_1000pp
from #drug_outcome_summary dos1
inner join @cdmDatabaseSchema.concept c1
on dos1.cohort_definition_id = c1.concept_id
;


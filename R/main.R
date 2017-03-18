.onLoad <- function(libname, pkgname)
{
}

# @file main
# @author Ajit Londhe, Christopher Knoll

#' Run Phenotype Characterization Query
#'
#' @details
#' Generates table of cohort measures, stratified
#'
#' @return
#' none
#'
#' @export
characterize <- function(connectionDetails, 
                         cdmDatabaseSchema, 
                         resultsDatabaseSchema, 
                         cohortDefinitionId,
                         cohortTableName = "cohort")
{
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "stratified.sql", 
                                    packageName = "PhenotypeCharacterization", 
                                    dbms = connectionDetails$dbms, 
                                    cdmDatabaseSchema = cdmDatabaseSchema,
                                    resultsDatabaseSchema = resultsDatabaseSchema,
                                    cohortDefinitionId = cohortDefinitionId,
                                    cohortTableName = cohortTableName)
  
  connection <- connect(connectionDetails)
  result <- querySql(connection = connection, sql = sql)
  write.csv(x = result, file = "output/stratified.csv", na = "0", row.names = FALSE)
}

# @file main
# @author Ajit Londhe, Christopher Knoll

#' Run Aggregation of Incidence Query
#'
#' @details
#' Generates a cohort's overall incidence in person-time for a CDM
#'
#' @return
#' none
#'
#' @export
aggregateIncidence <- function(connectionDetails, 
                                    cdmSourceName, 
                                    cdmDatabaseSchema, 
                                    resultsDatabaseSchema, 
                                    cohortDefinitionId,
                                    cohortTableName = "cohort")
{
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "aggregated.sql", 
                                           packageName = "PhenotypeCharacterization", 
                                           dbms = connectionDetails$dbms, 
                                           cdmSourceName = cdmSourceName,
                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                           resultsDatabaseSchema = resultsDatabaseSchema,
                                           cohortDefinitionId = cohortDefinitionId,
                                           cohortTableName = cohortTableName)
  
  connection <- connect(connectionDetails)
  result <- querySql(connection = connection, sql = sql)
  write.csv(x = result, file = "output/aggregated.csv", na = "0", row.names = FALSE)
}

# @file main
# @author Ajit Londhe, Christopher Knoll

#' Plot prevalence stratified by age, gender
#'
#' @details
#' Plot prevalence stratified by age, gender in trellis
#'
#' @return
#' none
#'
#' @export
plotPrevalanceTrellis <- function(result)
{
  #result <- read.csv("phenotypeDemo.csv")
  result$INCIDENCE <- result$INCIDENCE*1000
  
  deciles <- sort(unique(result$AGE_DECILE))
  plots <- lapply(deciles, function(decile) {
    plot_ly(data = result[result$AGE_DECILE == decile,],
            x = ~INDEX_YEAR,
            y = ~INCIDENCE,
            color = ~GENDER,
            colors = "Dark2") %>%
      add_lines(name = "GENDER") %>%
      layout(
        title = "Patients with Condition Stratified by Age, Gender",
        xaxis = list(title = "Year", range = c(min(result$INDEX_YEAR), max(result$INDEX_YEAR))),
        yaxis = list(title = "Incidence per 1000 patients", rangemode = "nonnegative"))
  })
  subplot(plots, shareY = TRUE, titleX = FALSE, titleY = TRUE)
}

# @file main
# @author Ajit Londhe, Patrick Ryan

#' Run Drug Outcome Summary
#'
#' @details
#' Runs Drug Outcome Summary query
#'
#' @return
#' none
#'
#' @export
runDrugOutcomeSummary <- function(connectionDetails, 
                                  cdmSourceName, 
                                  cdmDatabaseSchema, 
                                  resultsDatabaseSchema, 
                                  cohortDefinitionId,
                                  cohortTableName = "cohort")
{
  ctasHashSubjectId <- ctasHashPersonId <- ctasTempPdw <- ""
  if (connectionDetails$dbms == "pdw")
  {
    ctasHashSubjectId <- "with (location=user_db, distribution=hash(subject_id))"
    ctasHashPersonId <- "with (location=user_db, distribution=hash(person_id))"
    ctasTempPdw <- "with (location=user_db, distribution=replicate)"
  }
  if (connectionDetails$dbms == "redshift")
  {
    ctasHashSubjectId <- "distkey(subject_id)"
    ctasHashPersonId <- "distkey(person_id)"
  }
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "drug_outcome_summary.sql", 
                                           packageName = "PhenotypeCharacterization", 
                                           dbms = connectionDetails$dbms,
                                           cdmSourceName = cdmSourceName,
                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                           resultsDatabaseSchema = resultsDatabaseSchema,
                                           cohortDefinitionId = cohortDefinitionId,
                                           cohortTableName = cohortTableName)
  sql <- str_replace_all(string = sql, pattern = "-- HACK ", replacement = "")
  sql <- renderSql(sql = sql, 
                   ctasHashSubjectId = ctasHashSubjectId,
                   ctasHashPersonId = ctasHashPersonId,
                   ctasTempPdw = ctasTempPdw)$sql
  
  connection <- connect(connectionDetails)
  executeSql(connection = connection, sql)
  result <- querySql(connection = connection, sql = "select * from #hackathon_cohort_outcome_summary")
  write.csv(x = result, file = "output/drug_outcome_summary.csv", na = "0", row.names = FALSE)
}
                               
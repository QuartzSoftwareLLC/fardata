getConfig <- function(key, default = NULL) {
  getOption(paste0("far.data.", key), default = default)
}

#' @export
get_tabular_data_extension <- function() {
  getConfig("tabular_data_extension", "parquet")
}

#' @import here
#' @export
get_data_path <- function() {
  path <- here::here(getConfig("data_path", "data"))
  dir.create(path, showWarnings = FALSE, recursive = TRUE)
  path
}

file_cache <- memoise::cache_filesystem("/tmp/far-data")
memory_cache <- memoise::cache_memory()

#' call_cached
#' @description Call a code block with memoisation
#' @param fun The function to memoise
#' @param cache_string The string to use as the cache key
#' @export
call_cached <- function(fun, cache_string = Sys.Date(), cache = memory_cache) {
  quosured <- rlang::enquo(fun)

  hash <- digest::digest(quosured)
  memoised <- memoise::memoise(\(...) rlang::eval_tidy(quosured), cache = cache)

  memoised(cache_string, hash, cache_string)
}

#' query_cdc_data
#' @export
#' @param dataset The dataset to query
#' @param query The query to pass to the dataset
#' @importFrom readr read_csv
#' @importFrom tibble tibble
query_cdc_data <- function(dataset, query) {
  sprintf("https://data.cdc.gov/resource/%s.csv?$query=%s", dataset, query |> URLencode()) |>
    read_csv() |>
    tibble() |> 
    call_cached(cache = memory_cache)
}

query_cdc_data("53g5-jf7x", "SELECT end_date, demographic_type, demographic_values, pathogen, deaths, total_deaths WHERE state = 'United States' and demographic_type in ('Age Group', 'Sex') and not pathogen = 'Combined' LIMIT 15600000")  

#' write_artifact
#' @export
#' @param name The name of the artifact
#' @importFrom readr write_csv
#' @importFrom stringr str_glue
write_artifact <- function(data, data_name = deparse(substitute(data)), data_path = get_data_path(), extension = get_tabular_data_extension()) {
  if (extension == "csv") {
    write_csv(data, str_glue("{data_path}/{data_name}.csv"))
  } else if (extension == "parquet") {
    arrow::write_parquet(data, str_glue("{data_path}/{data_name}.parquet"))
  } else {
    stop("Unsupported extension")
  }
}

#' read_artifact
#' @export
#' @param name The name of the artifact
#' @importFrom readr read_csv
#' @importFrom stringr str_glue str_remove
#' @examples
#' read_artifact(iris)
#' read_artifact("iris")
read_artifact <- function(name, extension = get_tabular_data_extension(), data_path = get_data_path()) {
  if (extension == "csv") {
    read_csv(str_glue("{data_path}/{name}.csv"))
  } else if (extension == "parquet") {
    arrow::read_parquet(str_glue("{data_path}/{name}.parquet"))
  } else {
    stop("Unsupported extension")
  }
}

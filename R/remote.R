getConfig <- function(key, default = NULL) {
  getOption(paste0("far.data.", key), default = default)
}

#' @export
get_tabular_data_extension <- function() {
  getConfig("tabular_data_extension", "csv")
}

#' @import here
#' @export
get_data_path <- function() {
  path <- getConfig("data_path", "data")
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

#' write_artifact
#' @export
#' @param name The name of the artifact
#' @importFrom readr write_csv
#' @importFrom stringr str_glue
#' @import aws.s3
write_artifact <- function(data, data_name = deparse(substitute(data)), data_path = get_data_path(), extension = get_tabular_data_extension()) {
  file_name <- str_glue("{data_name}.{extension}")
  path <- str_glue("{data_path}/{file_name}")
  
  if (extension == "csv") {
    write_csv(data, path)
  } else if (extension == "parquet") {
    arrow::write_parquet(data, path)
  } else {
    stop("Unsupported extension")
  }

  if(!is.null(getConfig("bucket"))) {
      aws.s3::put_object(file = path, object = path , bucket = getConfig("bucket"))

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
read_artifact <- function(name, extension = get_tabular_data_extension(), data_path = get_data_path(), env = globalenv()) {
  file_name <- str_glue("{name}.{extension}")
  path <- str_glue("{data_path}/{file_name}")

  if(!is.null(getConfig("bucket"))) {
    aws.s3::save_object(object = path, bucket = getConfig("bucket"), file = path)
  }

  if (extension == "csv") {
    read_csv(str_glue("{data_path}/{name}.csv")) |>
      call_cached(cache_string = name) |>
      assign(name, value = _,  envir = env)
  } else if (extension == "parquet") {
    arrow::read_parquet(str_glue("{data_path}/{name}.parquet")) |>
      call_cached(cache_string = name) |>
      assign(name, value = _, envir = env)
  } else {
    stop("Unsupported extension")
  }

  get(name, env = env)
}

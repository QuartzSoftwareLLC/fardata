test_that("write_artifact works with iris dataset", {
  temp_dir <- tempdir()
  on.exit(unlink(temp_dir), add = TRUE)
  options(fardata.data_path = temp_dir)

  iris <- datasets::iris
  write_artifact(iris)
  result <- read_artifact(iris)

  expect_equal(result, tibble(iris))
})

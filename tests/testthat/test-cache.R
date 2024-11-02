test_that("cache works", {
  data <- "one"

  result <- call_cached(data)

  data <- "two"

  expect_equal(result, "one")

})

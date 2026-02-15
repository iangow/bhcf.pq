#' Scan BHCF Parquet files into DuckDB
#'
#' @param conn A valid DuckDB connection.
#' @param kind Optional BHCF parquet kind (e.g. `"bhcf_float"`, `"float"`).
#'   All matching files of the form `bhcf_<kind>_YYYYMMDD.parquet` are scanned.
#' @param pq_file Optional Parquet filename or glob relative to resolved output
#'   directory. May also be an absolute parquet path/glob when `data_dir` is not
#'   supplied.
#' @param data_dir Optional parent directory for Parquet output.
#' @param schema Schema subdirectory. Default is `"bhcf"`.
#' @param keep_filename Logical; include source filename in output.
#'
#' @return A lazy `tbl` backed by DuckDB.
#' @export
bhcf_scan_pqs <- function(conn,
                          kind = NULL,
                          pq_file = NULL,
                          data_dir = NULL,
                          schema = "bhcf",
                          keep_filename = FALSE) {

  stopifnot(DBI::dbIsValid(conn))

  # allow backward-compatible positional use: bhcf_scan_pqs(conn, "bhcf_float")
  n_specified <- sum(!is.null(kind), !is.null(pq_file))
  if (n_specified == 0L) {
    kind <- "bhcf"
  } else if (n_specified > 1L) {
    stop("Provide at most one of `kind` or `pq_file`.", call. = FALSE)
  }

  pq_path <- resolve_out_dir(data_dir = data_dir, schema = schema)
  if (!is.null(pq_path)) {
    pq_path <- normalizePath(pq_path, mustWork = FALSE)
  }

  if (!is.null(kind)) {
    if (is.null(pq_path) || !nzchar(pq_path)) {
      stop("Provide `data_dir` or set `DATA_DIR`.", call. = FALSE)
    }

    kind_pattern <- if (identical(kind, "bhcf")) {
      "bhcf"
    } else if (startsWith(kind, "bhcf_")) {
      kind
    } else {
      paste0("bhcf_", kind)
    }
    glob <- file.path(pq_path, sprintf("%s_*.parquet", kind_pattern))
  } else {
    glob <- if (is.null(pq_path) || !nzchar(pq_path)) {
      pq_file
    } else {
      file.path(pq_path, pq_file)
    }
  }

  matches <- Sys.glob(glob)
  if (length(matches) == 0L) {
    stop(sprintf("No parquet files found with pattern:\n  %s", glob), call. = FALSE)
  }

  sql <- sprintf(
    "SELECT * FROM read_parquet(%s, union_by_name=true, filename=%s)",
    DBI::dbQuoteString(conn, glob),
    if (isTRUE(keep_filename)) "true" else "false"
  )

  dplyr::tbl(conn, dbplyr::sql(sql))
}

#' Check primary-key and non-NULL constraints in BHCF Parquet files
#'
#' @param conn A valid DuckDB connection.
#' @param cols Character vector of columns that should be non-NULL and unique
#'   jointly.
#' @param pq_file Optional Parquet filename/glob passed to [bhcf_scan_pqs()].
#' @param data_dir Optional parent directory for Parquet output.
#' @param schema Schema subdirectory. Default is `"bhcf"`.
#'
#' @return A tibble with key-check results.
#' @export
bhcf_check_pq_keys <- function(conn,
                               cols,
                               pq_file = NULL,
                               data_dir = NULL,
                               schema = "bhcf") {

  df <- bhcf_scan_pqs(
    conn = conn,
    pq_file = pq_file,
    data_dir = data_dir,
    schema = schema,
    keep_filename = FALSE
  )

  res <- check_pk_and_non_null(df, cols)

  tibble::tibble(
    ok = res$ok,
    null_violations = list(res$null_violations),
    pk_violations = list(res$pk_violations)
  )
}

#' @keywords internal
#' @noRd
check_pk_and_non_null <- function(df, cols) {
  stopifnot(is.character(cols))

  nulls <- df |>
    dplyr::summarise(
      dplyr::across(
        dplyr::all_of(cols),
        ~ sum(is.na(.x), na.rm = TRUE),
        .names = "{.col}"
      )
    ) |>
    tidyr::pivot_longer(
      dplyr::everything(),
      names_to = "column",
      values_to = "n_na"
    ) |>
    dplyr::filter(.data$n_na > 0L) |>
    dplyr::collect()

  dupes <- df |>
    dplyr::count(dplyr::across(dplyr::all_of(cols))) |>
    dplyr::filter(.data$n > 1L) |>
    dplyr::collect()

  list(
    ok = nrow(nulls) == 0L && nrow(dupes) == 0L,
    null_violations = nulls,
    pk_violations = dupes
  )
}

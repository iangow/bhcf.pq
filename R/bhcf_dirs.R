#' @keywords internal
#' @noRd
resolve_in_dir <- function(raw_data_dir = NULL, schema = NULL) {
  base <- if (!is.null(raw_data_dir) && nzchar(raw_data_dir)) {
    raw_data_dir
  } else {
    Sys.getenv("RAW_DATA_DIR", unset = "")
  }

  if (!nzchar(base)) return(NULL)
  if (is.null(schema)) base else file.path(base, schema)
}

#' @keywords internal
#' @noRd
resolve_out_dir <- function(data_dir = NULL, schema = NULL) {
  base <- if (!is.null(data_dir) && nzchar(data_dir)) {
    data_dir
  } else {
    Sys.getenv("DATA_DIR", unset = "")
  }

  if (!nzchar(base)) return(NULL)
  if (is.null(schema)) base else file.path(base, schema)
}

#' @keywords internal
#' @noRd
resolve_dirs <- function(raw_data_dir = NULL, data_dir = NULL,
                         schema = "bhcf") {

  in_dir <- resolve_in_dir(raw_data_dir = raw_data_dir, schema = schema)
  out_dir <- resolve_out_dir(data_dir = data_dir, schema = schema)

  if (!is.null(in_dir)) in_dir <- normalizePath(in_dir, mustWork = TRUE)
  if (!is.null(out_dir)) out_dir <- normalizePath(out_dir, mustWork = FALSE)

  list(in_dir = in_dir, out_dir = out_dir)
}

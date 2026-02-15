# Build bhcf_items from the BHCF lookup table.
#
# Usage:
#   source("data-raw/bhcf_items.R")
#   build_bhcf_items()

build_bhcf_items <- function(
    lookup = NULL,
    data_dir = NULL,
    schema = "bhcf",
    use_mdrm = TRUE,
    mdrm_url = "https://www.federalreserve.gov/apps/mdrm/pdf/MDRM.zip",
    out_data_file = "data/bhcf_items.rda",
    out_extdata_file = "inst/extdata/bhcf_items.parquet") {

  if (is.null(lookup)) {
    if (exists("bhcf_types", inherits = TRUE)) {
      lookup <- get("bhcf_types", inherits = TRUE)
    } else if (file.exists("data/bhcf_types.rda")) {
      e <- new.env(parent = emptyenv())
      load("data/bhcf_types.rda", envir = e)
      lookup <- e$bhcf_types
    } else {
      stop("Provide `lookup`, load `bhcf_types`, or create data/bhcf_types.rda first.", call. = FALSE)
    }
  }

  lookup <- tibble::as_tibble(lookup)
  if (!"item" %in% names(lookup)) {
    stop("`lookup` must include an `item` column.", call. = FALSE)
  }

  # Use the first available descriptive name column if present.
  name_col <- c("item_name", "description", "desc", "label")[c("item_name", "description", "desc", "label") %in% names(lookup)]
  if (length(name_col) == 0L) {
    lookup$item_name <- NA_character_
    name_col <- "item_name"
  } else {
    name_col <- name_col[[1]]
  }

  dtype_map <- c(
    float = "Float64",
    int = "Int32",
    integer = "Int32",
    text = "Utf8",
    string = "Utf8",
    date = "Date32",
    bool = "Boolean",
    boolean = "Boolean"
  )

  out_base <- if (!is.null(data_dir) && nzchar(data_dir)) {
    if (is.null(schema)) data_dir else file.path(data_dir, schema)
  } else {
    env_base <- Sys.getenv("DATA_DIR", unset = "")
    if (!nzchar(env_base)) NULL else if (is.null(schema)) env_base else file.path(env_base, schema)
  }

  observed <- tibble::tibble(
    item = as.character(lookup$item),
    data_type = tolower(as.character(lookup$data_type))
  ) |>
    dplyr::filter(!is.na(.data$item), nzchar(.data$item)) |>
    dplyr::distinct()

  if (!is.null(out_base) && dir.exists(out_base)) {
    type_patterns <- c(float = "float", int = "int", text = "text", date = "date", bool = "bool")

    observed_from_pq <- purrr::imap_dfr(type_patterns, function(tp, nm) {
      files <- list.files(
        out_base,
        pattern = paste0("^bhcf_", tp, "_\\d{8}\\.parquet$"),
        full.names = TRUE
      )
      if (length(files) == 0L) return(NULL)

      ds <- arrow::open_dataset(files)
      ds |>
        dplyr::select(item) |>
        dplyr::distinct() |>
        dplyr::collect() |>
        dplyr::mutate(data_type = nm)
    })

    if (nrow(observed_from_pq) > 0L) {
      observed <- observed |>
        dplyr::bind_rows(observed_from_pq) |>
        dplyr::distinct(.data$item, .keep_all = TRUE)
    }
  }

  joined <- observed |>
    dplyr::left_join(lookup, by = "item", suffix = c("", ".lookup"))

  name_candidates <- c("item_name", "description", "desc", "label")
  name_candidates <- name_candidates[name_candidates %in% names(joined)]

  item_name_vec <- if (length(name_candidates) == 0L) {
    rep(NA_character_, nrow(joined))
  } else {
    out <- as.character(joined[[name_candidates[[1]]]])
    if (length(name_candidates) > 1L) {
      for (nm in name_candidates[-1]) {
        out <- dplyr::coalesce(out, as.character(joined[[nm]]))
      }
    }
    out
  }

  # Optionally backfill item names from MDRM when lookup does not provide them.
  if (isTRUE(use_mdrm) && any(is.na(item_name_vec) | !nzchar(item_name_vec))) {
    mdrm_zip <- tempfile(fileext = ".zip")
    try({
      utils::download.file(mdrm_url, mdrm_zip, mode = "wb", quiet = TRUE)
      zip_contents <- utils::unzip(mdrm_zip, list = TRUE)$Name
      if ("MDRM_CSV.csv" %in% zip_contents) {
        mdrm_con <- function() unz(mdrm_zip, "MDRM_CSV.csv", open = "rb")

        # First pass: read one data row to obtain reliable header names.
        mdrm_sample <- readr::read_csv(
          mdrm_con(),
          skip = 1,
          n_max = 1,
          show_col_types = FALSE,
          progress = FALSE
        )

        clean_names <- function(x) {
          x <- tolower(trimws(x))
          x <- gsub("[^a-z0-9]+", "_", x)
          x <- gsub("^_+|_+$", "", x)
          make.unique(x, sep = "_")
        }

        col_names <- clean_names(names(mdrm_sample))

        mdrm <- readr::read_csv(
          mdrm_con(),
          skip = 2,
          col_names = col_names,
          col_types = readr::cols(.default = readr::col_character()),
          show_col_types = FALSE,
          progress = FALSE
        )

        if (all(c("mnemonic", "item_code", "item_name") %in% col_names)) {
          mdrm_names <- mdrm |>
            dplyr::transmute(
              item = paste0(.data$mnemonic, .data$item_code),
              mdrm_item_name = as.character(.data$item_name)
            ) |>
            dplyr::distinct(.data$item, .keep_all = TRUE)

          joined2 <- joined |>
            dplyr::left_join(mdrm_names, by = "item")

          item_name_vec <- dplyr::coalesce(
            item_name_vec,
            joined2$mdrm_item_name
          )
          joined <- joined2
        }
      }
    }, silent = TRUE)
  }

  bhcf_items <- joined |>
    dplyr::mutate(.item_name = item_name_vec) |>
    dplyr::transmute(
      item = as.character(.data$item),
      mnemonic = stringr::str_sub(.data$item, 1, 4),
      item_code = stringr::str_sub(.data$item, 5),
      item_name = as.character(.data$.item_name),
      data_type = dplyr::coalesce(
        unname(dtype_map[tolower(as.character(.data$data_type))]),
        unname(dtype_map[tolower(as.character(.data$data_type.lookup))]),
        as.character(.data$data_type),
        as.character(.data$data_type.lookup)
      )
    ) |>
    dplyr::distinct() |>
    dplyr::arrange(.data$item)

  dir.create(dirname(out_data_file), recursive = TRUE, showWarnings = FALSE)
  dir.create(dirname(out_extdata_file), recursive = TRUE, showWarnings = FALSE)

  save(bhcf_items, file = out_data_file, compress = "bzip2")
  if (requireNamespace("arrow", quietly = TRUE)) {
    arrow::write_parquet(bhcf_items, out_extdata_file)
  }

  invisible(bhcf_items)
}

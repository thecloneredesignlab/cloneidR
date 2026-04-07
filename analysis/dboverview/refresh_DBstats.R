#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(DBI)
  library(RMySQL)
  library(yaml)
  library(jsonlite)
})

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0 || identical(x, "")) y else x

parse_args <- function(argv) {
  out <- list(
    output_dir = getwd(),
    config = NA_character_
  )

  i <- 1L
  while (i <= length(argv)) {
    arg <- argv[[i]]
    if (identical(arg, "--output-dir")) {
      i <- i + 1L
      if (i > length(argv)) stop("--output-dir requires a value")
      out$output_dir <- argv[[i]]
    } else if (identical(arg, "--config")) {
      i <- i + 1L
      if (i > length(argv)) stop("--config requires a value")
      out$config <- argv[[i]]
    } else {
      stop("Unknown argument: ", arg)
    }
    i <- i + 1L
  }

  out
}

candidate_config_paths <- function(explicit_path = NA_character_) {
  script_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
  script_file <- if (length(script_arg) > 0) sub("^--file=", "", script_arg[[1]]) else NA_character_
  repo_root_from_script <- if (!is.na(script_file)) {
    normalizePath(file.path(dirname(script_file), "..", ".."), winslash = "/", mustWork = FALSE)
  } else {
    NA_character_
  }

  paths <- c(
    explicit_path,
    Sys.getenv("CLONEID_CONFIG_FILE", unset = NA_character_),
    Sys.getenv("CONFIG_FILE", unset = NA_character_),
    tryCatch({
      pkg_cfg <- system.file("config", "config.yaml", package = "cloneid")
      if (nzchar(pkg_cfg)) pkg_cfg else NA_character_
    }, error = function(...) NA_character_),
    "/opt/lake/cloneid/capi.yaml",
    "/opt/lake/cloneid/db/credentials/config.yaml",
    file.path(normalizePath(getwd(), winslash = "/", mustWork = FALSE), "cloneid-aws-repos/cloneid/rpackage/inst/config/config.yaml"),
    file.path(repo_root_from_script, "cloneid-aws-repos/cloneid/rpackage/inst/config/config.yaml")
  )

  paths <- unique(paths[!is.na(paths) & nzchar(paths)])
  paths[file.exists(paths)]
}

read_cloneid_config <- function(explicit_path = NA_character_) {
  candidates <- candidate_config_paths(explicit_path)
  if (length(candidates) == 0) {
    stop(
      "Could not locate a CLONEID config.yaml/capi.yaml. ",
      "Pass --config or set CLONEID_CONFIG_FILE/CONFIG_FILE."
    )
  }

  path <- candidates[[1]]
  cfg <- yaml::read_yaml(path)
  list(path = path, config = cfg)
}

effective_mysql_config <- function(cfg) {
  mysql <- cfg$mysqlConnection %||% list()

  list(
    host = Sys.getenv("SQL_HOST", unset = mysql$host %||% NA_character_),
    port = as.integer(Sys.getenv("SQL_PORT", unset = as.character(mysql$port %||% NA_character_))),
    user = Sys.getenv("SQL_USER", unset = mysql$user %||% NA_character_),
    password = Sys.getenv("SQL_PASSWORD", unset = mysql$password %||% NA_character_),
    database = Sys.getenv("SQL_DATABASE", unset = mysql$database %||% NA_character_)
  )
}

effective_cellseg_config <- function(cfg) {
  cellseg <- cfg$cellSegmentation %||% list()

  list(
    backend = tolower(Sys.getenv("CELLSEGMENTATIONS_BACKEND", unset = cellseg$backend %||% "local")),
    input = Sys.getenv("CELLSEGMENTATIONS_INDIR", unset = cellseg$input %||% NA_character_),
    output = Sys.getenv("CELLSEGMENTATIONS_OUTDIR", unset = cellseg$output %||% NA_character_),
    bucket = Sys.getenv("CELLSEGMENTATIONS_BUCKET", unset = cellseg$bucket %||% NA_character_),
    region = Sys.getenv("CELLSEGMENTATIONS_REGION", unset = cellseg$region %||% NA_character_),
    endpoint = Sys.getenv("CELLSEGMENTATIONS_ENDPOINT", unset = cellseg$endpoint %||% ""),
    inputPrefix = Sys.getenv("CELLSEGMENTATIONS_INPUT_PREFIX", unset = cellseg$inputPrefix %||% NA_character_),
    outputPrefix = Sys.getenv("CELLSEGMENTATIONS_OUTPUT_PREFIX", unset = cellseg$outputPrefix %||% NA_character_)
  )
}

validate_required <- function(cfg, fields, label) {
  missing <- fields[!nzchar(trimws(as.character(unlist(cfg[fields]))))]
  if (length(missing) > 0) {
    stop(
      "Missing required ", label, " settings: ",
      paste(missing, collapse = ", "),
      ". Supply them via the stamped config or environment overrides."
    )
  }
}

connect_cloneid_db <- function(mysql_cfg) {
  validate_required(mysql_cfg, c("host", "port", "user", "password", "database"), "mysql")
  DBI::dbConnect(
    RMySQL::MySQL(),
    host = mysql_cfg$host,
    port = mysql_cfg$port,
    user = mysql_cfg$user,
    password = mysql_cfg$password,
    dbname = mysql_cfg$database
  )
}

require_table_columns <- function(conn, table, columns) {
  tables <- DBI::dbListTables(conn)
  if (!(table %in% tables)) {
    stop("Required table missing: ", table)
  }

  fields <- DBI::dbListFields(conn, table)
  missing_cols <- setdiff(columns, fields)
  if (length(missing_cols) > 0) {
    stop("Table ", table, " is missing required columns: ", paste(missing_cols, collapse = ", "))
  }
}

query_df <- function(conn, sql) {
  DBI::dbGetQuery(conn, sql)
}

query_scalar <- function(conn, sql) {
  df <- query_df(conn, sql)
  if (nrow(df) != 1L || ncol(df) != 1L) {
    stop("Expected 1x1 result for scalar query, got ", nrow(df), "x", ncol(df), ": ", sql)
  }
  df[[1]][[1]]
}

s3_extract_keys <- function(objects) {
  if (length(objects) == 0) return(character(0))
  vapply(objects, function(x) x[["Key"]] %||% "", character(1))
}

list_s3_keys <- function(bucket, prefix, region = "", endpoint = "") {
  suppressPackageStartupMessages(library(aws.s3))
  args <- list(bucket = bucket, prefix = prefix, max = Inf)
  if (nzchar(endpoint)) {
    old_endpoint <- Sys.getenv("AWS_S3_ENDPOINT", unset = "")
    on.exit(Sys.setenv(AWS_S3_ENDPOINT = old_endpoint), add = TRUE)
    Sys.setenv(AWS_S3_ENDPOINT = endpoint)
  }
  if (nzchar(region)) {
    old_region <- Sys.getenv("AWS_DEFAULT_REGION", unset = "")
    on.exit(Sys.setenv(AWS_DEFAULT_REGION = old_region), add = TRUE)
    Sys.setenv(AWS_DEFAULT_REGION = region)
  }
  s3_extract_keys(do.call(aws.s3::get_bucket, args))
}

list_storage_keys <- function(cellseg_cfg, which = c("input", "images")) {
  which <- match.arg(which)
  if (!identical(cellseg_cfg$backend, "s3")) {
    root <- if (which == "input") cellseg_cfg$input else file.path(cellseg_cfg$output, "Images")
    if (!dir.exists(root)) {
      stop("Configured local storage path does not exist: ", root)
    }
    return(list.files(root, recursive = TRUE, full.names = FALSE))
  }

  validate_required(cellseg_cfg, c("bucket"), "cellseg")
  prefix <- if (which == "input") {
    cellseg_cfg$inputPrefix %||% sub("^s3://[^/]+/?", "", cellseg_cfg$input)
  } else {
    file.path(cellseg_cfg$outputPrefix %||% sub("^s3://[^/]+/?", "", cellseg_cfg$output), "Images")
  }

  prefix <- gsub("^/+", "", prefix)
  prefix <- gsub("/+$", "", prefix)

  list_s3_keys(
    bucket = cellseg_cfg$bucket,
    prefix = prefix,
    region = cellseg_cfg$region %||% "",
    endpoint = cellseg_cfg$endpoint %||% ""
  )
}

classify_image_key <- function(key) {
  name <- basename(key)
  if (grepl("_(t1|t2|flair|pd)(_|\\.|$)", name, ignore.case = TRUE)) {
    return("medical_imaging_proxy")
  }
  if (grepl("_[0-9]+x_ph_", name, ignore.case = TRUE)) {
    return("microscopy_proxy")
  }
  if (grepl("\\.(svs|ndpi|mrxs|scn)$", name, ignore.case = TRUE)) {
    return("digital_pathology_proxy")
  }
  "other_or_unclassified"
}

extract_event_id <- function(key) {
  name <- basename(key)
  hit <- regexec("^(.+?)_((?:[0-9]+x_ph)|t1|t2|flair|pd)(?:_|\\.)", name, ignore.case = TRUE)
  parts <- regmatches(name, hit)[[1]]
  if (length(parts) >= 2) parts[[2]] else NA_character_
}

normalize_sample_type <- function(which_type) {
  if (is.null(which_type) || length(which_type) == 0) {
    return(character(0))
  }

  out <- trimws(as.character(which_type))
  out[is.na(out) | !nzchar(out)] <- "Unknown"
  out <- sub("^patient$", "clinical", out, ignore.case = TRUE)
  out <- sub("^cell line$", "in vitro", out, ignore.case = TRUE)
  out
}

load_sample_types_by_cellline <- function(conn) {
  rows <- query_df(conn, "SELECT name, whichType FROM CellLinesAndPatients")
  if (nrow(rows) == 0) {
    return(setNames(character(0), character(0)))
  }
  rows$name <- trimws(as.character(rows$name))
  rows$whichType <- normalize_sample_type(rows$whichType)
  rows <- rows[nzchar(rows$name) & !duplicated(rows$name), , drop = FALSE]
  setNames(rows$whichType, rows$name)
}

trace_descendants_df <- function(ids, passaging_df, recursive = TRUE) {
  ids <- unique(trimws(as.character(ids)))
  ids <- ids[nzchar(ids)]
  if (length(ids) == 0) {
    return(character(0))
  }
  kids_map <- split(as.character(passaging_df$id), as.character(passaging_df$passaged_from_id1))
  children <- unique(unlist(kids_map[ids], use.names = FALSE))
  children <- children[!is.na(children) & nzchar(children)]
  if (length(children) == 0) {
    return(character(0))
  }
  if (isTRUE(recursive)) {
    descendants <- trace_descendants_df(children, passaging_df, recursive = TRUE)
    children <- unique(c(children, descendants))
  }
  children
}

compute_cellline_summary_rows <- function(conn) {
  passaging_rows <- query_df(
    conn,
    paste(
      "SELECT id, cellLine, passaged_from_id1",
      "FROM Passaging",
      "WHERE cellLine IS NOT NULL AND cellLine <> ''"
    )
  )
  if (nrow(passaging_rows) == 0) {
    return(data.frame(cellLine=character(0), rootId=character(0), pathLen=integer(0), whichType=character(0), stringsAsFactors=FALSE))
  }

  passaging_rows$id <- trimws(as.character(passaging_rows$id))
  passaging_rows$cellLine <- trimws(as.character(passaging_rows$cellLine))
  passaging_rows$passaged_from_id1 <- as.character(passaging_rows$passaged_from_id1)
  passaging_rows <- passaging_rows[nzchar(passaging_rows$id) & nzchar(passaging_rows$cellLine), , drop = FALSE]
  rows_by_cellline <- split(passaging_rows, passaging_rows$cellLine)
  sample_types <- load_sample_types_by_cellline(conn)
  out <- list()

  for (cellline in names(rows_by_cellline)) {
    cellline_rows <- rows_by_cellline[[cellline]]
    root_ids <- trimws(as.character(cellline_rows$id[is.na(cellline_rows$passaged_from_id1) | !nzchar(trimws(cellline_rows$passaged_from_id1))]))
    root_ids <- unique(root_ids[nzchar(root_ids)])
    if (length(root_ids) == 0) next

    which_type <- normalize_sample_type(unname(sample_types[[cellline]]))
    if (length(which_type) == 0) which_type <- "Unknown"
    if (length(root_ids) == 1L) {
      path_len <- nrow(cellline_rows)
      if (path_len > 1L) {
        out[[length(out) + 1L]] <- data.frame(cellLine=cellline, rootId=root_ids[[1]], pathLen=as.integer(path_len), whichType=which_type, stringsAsFactors=FALSE)
      }
    } else {
      for (root_id in root_ids) {
        path_len <- length(trace_descendants_df(root_id, cellline_rows, recursive = TRUE)) + 1L
        if (path_len > 1L) {
          out[[length(out) + 1L]] <- data.frame(cellLine=cellline, rootId=root_id, pathLen=as.integer(path_len), whichType=which_type, stringsAsFactors=FALSE)
        }
      }
    }
  }

  if (length(out) == 0) {
    return(data.frame(cellLine=character(0), rootId=character(0), pathLen=integer(0), whichType=character(0), stringsAsFactors=FALSE))
  }

  summary_rows <- do.call(rbind, out)
  summary_rows <- summary_rows[order(summary_rows$pathLen, summary_rows$rootId), , drop = FALSE]
  rownames(summary_rows) <- NULL
  summary_rows
}

table_count_df <- function(x, name) {
  if (length(x) == 0) {
    return(data.frame(category = character(0), n = integer(0), stringsAsFactors = FALSE))
  }
  tbl <- sort(table(x), decreasing = TRUE)
  data.frame(
    category = names(tbl),
    n = as.integer(tbl),
    stringsAsFactors = FALSE
  )
}

ensure_dir <- function(path) {
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  normalizePath(path, winslash = "/", mustWork = TRUE)
}

build_summary_row <- function(stats, metadata) {
  data.frame(
    query_timestamp_utc = metadata$query_timestamp_utc,
    db_host = metadata$db_host,
    db_name = metadata$db_name,
    config_path = metadata$config_path,
    passaging_rows = stats$passaging_rows,
    rooted_event_histories = stats$rooted_event_histories,
    backend_case_count = stats$backend_case_count,
    passaging_source_count = stats$passaging_source_count,
    passaging_perspective_source_union = stats$passaging_perspective_source_union,
    passaging_perspective_source_union_excluding_test = stats$passaging_perspective_source_union_excluding_test,
    perspective_rows = stats$perspective_rows,
    perspective_distinct_parent_objects = stats$perspective_distinct_parent_objects,
    identity_rows = stats$identity_rows,
    raw_image_inputs = stats$raw_image_inputs,
    derived_image_outputs = stats$derived_image_outputs,
    raw_images_linked_to_events = stats$raw_images_linked_to_events,
    raw_images_linked_to_event_relationships = stats$raw_images_linked_to_event_relationships,
    distinct_events_referenced_by_raw_images = stats$distinct_events_referenced_by_raw_images,
    stringsAsFactors = FALSE
  )
}

normalize_which_perspective <- function(x) {
  x <- trimws(as.character(x %||% ""))
  ifelse(nzchar(x), x, "Unknown")
}

table_count_named_df <- function(x, column_name = "whichPerspective") {
  x <- normalize_which_perspective(x)
  tbl <- sort(table(x), decreasing = TRUE)
  data.frame(
    setNames(list(names(tbl)), column_name),
    n = as.integer(tbl),
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
}

compute_perspective_leaf_clone_breakdown <- function(conn) {
  perspective_rows <- query_df(
    conn,
    paste(
      "SELECT cloneID, parent, whichPerspective",
      "FROM Perspective"
    )
  )

  if (nrow(perspective_rows) == 0) {
    empty <- data.frame(whichPerspective = character(0), n = integer(0), stringsAsFactors = FALSE)
    return(list(
      single_cells = empty,
      clones = empty,
      totals = list(single_cells = 0L, clones = 0L)
    ))
  }

  perspective_rows$cloneID <- suppressWarnings(as.integer(perspective_rows$cloneID))
  perspective_rows$parent <- suppressWarnings(as.integer(perspective_rows$parent))
  perspective_rows$whichPerspective <- normalize_which_perspective(perspective_rows$whichPerspective)

  parent_ids <- sort(unique(stats::na.omit(perspective_rows$parent)))
  is_clone_parent <- !is.na(perspective_rows$cloneID) & perspective_rows$cloneID %in% parent_ids

  single_cells_df <- perspective_rows[!is_clone_parent, , drop = FALSE]
  clones_df <- perspective_rows[is_clone_parent, , drop = FALSE]

  list(
    single_cells = table_count_named_df(single_cells_df$whichPerspective),
    clones = table_count_named_df(clones_df$whichPerspective),
    totals = list(
      single_cells = nrow(single_cells_df),
      clones = nrow(clones_df)
    )
  )
}

case_summary_type_colors <- function(which_types) {
  palette_map <- c(
    "clinical" = "#6b6b6b",
    "in vivo" = "#9a9a9a",
    "in vitro" = "#3f3f3f",
    "Unknown" = "#c9c9c9",
    "Other" = "#b3b3b3"
  )
  x <- normalize_sample_type(which_types)
  cols <- unname(palette_map[x])
  cols[is.na(cols)] <- "#808080"
  cols
}

collapse_case_summary_for_barplot <- function(case_summary_rows, top_n = 24L) {
  if (nrow(case_summary_rows) == 0) {
    return(data.frame(
      label = character(0),
      pathLen = integer(0),
      whichType = character(0),
      stringsAsFactors = FALSE
    ))
  }

  df <- case_summary_rows[order(case_summary_rows$pathLen, case_summary_rows$rootId, decreasing = TRUE), , drop = FALSE]
  df$label <- paste(df$cellLine, df$rootId, sep = " | ")

  if (nrow(df) <= (top_n + 1L)) {
    out <- df[, c("label", "pathLen", "whichType"), drop = FALSE]
    rownames(out) <- NULL
    return(out)
  }

  top_df <- df[seq_len(top_n), c("label", "pathLen", "whichType"), drop = FALSE]
  other_df <- df[(top_n + 1L):nrow(df), , drop = FALSE]
  others_row <- data.frame(
    label = "others",
    pathLen = sum(other_df$pathLen, na.rm = TRUE),
    whichType = "Other",
    stringsAsFactors = FALSE
  )
  out <- rbind(top_df, others_row)
  rownames(out) <- NULL
  out
}

draw_case_summary_barplot <- function(case_summary_rows,
                                      top_n = 24L,
                                      main_title = "CLONEID rooted histories by lineage length") {
  plot_df <- collapse_case_summary_for_barplot(case_summary_rows, top_n = top_n)
  if (nrow(plot_df) == 0) {
    graphics::plot.new()
    graphics::text(0.5, 0.55, labels = main_title, cex = 1.2, font = 2)
    graphics::text(0.5, 0.45, labels = "No data available", cex = 1)
    return(invisible(plot_df))
  }

  labels <- plot_df$label
  cols <- case_summary_type_colors(plot_df$whichType)
  old_xpd <- graphics::par("xpd")
  on.exit(graphics::par(xpd = old_xpd), add = TRUE)
  graphics::par(xpd = TRUE)
  mids <- graphics::barplot(
    height = plot_df$pathLen,
    horiz = TRUE,
    names.arg = labels,
    las = 1,
    col = cols,
    border = NA,
    xlab = "Path length",
    main = main_title,
    cex.names = 0.82,
    cex.axis = 0.95,
    cex.lab = 1.0,
    cex.main = 1.1
  )
  graphics::box(bty = "l")
  offset <- max(plot_df$pathLen, na.rm = TRUE) * 0.01
  graphics::text(plot_df$pathLen + offset, mids, labels = plot_df$pathLen, pos = 4, cex = 0.85)
  legend_types <- unique(plot_df$whichType)
  graphics::legend(
    "topright",
    legend = legend_types,
    fill = case_summary_type_colors(legend_types),
    bty = "n",
    cex = 0.9,
    title = "whichType"
  )
  invisible(plot_df)
}

write_case_summary_barplot <- function(case_summary_rows, output_png, top_n = 24L) {
  grDevices::png(output_png, width = 2400, height = 2000, res = 220)
  op <- graphics::par(no.readonly = TRUE)
  on.exit({graphics::par(op); grDevices::dev.off()}, add = TRUE)
  graphics::par(mar = c(5, 20, 5, 2))
  draw_case_summary_barplot(case_summary_rows, top_n = top_n)
  invisible(output_png)
}

draw_perspective_pie_chart <- function(count_df, main_title, min_frac_for_internal_count = 0.06) {
  graphics::par(mar = c(2, 2, 5, 2))

  if (nrow(count_df) == 0 || sum(count_df$n) <= 0) {
    graphics::plot.new()
    graphics::text(0.5, 0.55, labels = main_title, cex = 1.2, font = 2)
    graphics::text(0.5, 0.45, labels = "No data available", cex = 1)
    return(invisible(NULL))
  }

  cols <- grDevices::gray.colors(nrow(count_df), start = 0.25, end = 0.85)
  labels <- count_df$whichPerspective
  graphics::pie(
    count_df$n,
    labels = rep(NA, nrow(count_df)),
    col = cols,
    clockwise = TRUE,
    main = main_title,
    cex = 0.95,
    border = "white"
  )
  graphics::legend(
    "topright",
    legend = labels,
    fill = cols,
    bty = "n",
    cex = 0.85
  )
  # graphics::pie() returns NULL; compute slice midpoint angles manually.
  # clockwise=TRUE uses init.angle=90 (degrees = pi/2 radians), going clockwise.
  fracs_all <- count_df$n / sum(count_df$n)
  cum_fracs <- cumsum(fracs_all)
  starts <- c(0, cum_fracs[-length(cum_fracs)])
  mids <- pi / 2 - (starts + cum_fracs) / 2 * 2 * pi

  n_slices <- nrow(count_df)
  text_cex <- if (n_slices > 8) 0.7 else 0.85
  for (i in seq_len(n_slices)) {
    x <- 0.58 * cos(mids[[i]])
    y <- 0.58 * sin(mids[[i]])
    is_transcriptome <- count_df$whichPerspective[[i]] == "TranscriptomePerspective"
    text_col <- if (is_transcriptome) "white" else "black"
    graphics::text(x, y, labels = format(count_df$n[[i]], big.mark = ","), cex = text_cex, col = text_col)
  }

  invisible(mids)
}

write_perspective_pie_chart <- function(count_df, output_png, main_title) {
  grDevices::png(output_png, width = 1800, height = 1800, res = 220)
  op <- graphics::par(no.readonly = TRUE)
  on.exit({graphics::par(op); grDevices::dev.off()}, add = TRUE)
  draw_perspective_pie_chart(count_df, main_title)
  invisible(output_png)
}

write_plot_composite <- function(case_summary_rows,
                                 single_cells_df,
                                 clones_df,
                                 output_png,
                                 top_n = 24L) {
  grDevices::png(output_png, width = 3200, height = 2400, res = 220)
  op <- graphics::par(no.readonly = TRUE)
  on.exit({graphics::par(op); grDevices::dev.off()}, add = TRUE)
  layout(matrix(c(1, 2, 1, 3), nrow = 2, byrow = TRUE), widths = c(1.45, 1), heights = c(1, 1))
  graphics::par(mar = c(5, 20, 5, 2))
  draw_case_summary_barplot(case_summary_rows, top_n = top_n)
  graphics::par(mar = c(2, 2, 5, 2))
  draw_perspective_pie_chart(single_cells_df, "Perspective rows that are not parents of other rows")
  graphics::par(mar = c(2, 2, 5, 2))
  draw_perspective_pie_chart(clones_df, "Perspective rows that are parents of at least one other row")
  invisible(output_png)
}

main <- function() {
  args <- parse_args(commandArgs(trailingOnly = TRUE))
  outdir <- ensure_dir(args$output_dir)

  cfg_info <- read_cloneid_config(args$config)
  mysql_cfg <- effective_mysql_config(cfg_info$config)
  cellseg_cfg <- effective_cellseg_config(cfg_info$config)

  conn <- connect_cloneid_db(mysql_cfg)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  require_table_columns(conn, "CellLinesAndPatients", c("name", "whichType", "source"))
  require_table_columns(conn, "Passaging", c("id", "cellLine", "passaged_from_id1", "passaged_from_id2", "date"))
  require_table_columns(conn, "Perspective", c("origin", "sampleSource", "whichPerspective", "parent"))
  require_table_columns(conn, "Identity", c("sampleSource"))

  passaging_sql <- list(
    total = "SELECT COUNT(*) AS n FROM Passaging",
    roots = "SELECT COUNT(*) AS n FROM Passaging WHERE passaged_from_id1 IS NULL AND passaged_from_id2 IS NULL",
    source_count = "SELECT COUNT(DISTINCT cellLine) AS n FROM Passaging",
    by_type = paste(
      "SELECT c.whichType, COUNT(DISTINCT p.cellLine) AS sample_sources, COUNT(*) AS event_rows",
      "FROM Passaging p LEFT JOIN CellLinesAndPatients c ON c.name = p.cellLine",
      "GROUP BY c.whichType ORDER BY c.whichType"
    ),
    active_sources = "SELECT DISTINCT cellLine AS sample_source FROM Passaging ORDER BY cellLine",
    rooted_source_names = paste(
      "SELECT DISTINCT cellLine AS sample_source FROM Passaging",
      "WHERE passaged_from_id1 IS NULL AND passaged_from_id2 IS NULL ORDER BY cellLine"
    ),
    relationship_edges = "SELECT COUNT(*) AS n FROM Passaging WHERE passaged_from_id1 IS NOT NULL",
    dual_parent = "SELECT COUNT(*) AS n FROM Passaging WHERE passaged_from_id2 IS NOT NULL"
  )

  perspective_sql <- list(
    total = "SELECT COUNT(*) AS n FROM Perspective",
    by_type = paste(
      "SELECT whichPerspective, COUNT(*) AS row_count, COUNT(DISTINCT origin) AS origin_count,",
      "COUNT(DISTINCT sampleSource) AS sample_source_count",
      "FROM Perspective GROUP BY whichPerspective ORDER BY row_count DESC"
    ),
    source_count = "SELECT COUNT(DISTINCT sampleSource) AS n FROM Perspective",
    active_sources = "SELECT DISTINCT sampleSource AS sample_source FROM Perspective ORDER BY sampleSource",
    by_domain = paste(
      "SELECT c.whichType, COUNT(DISTINCT p.sampleSource) AS sample_sources, COUNT(*) AS perspective_rows",
      "FROM Perspective p LEFT JOIN CellLinesAndPatients c ON c.name = p.sampleSource",
      "GROUP BY c.whichType ORDER BY c.whichType"
    )
  )

  identity_sql <- list(
    total = "SELECT COUNT(*) AS n FROM Identity",
    source_count = "SELECT COUNT(DISTINCT sampleSource) AS n FROM Identity",
    active_sources = "SELECT DISTINCT sampleSource AS sample_source FROM Identity ORDER BY sampleSource",
    by_domain = paste(
      "SELECT c.whichType, COUNT(DISTINCT i.sampleSource) AS sample_sources, COUNT(*) AS identity_rows",
      "FROM Identity i LEFT JOIN CellLinesAndPatients c ON c.name = i.sampleSource",
      "GROUP BY c.whichType ORDER BY c.whichType"
    )
  )

  case_summary_rows <- compute_cellline_summary_rows(conn)
  backend_case_count <- nrow(case_summary_rows)
  case_summary_by_type <- if (backend_case_count == 0) {
    data.frame(whichType = character(0), cases = integer(0), stringsAsFactors = FALSE)
  } else {
    stats::aggregate(rootId ~ whichType, case_summary_rows, length)
  }
  if (nrow(case_summary_by_type) > 0) {
    names(case_summary_by_type)[names(case_summary_by_type) == "rootId"] <- "cases"
    case_summary_by_type <- case_summary_by_type[order(case_summary_by_type$whichType), , drop = FALSE]
  }

  perspective_leaf_clone_breakdown <- compute_perspective_leaf_clone_breakdown(conn)


  passaging_total <- as.integer(query_scalar(conn, passaging_sql$total))
  rooted_events <- as.integer(query_scalar(conn, passaging_sql$roots))
  passaging_source_count <- as.integer(query_scalar(conn, passaging_sql$source_count))
  relationship_edges <- as.integer(query_scalar(conn, passaging_sql$relationship_edges))
  dual_parent_edges <- as.integer(query_scalar(conn, passaging_sql$dual_parent))
  passaging_by_type <- query_df(conn, passaging_sql$by_type)
  passaging_sources <- query_df(conn, passaging_sql$active_sources)
  rooted_source_names <- query_df(conn, passaging_sql$rooted_source_names)

  perspective_total <- as.integer(query_scalar(conn, perspective_sql$total))
  perspective_distinct_parent_objects <- as.integer(query_scalar(conn, "SELECT COUNT(DISTINCT parent) AS n FROM Perspective WHERE parent IS NOT NULL"))
  perspective_source_count <- as.integer(query_scalar(conn, perspective_sql$source_count))
  perspective_by_type <- query_df(conn, perspective_sql$by_type)
  perspective_by_domain <- query_df(conn, perspective_sql$by_domain)
  perspective_sources <- query_df(conn, perspective_sql$active_sources)

  identity_total <- as.integer(query_scalar(conn, identity_sql$total))
  identity_source_count <- as.integer(query_scalar(conn, identity_sql$source_count))
  identity_by_domain <- query_df(conn, identity_sql$by_domain)
  identity_sources <- query_df(conn, identity_sql$active_sources)

  passaging_perspective_union <- sort(unique(c(passaging_sources$sample_source, perspective_sources$sample_source)))
  passaging_perspective_union_non_test <- passaging_perspective_union[!grepl("^TEST_", passaging_perspective_union)]
  all_layer_union <- sort(unique(c(passaging_perspective_union, identity_sources$sample_source)))

  input_keys <- list_storage_keys(cellseg_cfg, "input")
  output_image_keys <- list_storage_keys(cellseg_cfg, "images")

  input_df <- data.frame(
    key = input_keys,
    modality_proxy = vapply(input_keys, classify_image_key, character(1)),
    event_id = vapply(input_keys, extract_event_id, character(1)),
    stringsAsFactors = FALSE
  )

  output_df <- data.frame(
    key = output_image_keys,
    modality_proxy = vapply(output_image_keys, classify_image_key, character(1)),
    event_id = vapply(output_image_keys, extract_event_id, character(1)),
    stringsAsFactors = FALSE
  )

  passaging_link <- query_df(
    conn,
    paste(
      "SELECT id AS event_id, cellLine AS sample_source,",
      "CASE WHEN passaged_from_id1 IS NOT NULL OR passaged_from_id2 IS NOT NULL THEN 1 ELSE 0 END AS has_parent_relationship,",
      "passaged_from_id1, passaged_from_id2",
      "FROM Passaging"
    )
  )

  input_join <- merge(input_df, passaging_link, by = "event_id", all.x = TRUE)
  output_join <- merge(output_df, passaging_link, by = "event_id", all.x = TRUE)

  raw_images_linked_to_events <- sum(!is.na(input_join$sample_source))
  raw_images_linked_to_event_relationships <- sum(!is.na(input_join$sample_source) & input_join$has_parent_relationship == 1)
  distinct_events_referenced_by_raw_images <- length(unique(input_join$event_id[!is.na(input_join$sample_source)]))

  manuscript_summary <- list(
    paragraph_event_history = list(
      event_anchored_specimen_records = passaging_total,
      rooted_lineages = rooted_events,
      backend_case_count = backend_case_count,
      raw_images = nrow(input_df),
      derived_image_outputs = nrow(output_df)
    ),
    paragraph_database_coverage = list(
      specimens_and_lineages_primary = passaging_total,
      backend_case_count = backend_case_count,
      multiomics_individual_cells = perspective_total,
      distinct_clone_level_objects = perspective_distinct_parent_objects,
      perspective_types_present = perspective_by_type$whichPerspective,
      identity_rows = identity_total
    ),
    caveats = c(
      "Passaging is the historical table name; manuscript-facing Event counts map most defensibly to Passaging rows and rooted Passaging histories.",
      "The backend-style 'case' count is reproduced from the public DatabaseService.getCellLineSummary() logic: one retained rooted history per cellLine/rootId with pathLen > 1, not simply distinct sample sources.",
      "Perspective row count is used as the manuscript's individual-cell count, per the current multi-omics operationalization.",
      "Distinct clone-level objects are operationalized here as COUNT(DISTINCT Perspective.parent) with NULL parents excluded, per the current manuscript definition.",
      "Image counts are not stored in MySQL tables. They are derived from the production cell-seg durable storage configured alongside the DB. Raw input images are used as the primary image count; output/Images are reported separately as derived image assets.",
      "Digital pathology is not cleanly separable from filenames/storage conventions in the current production image store. This script reports microscopy and medical-imaging proxies plus an unclassified bucket."
    )
  )

  audit <- list(
    config_path = cfg_info$path,
    mysql = list(
      host = mysql_cfg$host,
      port = mysql_cfg$port,
      database = mysql_cfg$database,
      user = mysql_cfg$user
    ),
    cellseg = cellseg_cfg,
    sql_queries = c(passaging_sql, perspective_sql, identity_sql),
    backend_case_summary_rows = case_summary_rows,
    backend_case_summary_by_type = case_summary_by_type,
    active_source_names = list(
      passaging = passaging_sources$sample_source,
      rooted_passaging = rooted_source_names$sample_source,
      perspective = perspective_sources$sample_source,
      identity = identity_sources$sample_source,
      passaging_perspective_union = passaging_perspective_union,
      passaging_perspective_union_excluding_test = passaging_perspective_union_non_test,
      all_layer_union = all_layer_union
    )
  )

  stats <- list(
    passaging_rows = passaging_total,
    rooted_event_histories = rooted_events,
    backend_case_count = backend_case_count,
    passaging_source_count = passaging_source_count,
    passaging_perspective_source_union = length(passaging_perspective_union),
    passaging_perspective_source_union_excluding_test = length(passaging_perspective_union_non_test),
    all_layer_source_union = length(all_layer_union),
    relationship_edges = relationship_edges,
    dual_parent_edges = dual_parent_edges,
    perspective_rows = perspective_total,
    perspective_distinct_parent_objects = perspective_distinct_parent_objects,
    perspective_single_cells_total = perspective_leaf_clone_breakdown$totals$single_cells,
    perspective_clone_rows_total = perspective_leaf_clone_breakdown$totals$clones,
    perspective_single_cells_by_type = perspective_leaf_clone_breakdown$single_cells,
    perspective_clones_by_type = perspective_leaf_clone_breakdown$clones,
    perspective_source_count = perspective_source_count,
    identity_rows = identity_total,
    identity_source_count = identity_source_count,
    raw_image_inputs = nrow(input_df),
    derived_image_outputs = nrow(output_df),
    raw_images_linked_to_events = raw_images_linked_to_events,
    raw_images_linked_to_event_relationships = raw_images_linked_to_event_relationships,
    distinct_events_referenced_by_raw_images = distinct_events_referenced_by_raw_images,
    raw_input_modality_breakdown = table_count_df(input_df$modality_proxy, "category"),
    output_image_modality_breakdown = table_count_df(output_df$modality_proxy, "category"),
    backend_case_summary_rows = case_summary_rows,
    backend_case_summary_by_type = case_summary_by_type,
    passaging_by_type = passaging_by_type,
    perspective_by_type = perspective_by_type,
    perspective_by_domain = perspective_by_domain,
    identity_by_domain = identity_by_domain
  )

  metadata <- list(
    query_timestamp_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
    db_host = mysql_cfg$host,
    db_name = mysql_cfg$database,
    config_path = cfg_info$path
  )

  payload <- list(
    metadata = metadata,
    manuscript_mapping = manuscript_summary,
    stats = stats,
    audit = audit
  )

  json_path <- file.path(outdir, "paragraph_stats.json")
  csv_path <- file.path(outdir, "paragraph_stats_summary.csv")
  case_summary_csv <- file.path(outdir, "backend_case_summary_rows.csv")
  case_summary_by_type_csv <- file.path(outdir, "backend_case_summary_by_type.csv")
  perspective_single_cells_csv <- file.path(outdir, "perspective_single_cells_by_type.csv")
  perspective_clones_csv <- file.path(outdir, "perspective_clones_by_type.csv")
  case_barplot_png <- file.path(outdir, "backend_case_summary_barplot.png")
  perspective_single_cells_pie_png <- file.path(outdir, "perspective_single_cells_pie.png")
  perspective_clones_pie_png <- file.path(outdir, "perspective_clones_pie.png")
  plot_composite_png <- file.path(outdir, "case_barplot_and_pies_composite.png")

  jsonlite::write_json(payload, json_path, pretty = TRUE, auto_unbox = TRUE, null = "null")
  utils::write.csv(build_summary_row(stats, metadata), csv_path, row.names = FALSE)
  utils::write.csv(case_summary_rows, case_summary_csv, row.names = FALSE)
  utils::write.csv(case_summary_by_type, case_summary_by_type_csv, row.names = FALSE)
  utils::write.csv(perspective_leaf_clone_breakdown$single_cells, perspective_single_cells_csv, row.names = FALSE)
  utils::write.csv(perspective_leaf_clone_breakdown$clones, perspective_clones_csv, row.names = FALSE)
  write_case_summary_barplot(case_summary_rows, case_barplot_png, top_n = 24L)
  write_perspective_pie_chart(
    perspective_leaf_clone_breakdown$single_cells,
    perspective_single_cells_pie_png,
    "Perspective rows that are not parents of other rows"
  )
  write_perspective_pie_chart(
    perspective_leaf_clone_breakdown$clones,
    perspective_clones_pie_png,
    "Perspective rows that are parents of at least one other row"
  )
  write_plot_composite(
    case_summary_rows,
    perspective_leaf_clone_breakdown$single_cells,
    perspective_leaf_clone_breakdown$clones,
    plot_composite_png,
    top_n = 24L
  )

  cat("\n=== CLONEID manuscript paragraph support summary ===\n")
  cat("Query timestamp (UTC): ", metadata$query_timestamp_utc, "\n", sep = "")
  cat("Config path: ", metadata$config_path, "\n", sep = "")
  cat("DB host / database: ", metadata$db_host, " / ", metadata$db_name, "\n\n", sep = "")

  cat("Primary operationalization\n")
  cat("  Cases (primary): ", rooted_events, " rooted Passaging histories\n", sep = "")
  cat("  Specimen-like records (primary): ", passaging_total, " Passaging rows\n", sep = "")
  cat("  Lineages (primary): ", rooted_events, " rooted event histories\n", sep = "")
  cat("  Raw stored images (primary): ", nrow(input_df), "\n", sep = "")
  cat("  Derived image outputs (reported separately): ", nrow(output_df), "\n\n", sep = "")

  cat("Active event-source domain proxy (Passaging.cellLine -> CellLinesAndPatients.whichType)\n")
  if (nrow(passaging_by_type) == 0) {
    cat("  - No domain classification rows found.\n\n")
  } else {
    for (i in seq_len(nrow(passaging_by_type))) {
      cat(
        "  - ", passaging_by_type$whichType[[i]] %||% "unclassified", ": ",
        passaging_by_type$sample_sources[[i]], " active source names, ",
        passaging_by_type$event_rows[[i]], " event rows\n",
        sep = ""
      )
    }
    cat("\n")
  }

  cat("Backend case summary by type (matching DatabaseService.getCellLineSummary logic)\n")
  if (nrow(case_summary_by_type) == 0) {
    cat("  - No backend summary rows found.\n\n")
  } else {
    for (i in seq_len(nrow(case_summary_by_type))) {
      cat(
        "  - ", case_summary_by_type$whichType[[i]] %||% "Unknown", ": ",
        case_summary_by_type$cases[[i]], " cases\n",
        sep = ""
      )
    }
    cat("\n")
  }

  cat("Alternative candidate counts for ambiguous manuscript nouns\n")
  cat("  Distinct active Passaging.cellLine sources: ", passaging_source_count, "\n", sep = "")
  cat("  Distinct active Perspective.sampleSource values: ", perspective_source_count, "\n", sep = "")
  cat("  Distinct Passaging ∪ Perspective sample sources: ", length(passaging_perspective_union), "\n", sep = "")
  cat("  Distinct Passaging ∪ Perspective sample sources excluding TEST_*: ", length(passaging_perspective_union_non_test), "\n", sep = "")
  cat("  Distinct Passaging ∪ Perspective ∪ Identity sample sources: ", length(all_layer_union), "\n\n", sep = "")

  cat("Multi-omics paragraph support\n")
  cat("  Perspective rows (all rows): ", perspective_total, "\n", sep = "")
  cat("  Distinct non-NULL Perspective.parent values: ", perspective_distinct_parent_objects, "\n", sep = "")
  cat("  Perspective rows that are not parents of any other row: ", perspective_leaf_clone_breakdown$totals$single_cells, "\n", sep = "")
  cat("  Perspective rows that are parents of at least one other row: ", perspective_leaf_clone_breakdown$totals$clones, "\n", sep = "")
  cat("  Perspective types present: ", paste(perspective_by_type$whichPerspective, collapse = ", "), "\n\n", sep = "")

  cat("Perspective hierarchy breakdown by whichPerspective\n")
  if (nrow(perspective_leaf_clone_breakdown$single_cells) == 0) {
    cat("  - No single-cell leaf rows found.\n")
  } else {
    cat("  Single-cell leaves (not parents):\n")
    for (i in seq_len(nrow(perspective_leaf_clone_breakdown$single_cells))) {
      cat(
        "    - ", perspective_leaf_clone_breakdown$single_cells$whichPerspective[[i]], ": ",
        perspective_leaf_clone_breakdown$single_cells$n[[i]], "\n",
        sep = ""
      )
    }
  }
  if (nrow(perspective_leaf_clone_breakdown$clones) == 0) {
    cat("  - No parent clone rows found.\n\n")
  } else {
    cat("  Clone rows (parents of at least one other row):\n")
    for (i in seq_len(nrow(perspective_leaf_clone_breakdown$clones))) {
      cat(
        "    - ", perspective_leaf_clone_breakdown$clones$whichPerspective[[i]], ": ",
        perspective_leaf_clone_breakdown$clones$n[[i]], "\n",
        sep = ""
      )
    }
    cat("\n")
  }

  cat("Evidence for modality claims\n")
  cat("  Perspective rows: ", perspective_total, "\n", sep = "")
  cat("  Perspective types present: ", paste(perspective_by_type$whichPerspective, collapse = ", "), "\n", sep = "")
  cat("  Raw input image modality proxies:\n")
  for (i in seq_len(nrow(stats$raw_input_modality_breakdown))) {
    cat("    - ", stats$raw_input_modality_breakdown$category[[i]], ": ",
        stats$raw_input_modality_breakdown$n[[i]], "\n", sep = "")
  }
  cat("\n")

  cat("Image linkage back to event/specimen relationships\n")
  cat("  Raw images linked to Passaging rows: ", raw_images_linked_to_events, "\n", sep = "")
  cat("  Raw images linked to Passaging rows with a recorded parent relationship: ",
      raw_images_linked_to_event_relationships, "\n", sep = "")
  cat("  Distinct linked Passaging ids referenced by raw images: ",
      distinct_events_referenced_by_raw_images, "\n", sep = "")
  cat("  Total Passaging parent-child edges in SQL: ", relationship_edges, "\n\n", sep = "")

  cat("Caveats\n")
  for (msg in manuscript_summary$caveats) {
    cat("  - ", msg, "\n", sep = "")
  }

  cat("\nStructured outputs\n")
  cat("  - ", json_path, "\n", sep = "")
  cat("  - ", csv_path, "\n", sep = "")
  cat("  - ", case_summary_csv, "\n", sep = "")
  cat("  - ", case_summary_by_type_csv, "\n", sep = "")
  cat("  - ", perspective_single_cells_csv, "\n", sep = "")
  cat("  - ", perspective_clones_csv, "\n", sep = "")
  cat("  - ", case_barplot_png, "\n", sep = "")
  cat("  - ", perspective_single_cells_pie_png, "\n", sep = "")
  cat("  - ", perspective_clones_pie_png, "\n", sep = "")
  cat("  - ", plot_composite_png, "\n", sep = "")
}

main()

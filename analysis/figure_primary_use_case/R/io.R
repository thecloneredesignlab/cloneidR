cloneid_figure_paths <- function(root_dir,
                                 subtree_rds,
                                 panel_a_zip,
                                 panel_de_zip,
                                 manifest_a_yaml,
                                 manifest_de_yaml,
                                 output_panels_dir,
                                 output_final_dir,
                                 cache_dir) {
  root_dir <- normalizePath(root_dir, winslash = "/", mustWork = TRUE)

  list(
    root_dir = root_dir,
    subtree_rds = file.path(root_dir, subtree_rds),
    panel_a_zip = file.path(root_dir, panel_a_zip),
    panel_de_zip = file.path(root_dir, panel_de_zip),
    manifest_a_yaml = file.path(root_dir, manifest_a_yaml),
    manifest_de_yaml = file.path(root_dir, manifest_de_yaml),
    output_panels_dir = file.path(root_dir, output_panels_dir),
    output_final_dir = file.path(root_dir, output_final_dir),
    cache_dir = file.path(root_dir, cache_dir)
  )
}

cloneid_lineage_palette <- c(
  "SNU-668_C_A24_seed" = "#8c6d31",
  "SNU-668_C_A4_seed" = "#c2a15a",
  "SNU-668_K1_A34_seedT1" = "#355f8d",
  "SNU-668_K2_A34_seedT1" = "#4f86b8",
  "SNU-668_K3_A34_seedT1" = "#79acd8",
  "SNU-668_r1_A55_seedT1" = "#8a3b12",
  "SNU-668_r2_A55_seedT1" = "#bf5b17",
  "SNU-668_r3_A55_seedT1" = "#e08214",
  "K1" = "#355f8d",
  "K2" = "#4f86b8",
  "K3" = "#79acd8",
  "r1" = "#8a3b12",
  "r2" = "#bf5b17",
  "r3" = "#e08214",
  "C_A24" = "#8c6d31",
  "C_A4" = "#c2a15a",
  "control" = "#8c6d31",
  "K" = "#4f86b8",
  "r" = "#bf5b17"
)

cloneid_figure_text_sizes <- list(
  ggplot_base_size = 14,
  axis_title = 14,
  axis_text = 11.5,
  legend_title = 12.5,
  legend_text = 11.5,
  strip_text = 13,
  annotation_text = 4.2,
  panel_a_plot_legend_cex = 0.82,
  panel_a_legend_image_cex = 1.2,
  panel_e_fontsize = 10.5,
  panel_e_fontsize_col = 10.5,
  panel_e_fontsize_row = 10.5,
  panel_label_min_pointsize = 68,
  panel_label_max_pointsize = 68,
  panel_label_size_frac = 0.07,
  panel_label_x_offset_frac = 0.016,
  panel_label_y_offset_frac = 0.01
)

cloneid_figure_theme <- function() {
  ggplot2::theme_bw(base_size = cloneid_figure_text_sizes$ggplot_base_size) +
    ggplot2::theme(
      axis.title = ggplot2::element_text(size = cloneid_figure_text_sizes$axis_title),
      axis.text = ggplot2::element_text(size = cloneid_figure_text_sizes$axis_text),
      legend.title = ggplot2::element_text(size = cloneid_figure_text_sizes$legend_title),
      legend.text = ggplot2::element_text(size = cloneid_figure_text_sizes$legend_text),
      strip.text = ggplot2::element_text(size = cloneid_figure_text_sizes$strip_text),
      panel.grid.minor = ggplot2::element_blank()
    )
}

describe_primary_figure_inputs <- function(paths) {
  data.frame(
    input = c(
      "subtree_rds",
      "panel_a_zip",
      "panel_de_zip",
      "manifest_a_yaml",
      "manifest_de_yaml",
      "output_panels_dir",
      "output_final_dir",
      "cache_dir"
    ),
    path = unlist(paths[c(
      "subtree_rds",
      "panel_a_zip",
      "panel_de_zip",
      "manifest_a_yaml",
      "manifest_de_yaml",
      "output_panels_dir",
      "output_final_dir",
      "cache_dir"
    )], use.names = FALSE),
    exists = c(
      file.exists(paths$subtree_rds),
      file.exists(paths$panel_a_zip),
      file.exists(paths$panel_de_zip),
      file.exists(paths$manifest_a_yaml),
      file.exists(paths$manifest_de_yaml),
      dir.exists(paths$output_panels_dir),
      dir.exists(paths$output_final_dir),
      dir.exists(paths$cache_dir)
    ),
    stringsAsFactors = FALSE
  )
}

read_subtree_bundle <- function(subtree_rds) {
  stopifnot(file.exists(subtree_rds))
  readRDS(subtree_rds)
}

read_bundle_manifest_template <- function(subtree_rds) {
  x <- read_subtree_bundle(subtree_rds)
  yaml::yaml.load(x[["manifest_template"]])
}

summarize_subtree_for_growth <- function(subtree_rds) {
  x <- read_subtree_bundle(subtree_rds)
  p <- x[["tables"]][["Passaging"]]

  bundle_overview <- data.frame(
    metric = c(
      "export_id_count",
      "passaging_rows",
      "passaging_cols",
      "seeding_rows",
      "harvest_rows",
      "perspective_rows"
    ),
    value = c(
      length(x[["export_ids"]]),
      nrow(p),
      ncol(p),
      sum(p[["event"]] == "seeding", na.rm = TRUE),
      sum(p[["event"]] == "harvest", na.rm = TRUE),
      nrow(x[["tables"]][["Perspective"]])
    ),
    stringsAsFactors = FALSE
  )

  list(bundle_overview = bundle_overview)
}

descendant_ids <- function(passaging, root_id) {
  kids <- split(passaging[["id"]], passaging[["passaged_from_id1"]])
  out <- character()
  stack <- root_id
  seen <- character()

  while (length(stack) > 0) {
    cur <- stack[[1]]
    stack <- stack[-1]
    if (cur %in% seen) {
      next
    }
    seen <- c(seen, cur)
    out <- c(out, cur)
    stack <- c(stack, kids[[cur]])
  }

  out
}

empty_yaml_sequence <- function() {
  list()
}

normalize_yaml_sequence <- function(x) {
  if (is.null(x) || length(x) == 0) {
    return(empty_yaml_sequence())
  }
  as.list(as.character(x))
}

write_manifest_if_changed <- function(manifest, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  new_text <- yaml::as.yaml(manifest, indent.mapping.sequence = TRUE)
  if (file.exists(path)) {
    old_text <- paste(readLines(path, warn = FALSE), collapse = "\n")
    if (identical(old_text, new_text)) {
      return(invisible(path))
    }
  }
  writeLines(strsplit(new_text, "\n", fixed = TRUE)[[1]], path, useBytes = TRUE)
  invisible(path)
}

selection_regime_from_id <- function(id) {
  if (grepl("(^|_)C_", id)) {
    return("control")
  }
  if (grepl("(^|_)K[0-9]+_", id)) {
    return("K")
  }
  if (grepl("(^|_)r[0-9]+_", id)) {
    return("r")
  }
  if (grepl("_rK_", id)) {
    return("mixed_root")
  }
  NA_character_
}

replicate_from_id <- function(id) {
  if (grepl("(^|_)C_", id)) {
    return(sub(".*(^|_)(C)_.*", "\\2", id))
  }
  if (grepl("(^|_)K[0-9]+_", id)) {
    return(sub(".*(^|_)(K[0-9]+)_.*", "\\2", id))
  }
  if (grepl("(^|_)r[0-9]+_", id)) {
    return(sub(".*(^|_)(r[0-9]+)_.*", "\\2", id))
  }
  NA_character_
}

passage_from_id <- function(id) {
  hit <- regmatches(id, regexpr("A[0-9]+", id))
  if (length(hit) == 0 || identical(hit, character(0)) || is.na(hit)) {
    return(NA_integer_)
  }
  as.integer(sub("^A", "", hit))
}

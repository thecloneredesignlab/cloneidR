direct_replicate_pattern <- "^SNU-668_(K[123]|r[123])_A[0-9]+_seed(T[0-9]+)?$"
bc_passage_cutoffs <- c(
  K = 33L,
  r = 53L
)

apply_bc_passage_cutoff <- function(df, cutoffs = bc_passage_cutoffs) {
  keep <- !is.na(df$parsed_passage) & df$parsed_passage <= unname(cutoffs[df$regime])
  df[keep, , drop = FALSE]
}

growthfit_like_ranges <- function(passaging) {
  seeds <- passaging[
    grepl(direct_replicate_pattern, passaging$id),
    ,
    drop = FALSE
  ]
  if (nrow(seeds) == 0) {
    return(data.frame(
      first = character(),
      last = character(),
      label = character(),
      label2 = character(),
      stringsAsFactors = FALSE
    ))
  }
  seeds <- seeds[order(seeds$regime, seeds$replicate, seeds$parsed_passage), , drop = FALSE]

  parts <- split(seeds, seeds$replicate) |>
    lapply(function(df) {
      data.frame(
        first = df$id[1],
        last = df$id[nrow(df)],
        label = unique(df$regime),
        label2 = unique(df$replicate),
        stringsAsFactors = FALSE
      )
    })

  out <- do.call(what = rbind, args = parts)
  if (is.null(out)) {
    return(data.frame(
      first = character(),
      last = character(),
      label = character(),
      label2 = character(),
      stringsAsFactors = FALSE
    ))
  }
  as.data.frame(out, stringsAsFactors = FALSE)
}

prepare_bc_passaging <- function(subtree_rds) {
  x <- read_subtree_bundle(subtree_rds)
  p <- x[["tables"]][["Passaging"]]

  p$regime <- vapply(p[["id"]], selection_regime_from_id, character(1))
  p$replicate <- vapply(p[["id"]], replicate_from_id, character(1))
  p$parsed_passage <- vapply(p[["id"]], passage_from_id, integer(1))
  p$date <- as.Date(p$date)
  p$event <- as.character(p$event)
  p$passaged_from_id1 <- as.character(p$passaged_from_id1)
  p$cellCount <- suppressWarnings(as.numeric(p$cellCount))
  p$correctedCount <- suppressWarnings(as.numeric(p$correctedCount))
  p <- apply_bc_passage_cutoff(p)
  p <- p[order(p$regime, p$replicate, p$parsed_passage, p$date, p$event), , drop = FALSE]
  rownames(p) <- NULL
  p
}

recover_bundle_lineage <- function(first_id, last_id, passaging) {
  current_id <- last_id
  lineage <- current_id

  while (!is.na(current_id) && current_id != first_id) {
    idx <- match(current_id, passaging$id)
    if (is.na(idx)) {
      stop(sprintf("Could not locate lineage node '%s' in passaging table.", current_id))
    }
    parent <- passaging$passaged_from_id1[[idx]]
    if (length(parent) == 0 || is.na(parent) || !nzchar(parent)) {
      stop(sprintf("Could not trace from '%s' back to '%s'.", last_id, first_id))
    }
    lineage <- c(parent, lineage)
    current_id <- parent
  }

  if (!identical(current_id, first_id)) {
    stop(sprintf("Could not recover lineage from '%s' to '%s'.", first_id, last_id))
  }

  lineage[passaging$event[match(lineage, passaging$id)] == "seeding"]
}

fit_growth_curve_for_seed <- function(df) {
  df <- df[order(df$date, df$event), , drop = FALSE]
  df$passage_time <- as.numeric(df$date - min(df$date, na.rm = TRUE))
  df$pred <- NA_real_
  df$g <- NA_real_
  df$intercept <- NA_real_
  df$fit_r2 <- NA_real_

  if (nrow(df) < 2 || all(is.na(df$correctedCount))) {
    return(df)
  }

  fit <- stats::lm(log(pmax(1, df$correctedCount)) ~ passage_time, data = df)
  df$pred <- exp(stats::predict(fit, newdata = df))
  df$g <- unname(stats::coef(fit)[["passage_time"]])
  df$intercept <- exp(unname(stats::coef(fit)[["(Intercept)"]]))
  df$fit_r2 <- summary(fit)$r.squared
  df
}

derive_growthfit_like_dataset <- function(subtree_rds) {
  passaging <- prepare_bc_passaging(subtree_rds)
  ranges <- growthfit_like_ranges(passaging)
  if (!is.data.frame(ranges) || nrow(ranges) == 0) {
    stop("No direct K/r replicate seeding ranges were found in the subtree bundle.")
  }

  per_replicate <- lapply(seq_len(nrow(ranges)), function(i) {
    row <- ranges[i, , drop = FALSE]
    seeds <- recover_bundle_lineage(
      first_id = row$first,
      last_id = row$last,
      passaging = passaging
    )

    seed_groups <- lapply(seq_along(seeds), function(j) {
      seed_id <- seeds[[j]]
      dfi <- passaging[
        passaging$id %in% seed_id | passaging$passaged_from_id1 %in% seed_id,
        ,
        drop = FALSE
      ]
      dfi <- dfi[dfi$event == "harvest", , drop = FALSE]
      if (nrow(dfi) < 2) {
        return(NULL)
      }
      dfi$adjPass <- j
      dfi$label_value <- row$label
      dfi$sublabel_value <- row$label2
      dfi$passage_id <- seed_id
      fit_growth_curve_for_seed(dfi)
    })

    seed_groups <- Filter(Negate(is.null), seed_groups)
    if (length(seed_groups) == 0) {
      return(NULL)
    }
    do.call(rbind, seed_groups)
  })

  per_replicate <- Filter(Negate(is.null), per_replicate)
  if (length(per_replicate) == 0) {
    stop("No harvest-only passages with at least two datapoints were found in the subtree bundle.")
  }
  df <- do.call(rbind, per_replicate)
  df <- df[!is.na(df$date), , drop = FALSE]
  df$days_after_seeding <- df$passage_time
  df
}

choose_panel_c_passages <- function(growth_df, per_regime = 6L) {
  passages <- unique(growth_df[, c("regime", "parsed_passage"), drop = FALSE])
  split_passages <- split(passages$parsed_passage, passages$regime)
  common_vals <- Reduce(
    intersect,
    lapply(split_passages[c("K", "r")], function(x) sort(unique(stats::na.omit(x))))
  )
  common_vals <- sort(unique(common_vals))
  if (length(common_vals) == 0) {
    stop("No parsed passages are shared between K and r lineages for panel B.")
  }

  if (length(common_vals) <= per_regime) {
    picked <- common_vals
  } else {
    idx <- unique(round(seq(1, length(common_vals), length.out = per_regime)))
    idx[1] <- 1
    idx[length(idx)] <- length(common_vals)
    picked <- common_vals[idx]
  }

  do.call(rbind, lapply(c("K", "r"), function(regime) {
    data.frame(regime = regime, parsed_passage = picked, stringsAsFactors = FALSE)
  }))
}

build_panel_c_data <- function(subtree_rds, passages_per_regime = 6L) {
  growth_df <- derive_growthfit_like_dataset(subtree_rds)
  selected <- choose_panel_c_passages(growth_df, per_regime = passages_per_regime)
  passage_rows <- unique(growth_df[, c("regime", "replicate", "parsed_passage", "passage_id"), drop = FALSE])
  keep_ids <- merge(passage_rows, selected, by = c("regime", "parsed_passage"))

  panel_df <- growth_df[growth_df$passage_id %in% keep_ids$passage_id, , drop = FALSE]
  panel_df$replicate <- factor(panel_df$replicate, levels = c("K1", "K2", "K3", "r1", "r2", "r3"))
  panel_df$regime <- factor(panel_df$regime, levels = c("K", "r"))
  panel_df$parsed_passage <- factor(panel_df$parsed_passage, levels = sort(unique(panel_df$parsed_passage)))

  list(
    growth_df = growth_df,
    selected_passages = selected,
    panel_df = panel_df
  )
}

summarize_d_metrics <- function(growth_df) {
  split(growth_df, growth_df$passage_id) |>
    lapply(function(df) {
      harvest_rows <- df[df$event == "harvest", , drop = FALSE]
      harvest_rows <- harvest_rows[order(harvest_rows$date), , drop = FALSE]
      first_row <- df[order(df$date, df$event), , drop = FALSE][1, , drop = FALSE]

      data.frame(
        passage_id = first_row$passage_id[[1]],
        regime = first_row$regime[[1]],
        replicate = first_row$replicate[[1]],
        parsed_passage = first_row$parsed_passage[[1]],
        seed_date = first_row$date[[1]],
        growth_rate = unique(stats::na.omit(df$g))[1],
        fit_r2 = unique(stats::na.omit(df$fit_r2))[1],
        max_population_size = if (nrow(harvest_rows) > 0) {
          harvest_rows$correctedCount[[nrow(harvest_rows)]]
        } else {
          NA_real_
        },
        stringsAsFactors = FALSE
      )
    }) |>
    do.call(what = rbind)
}

write_growthfit_like_json <- function(subtree_rds, output_json, panel_c_data = NULL) {
  if (is.null(panel_c_data)) {
    panel_c_data <- build_panel_c_data(subtree_rds = subtree_rds)
  }

  passaging <- prepare_bc_passaging(subtree_rds)
  ranges <- growthfit_like_ranges(passaging)
  if (!is.data.frame(ranges)) {
    stop("growthfit_like_ranges() did not return a data frame.")
  }

  payload <- list(
    source = basename(subtree_rds),
    generated_at = format(Sys.time(), tz = "UTC", usetz = TRUE),
    filters = list(
      list(
        ranges = lapply(seq_len(nrow(ranges)), function(i) {
          list(
            first = ranges$first[[i]],
            last = ranges$last[[i]],
            label = ranges$label[[i]],
            label2 = ranges$label2[[i]]
          )
        })
      )
    ),
    panel_c_selected_passages = split(
      panel_c_data$selected_passages$parsed_passage,
      panel_c_data$selected_passages$regime
    )
  )

  dir.create(dirname(output_json), recursive = TRUE, showWarnings = FALSE)
  jsonlite::write_json(payload, output_json, pretty = TRUE, auto_unbox = TRUE)
  invisible(output_json)
}

plot_panel_c <- function(panel_c_data) {
  df <- panel_c_data$panel_df
  replicate_colors <- cloneid_lineage_palette[c("K1", "K2", "K3", "r1", "r2", "r3")]
  ggplot2::ggplot(df, ggplot2::aes(
    x = days_after_seeding,
    y = correctedCount,
    color = replicate
  )) +
    ggplot2::geom_point(size = 1.8, alpha = 0.8) +
    ggplot2::geom_line(ggplot2::aes(y = pred, group = passage_id), linewidth = 0.5) +
    # ggplot2::facet_grid(regime ~ parsed_passage, scales = "free_y") +
    ggplot2::facet_grid(regime ~ parsed_passage, scales = "free_y",
      labeller = ggplot2::labeller(
        parsed_passage = function(x) paste("Passage", x)
      )
    ) +
    ggplot2::scale_color_manual(values = replicate_colors, drop = FALSE) +
    ggplot2::scale_y_continuous(labels = scales::label_number(big.mark = ",")) +
  ggplot2::labs(
      x = "Days after seeding",
      y = "Corrected cell count",
      color = "Replicate"
    ) +
    cloneid_figure_theme() +
    ggplot2::theme(
      strip.background = ggplot2::element_rect(fill = "grey95")
    )
}

panel_d_trend_stats <- function(df, value_col) {
  keep <- df[, c("parsed_passage", value_col), drop = FALSE]
  keep <- keep[stats::complete.cases(keep), , drop = FALSE]
  if (nrow(keep) < 3 || length(unique(keep$parsed_passage)) < 2) {
    return(list(
      slope = NA_real_,
      p_value = NA_real_,
      rho = NA_real_,
      label = "slope = NA\np = NA\nrho = NA"
    ))
  }

  fit <- stats::lm(stats::as.formula(sprintf("%s ~ parsed_passage", value_col)), data = keep)
  coefs <- summary(fit)$coefficients
  slope <- unname(coefs["parsed_passage", "Estimate"])
  p_value <- unname(coefs["parsed_passage", "Pr(>|t|)"])
  rho <- suppressWarnings(stats::cor(
    keep$parsed_passage,
    keep[[value_col]],
    method = "pearson",
    use = "complete.obs"
  ))

  list(
    slope = slope,
    p_value = p_value,
    rho = rho,
    label = sprintf("slope = %.3g\np = %.3g\nrho = %.3g", slope, p_value, rho)
  )
}

panel_d_x_breaks <- function(parsed_passages, n = 5L) {
  vals <- sort(unique(stats::na.omit(parsed_passages)))
  if (length(vals) <= n) {
    return(vals)
  }
  idx <- unique(round(seq(1, length(vals), length.out = n)))
  idx[1] <- 1L
  idx[length(idx)] <- length(vals)
  vals[idx]
}

plot_single_c_metric <- function(metrics, regime, value_col, y_lab,
                                 x_limits = NULL, y_limits = NULL,
                                 show_x = TRUE, show_y = TRUE) {
  df <- metrics[metrics$regime == regime, , drop = FALSE]
  stats_label <- panel_d_trend_stats(df, value_col)
  replicate_levels <- if (identical(regime, "K")) c("K1", "K2", "K3") else c("r1", "r2", "r3")
  replicate_colors <- cloneid_lineage_palette[replicate_levels]
  p <- ggplot2::ggplot(df, ggplot2::aes(
    x = parsed_passage,
    y = .data[[value_col]],
    color = replicate
  )) +
    ggplot2::geom_point(size = 2, alpha = 0.9) +
    ggplot2::geom_smooth(
      data = df,
      mapping = ggplot2::aes(x = parsed_passage, y = .data[[value_col]], group = 1),
      inherit.aes = FALSE,
      method = "lm",
      se = FALSE,
      color = "black",
      linewidth = 0.8
    ) +
    ggplot2::scale_color_manual(values = replicate_colors, drop = FALSE) +
    ggplot2::scale_y_continuous(labels = scales::label_number(big.mark = ",")) +
    ggplot2::labs(
      x = "Passage number",
      y = y_lab,
      color = "Replicate"
    ) +
    ggplot2::annotate(
      "text",
      x = Inf,
      y = Inf,
      label = stats_label$label,
      hjust = 1.05,
      vjust = 1.1,
      size = cloneid_figure_text_sizes$annotation_text
    ) +
    cloneid_figure_theme()

  if (!is.null(x_limits) || !is.null(y_limits)) {
    p <- p + ggplot2::coord_cartesian(xlim = x_limits, ylim = y_limits)
  }
  if (!show_x) {
    p <- p + ggplot2::theme(
      axis.title.x = ggplot2::element_blank(),
      axis.text.x = ggplot2::element_blank(),
      axis.ticks.x = ggplot2::element_blank()
    )
  }
  if (!show_y) {
    p <- p + ggplot2::theme(
      axis.title.y = ggplot2::element_blank(),
      axis.text.y = ggplot2::element_blank(),
      axis.ticks.y = ggplot2::element_blank()
    )
  }
  p
}

plot_panel_d <- function(metrics) {
  c1 <- plot_single_c_metric(
    metrics = metrics,
    regime = "r",
    value_col = "growth_rate",
    y_lab = "Growth rate (day^-1)"
  )
  c2 <- plot_single_c_metric(
    metrics = metrics,
    regime = "K",
    value_col = "growth_rate",
    y_lab = "Growth rate (day^-1)"
  )
  c3 <- plot_single_c_metric(
    metrics = metrics,
    regime = "r",
    value_col = "max_population_size",
    y_lab = "Max observed cell count"
  )
  c4 <- plot_single_c_metric(
    metrics = metrics,
    regime = "K",
    value_col = "max_population_size",
    y_lab = "Max observed cell count"
  )

  combined_metrics <- rbind(
    data.frame(
      metrics,
      panel_row = "Growth rate (day^-1)",
      panel_value = metrics$growth_rate,
      stringsAsFactors = FALSE
    ),
    data.frame(
      metrics,
      panel_row = "Max observed cell count",
      panel_value = metrics$max_population_size,
      stringsAsFactors = FALSE
    )
  )
  combined_metrics$regime <- factor(combined_metrics$regime, levels = c("r", "K"))
  combined_metrics$panel_row <- factor(
    combined_metrics$panel_row,
    levels = c("Growth rate (day^-1)", "Max observed cell count")
  )

  combined_stats <- do.call(rbind, lapply(
    split(combined_metrics, list(combined_metrics$panel_row, combined_metrics$regime), drop = TRUE),
    function(df) {
      if (nrow(df) == 0) {
        return(NULL)
      }
      value_col <- if (identical(as.character(unique(df$panel_row)), "Growth rate (day^-1)")) "growth_rate" else "max_population_size"
      stats_label <- panel_d_trend_stats(df, value_col)
      data.frame(
        panel_row = unique(df$panel_row),
        regime = unique(df$regime),
        label = stats_label$label,
        stringsAsFactors = FALSE
      )
    }
  ))

  combined <- ggplot2::ggplot(
    combined_metrics,
    ggplot2::aes(x = parsed_passage, y = panel_value, color = replicate)
  ) +
    ggplot2::geom_point(size = 2, alpha = 0.9) +
    ggplot2::geom_smooth(
      data = combined_metrics,
      mapping = ggplot2::aes(x = parsed_passage, y = panel_value, group = interaction(panel_row, regime)),
      inherit.aes = FALSE,
      method = "lm",
      se = FALSE,
      color = "black",
      linewidth = 0.8
    ) +
    ggplot2::facet_grid(panel_row ~ regime, scales = "free") +
    ggplot2::scale_color_manual(values = cloneid_lineage_palette[c("K1", "K2", "K3", "r1", "r2", "r3")], drop = FALSE) +
    ggplot2::scale_x_continuous(
      breaks = panel_d_x_breaks(combined_metrics$parsed_passage)
    ) +
    ggplot2::scale_y_continuous(labels = scales::label_number(big.mark = ",")) +
    ggplot2::labs(
      x = "Passage number",
      y = NULL,
      color = "Replicate"
    ) +
    ggplot2::geom_text(
      data = combined_stats,
      mapping = ggplot2::aes(x = Inf, y = Inf, label = label),
      inherit.aes = FALSE,
      hjust = 1.05,
      vjust = 1.1,
      size = cloneid_figure_text_sizes$annotation_text
    ) +
    cloneid_figure_theme()

  list(c1 = c1, c2 = c2, c3 = c3, c4 = c4, combined = combined)
}

draw_panel_d_combined <- function(panel_d_plots) {
  print(panel_d_plots$combined)
  invisible(panel_d_plots$combined)
}

save_panel_d_plots <- function(panel_d_plots, output_panels_dir) {
  panel_files <- c(
    c1 = file.path(output_panels_dir, "panel_D1_growth_rate_r.png"),
    c2 = file.path(output_panels_dir, "panel_D2_growth_rate_K.png"),
    c3 = file.path(output_panels_dir, "panel_D3_max_population_r.png"),
    c4 = file.path(output_panels_dir, "panel_D4_max_population_K.png"),
    combined = file.path(output_panels_dir, "panel_D_trends.png")
  )

  for (nm in c("c1", "c2", "c3", "c4")) {
    ggplot2::ggsave(panel_files[[nm]], plot = panel_d_plots[[nm]], width = 6, height = 5, dpi = 300)
  }
  grDevices::png(panel_files[["combined"]], width = 3600, height = 3000, res = 300)
  draw_panel_d_combined(panel_d_plots)
  grDevices::dev.off()

  panel_files
}

save_cd_outputs <- function(subtree_rds, output_panels_dir, cache_dir, passages_per_regime = 6L) {
  panel_c_data <- build_panel_c_data(
    subtree_rds = subtree_rds,
    passages_per_regime = passages_per_regime
  )
  metrics <- summarize_d_metrics(panel_c_data$growth_df)
  used_harvest_rows <- unique(
    panel_c_data$growth_df[, c("passage_id", "id", "date", "correctedCount"), drop = FALSE]
  )
  used_harvest_rows <- used_harvest_rows[order(
    used_harvest_rows$passage_id,
    used_harvest_rows$date,
    used_harvest_rows$id
  ), , drop = FALSE]
  used_harvest_ids <- data.frame(
    id = sort(unique(used_harvest_rows$id)),
    stringsAsFactors = FALSE
  )

  dir.create(output_panels_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)

  panel_c_plot <- plot_panel_c(panel_c_data)
  panel_d_plots <- plot_panel_d(metrics)
  panel_c_file <- file.path(output_panels_dir, "panel_C_growth_curves.png")
  metrics_csv <- file.path(cache_dir, "panel_d_metrics.csv")
  used_harvest_rows_csv <- file.path(cache_dir, "panel_bc_used_harvest_rows.csv")
  used_harvest_ids_txt <- file.path(cache_dir, "panel_bc_used_harvest_ids.txt")
  growth_json <- file.path(cache_dir, "panel_c_growthfit_like_config.json")

  ggplot2::ggsave(panel_c_file, plot = panel_c_plot, width = 14, height = 6.5, dpi = 300)
  panel_d_files <- save_panel_d_plots(panel_d_plots, output_panels_dir)
  utils::write.csv(metrics, metrics_csv, row.names = FALSE)
  utils::write.csv(used_harvest_rows, used_harvest_rows_csv, row.names = FALSE)
  writeLines(used_harvest_ids$id, used_harvest_ids_txt, useBytes = TRUE)
  write_growthfit_like_json(subtree_rds, growth_json, panel_c_data = panel_c_data)

  list(
    panel_c_data = panel_c_data,
    metrics = metrics,
    panel_d_plots = panel_d_plots,
    files = data.frame(
      artifact = c(
        "panel_C_png",
        "panel_D1_png",
        "panel_D2_png",
        "panel_D3_png",
        "panel_D4_png",
        "panel_D_png",
        "panel_D_metrics_csv",
        "panel_BC_used_harvest_rows_csv",
        "panel_BC_used_harvest_ids_txt",
        "panel_C_growthfit_like_json"
      ),
      path = c(
        panel_c_file,
        unname(panel_d_files[["c1"]]),
        unname(panel_d_files[["c2"]]),
        unname(panel_d_files[["c3"]]),
        unname(panel_d_files[["c4"]]),
        unname(panel_d_files[["combined"]]),
        metrics_csv,
        used_harvest_rows_csv,
        used_harvest_ids_txt,
        growth_json
      ),
      stringsAsFactors = FALSE
    )
  )
}

inspect_bc_inputs <- function(subtree_rds) {
  parsed <- prepare_bc_passaging(subtree_rds)
  parsed <- parsed[grepl(direct_replicate_pattern, parsed$id), , drop = FALSE]
  summary <- data.frame(
    metric = c(
      "rows_in_direct_replicates",
      "distinct_regimes",
      "distinct_replicates",
      "harvest_rows",
      "min_parsed_passage",
      "max_parsed_passage"
    ),
    value = c(
      nrow(parsed),
      length(unique(stats::na.omit(parsed$regime))),
      length(unique(stats::na.omit(parsed$replicate))),
      sum(parsed$event == "harvest", na.rm = TRUE),
      min(parsed$parsed_passage, na.rm = TRUE),
      max(parsed$parsed_passage, na.rm = TRUE)
    ),
    stringsAsFactors = FALSE
  )

  list(parsed = parsed, summary = summary)
}

panel_c_status <- function() {
  data.frame(
    panel = "C",
    status = "rendered",
    note = "Saved panel C plot and growthfit-like JSON from the subtree bundle.",
    stringsAsFactors = FALSE
  )
}

panel_d_status <- function() {
  data.frame(
    panel = "D1-D4",
    status = "rendered",
    note = "Saved the four panel D trend subplots and per-passage metrics.",
    stringsAsFactors = FALSE
  )
}

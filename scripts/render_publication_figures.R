#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(jsonlite)
  library(ggplot2)
  library(patchwork)
  library(svglite)
  library(ragg)
  library(ggrepel)
  library(scales)
  library(dplyr)
  library(tidyr)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop("Usage: render_publication_figures.R <input_json> <output_dir>")
}

input_path <- args[[1]]
output_dir <- args[[2]]
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

payload <- fromJSON(input_path, simplifyVector = FALSE)
series <- payload$series

stage_palette <- c(
  "Diagnosed" = "#6ee0c0",
  "On ART" = "#5b9cf5",
  "Suppressed" = "#00d4aa"
)

positive_color <- "#2d9cdb"
negative_color <- "#e63946"
grid_color <- "#1e2f4c"
bg_color <- "transparent"
panel_color <- "transparent"

quarter_to_decimal <- function(period) {
  matches <- regmatches(period, regexec("^(\\d{4}) Q([1-4])$", period))[[1]]
  if (length(matches) == 3) {
    return(as.numeric(matches[2]) + (as.numeric(matches[3]) - 1) / 4)
  }
  suppressWarnings(as.numeric(period))
}

save_plot_assets <- function(plot, basename, width = 13, height = 8) {
  svg_path <- file.path(output_dir, paste0(basename, ".svg"))
  pdf_path <- file.path(output_dir, paste0(basename, ".pdf"))
  png_path <- file.path(output_dir, paste0(basename, ".png"))

  svglite::svglite(svg_path, width = width, height = height, bg = "transparent")
  print(plot)
  dev.off()

  grDevices::pdf(pdf_path, width = width, height = height, useDingbats = FALSE, bg = "white")
  print(plot)
  dev.off()

  ragg::agg_png(png_path, width = width * 180, height = height * 180, units = "px", res = 180, background = "#0c1629")
  print(plot)
  dev.off()
}

epi_theme <- function(base_size = 13) {
  theme_minimal(base_size = base_size, base_family = "sans") +
    theme(
      plot.background = element_rect(fill = "transparent", colour = NA),
      panel.background = element_blank(),
      panel.grid.major = element_line(colour = grid_color, linewidth = 0.5),
      panel.grid.minor = element_blank(),
      axis.title = element_text(face = "bold", colour = "#e0e6f0"),
      axis.text = element_text(colour = "#7889a0"),
      plot.title = element_text(face = "bold", family = "serif", size = base_size + 6, colour = "#ffffff"),
      plot.subtitle = element_text(size = base_size + 1, colour = "#a0b0c0", margin = margin(b = 10)),
      plot.caption = element_text(size = base_size - 2, colour = "#7889a0"),
      strip.text = element_text(face = "bold", family = "serif", size = base_size + 1, colour = "#e0e6f0"),
      legend.position = "top",
      legend.title = element_blank(),
      legend.text = element_text(size = base_size),
      plot.margin = margin(10, 12, 8, 12)
    )
}

compact_theme <- function(base_size = 12) {
  epi_theme(base_size) +
    theme(
      plot.title = element_text(size = base_size + 3),
      plot.subtitle = element_text(size = base_size),
      axis.text.x = element_text(size = base_size - 1),
      axis.text.y = element_text(size = base_size - 1)
    )
}

build_national_cascade <- function(data) {
  stage_map <- c(
    "first_95" = "Diagnosed",
    "second_95" = "On ART",
    "third_95" = "Suppressed"
  )

  annual <- bind_rows(lapply(data$rows, function(row) {
    if (length(row$official_annual) == 0) return(NULL)
    tibble(
      stage = stage_map[[row$series_id]],
      x = as.numeric(vapply(row$official_annual, `[[`, 0, "year")),
      value = as.numeric(vapply(row$official_annual, `[[`, 0, "value"))
    )
  }))
  annual$stage <- factor(annual$stage, levels = c("Diagnosed", "On ART", "Suppressed"))

  metrics <- tibble(
    stage = factor(stage_map[vapply(data$rows, `[[`, "", "series_id")], levels = c("Diagnosed", "On ART", "Suppressed")),
    latest_value = as.numeric(vapply(data$rows, `[[`, 0, "latest_value")),
    latest_period = vapply(data$rows, `[[`, "", "latest_period"),
    gap = as.numeric(vapply(data$rows, `[[`, 0, "gap_to_target"))
  )

  latest_counts <- setNames(
    lapply(data$rows, function(row) {
      points <- row$count_points
      if (length(points) == 0) return(NA_real_)
      as.numeric(points[[length(points)]][["value"]])
    }),
    vapply(data$rows, `[[`, "", "series_id")
  )

  annual_estimated <- NULL
  if (!is.null(data$estimated_points) && length(data$estimated_points)) {
    annual_estimated <- as.numeric(data$estimated_points[[length(data$estimated_points)]][["value"]])
  }
  estimated_plhiv <- if (!is.null(annual_estimated) && is.finite(annual_estimated)) {
    annual_estimated
  } else {
    latest_counts[["first_95"]] / (metrics$latest_value[metrics$stage == "Diagnosed"] / 100)
  }
  diagnosed <- latest_counts[["first_95"]]
  on_art <- latest_counts[["second_95"]]
  suppressed <- latest_counts[["third_95"]]

  bullet_df <- metrics %>%
    mutate(
      anchor = stage,
      target = 95,
      card_value = sprintf("%.0f%%", latest_value),
      context_label = sprintf("%s · gap %.0f pts", latest_period, gap),
      card_xmin = 0,
      card_xmax = 100
    )

  bullet <- ggplot(bullet_df, aes(y = anchor)) +
    geom_segment(aes(x = 0, xend = 100, yend = anchor), linewidth = 9.5, colour = "#101d33", lineend = "round") +
    geom_segment(aes(x = 0, xend = 95, yend = anchor), linewidth = 9.5, colour = "#2d9cdb44", lineend = "round") +
    geom_segment(aes(x = 0, xend = latest_value, yend = anchor, colour = stage), linewidth = 9.5, lineend = "round") +
    geom_point(aes(x = latest_value, fill = stage), shape = 21, size = 6.2, stroke = 1.25, colour = "#0c1629") +
    geom_vline(xintercept = 95, linetype = "22", linewidth = 0.7, colour = "#f0a030") +
    geom_text(aes(x = 2, y = anchor, label = card_value, colour = stage), hjust = 0, vjust = 1.55, size = 7.2, fontface = "bold", family = "serif") +
    geom_text(aes(x = 2, y = anchor, label = context_label), hjust = 0, vjust = -0.15, size = 3.55, colour = "#8f9ba8") +
    annotate("text", x = 95, y = 3.45, label = "95 target", hjust = 1.05, size = 3.5, colour = "#f0a030", fontface = "bold") +
    facet_wrap(~stage, nrow = 1) +
    scale_colour_manual(values = stage_palette) +
    scale_fill_manual(values = stage_palette) +
    scale_x_continuous(
      limits = c(0, 100),
      breaks = c(0, 50, 95, 100),
      labels = c("0%", "50%", "95%", "100%")
    ) +
    scale_y_discrete(NULL) +
    labs(x = NULL, y = NULL) +
    compact_theme(12) +
    theme(
      legend.position = "none",
      strip.background = element_blank(),
      strip.text = element_text(size = 16, face = "bold", family = "serif"),
      panel.grid = element_blank(),
      axis.text.y = element_blank(),
      panel.grid.minor = element_blank(),
      axis.text.x = element_text(size = 10, colour = "#7889a0"),
      axis.ticks.y = element_blank()
    )

  latest_annual <- annual %>%
    group_by(stage) %>%
    arrange(desc(x)) %>%
    slice_head(n = 1) %>%
    ungroup()

  latest_annual$label <- c("Diagnosed", "On ART", "Suppressed")

  timeline <- ggplot() +
    geom_hline(yintercept = 95, linetype = "22", linewidth = 0.6, colour = "#f0a030") +
    geom_line(data = annual, aes(x = x, y = value, colour = stage, group = stage), linewidth = 2.5, na.rm = TRUE) +
    geom_point(data = annual, aes(x = x, y = value, fill = stage), shape = 21, size = 3.8, stroke = 1.0, colour = "#0c1629", na.rm = TRUE) +
    ggrepel::geom_text_repel(
      data = latest_annual,
      aes(x = x, y = value, label = sprintf("%s  %.0f%%", label, value), colour = stage),
      direction = "y",
      hjust = 0,
      nudge_x = 0.28,
      size = 4.0,
      fontface = "bold",
      min.segment.length = 0,
      box.padding = 0.28,
      max.overlaps = 20,
      show.legend = FALSE
    ) +
    scale_colour_manual(values = stage_palette) +
    scale_fill_manual(values = stage_palette) +
    scale_x_continuous(
      limits = c(2018, 2025.55),
      breaks = 2018:2025,
      labels = as.character(2018:2025)
    ) +
    scale_y_continuous(limits = c(0, 100), labels = label_percent(scale = 1), breaks = c(0, 25, 50, 75, 95, 100)) +
    labs(
      title = "Observed year-end trajectory",
      x = NULL,
      y = "Coverage"
    ) +
    epi_theme(12) +
    theme(legend.position = "none") +
    guides(fill = "none", colour = "none")

  count_df <- tibble(
    stage = factor(
      c("Estimated PLHIV", "Diagnosed PLHIV", "PLHIV on ART", "Virally suppressed"),
      levels = c("Estimated PLHIV", "Diagnosed PLHIV", "PLHIV on ART", "Virally suppressed")
    ),
    value = c(estimated_plhiv, diagnosed, on_art, suppressed),
    pct = c(100, diagnosed / estimated_plhiv * 100, on_art / estimated_plhiv * 100, suppressed / estimated_plhiv * 100),
    fill_key = c("Estimated PLHIV", "Diagnosed", "On ART", "Suppressed"),
    label = c(
      label_number(scale_cut = cut_short_scale())(estimated_plhiv),
      paste0(label_number(scale_cut = cut_short_scale())(diagnosed), " · ", round(diagnosed / estimated_plhiv * 100), "%"),
      paste0(label_number(scale_cut = cut_short_scale())(on_art), " · ", round(on_art / estimated_plhiv * 100), "%"),
      paste0(label_number(scale_cut = cut_short_scale())(suppressed), " · ", round(suppressed / estimated_plhiv * 100), "%")
    )
  )

  count_plot <- ggplot(count_df, aes(x = value, y = stage, fill = fill_key)) +
    geom_col(width = 0.62, colour = "#0c1629", linewidth = 0.9) +
    geom_text(aes(label = label), hjust = -0.06, size = 3.6, colour = "#e0e6f0", fontface = "bold") +
    scale_fill_manual(values = c(
      "Estimated PLHIV" = "#435d8a",
      "Diagnosed" = "#61d85a",
      "On ART" = "#2d9cdb",
      "Suppressed" = "#00d4aa"
    )) +
    scale_x_continuous(labels = label_number(scale_cut = cut_short_scale()), expand = expansion(mult = c(0, 0.18))) +
    labs(
      title = "2025 cascade stage counts",
      x = "People",
      y = NULL
    ) +
    compact_theme(11.5) +
    theme(
      legend.position = "none",
      axis.text.y = element_text(face = "bold")
    )

    top_row <- bullet
    bottom_row <- timeline | count_plot + plot_layout(widths = c(2.55, 1.02))
    (top_row / bottom_row) + plot_layout(heights = c(0.64, 1.36)) &
      theme(plot.background = element_rect(fill = "transparent", colour = NA), panel.background = element_blank())
}

build_regional_matrix <- function(data) {
  rows <- bind_rows(lapply(data$rows, as_tibble))
  if (!nrow(rows)) return(NULL)

  rows <- rows %>%
    mutate(region = factor(region, levels = rev(region)))

  build_column <- function(stage_name, col_name, high_color) {
    col_df <- rows %>%
      select(region, value = !!sym(col_name)) %>%
      mutate(stage = stage_name)

    ggplot(col_df, aes(x = stage, y = region, fill = value)) +
      geom_tile(colour = "#101d33", linewidth = 0.7, width = 0.98, height = 0.92) +
      geom_text(aes(label = sprintf("%.0f%%", value)), size = 3.6, colour = "#ffffff", fontface = "bold") +
      scale_fill_gradient(low = "#1a2436", high = high_color, limits = c(30, 100)) +
      labs(x = NULL, y = NULL) +
      epi_theme(11) +
      theme(
        legend.position = "none",
        axis.text.x = element_text(face = "bold", size = 11, color = high_color),
        panel.grid = element_blank(),
        axis.text.y = element_text(size = 10),
        plot.margin = margin(0, 0, 0, 0)
      )
  }

  p1 <- build_column("Diagnosed", "diagnosis", "#6ee0c0")
  p2 <- build_column("On ART", "treatment", "#5b9cf5") + theme(axis.text.y = element_blank())
  p3 <- build_column("Suppressed", "suppression", "#00d4aa") + theme(axis.text.y = element_blank())

  (p1 | p2 | p3) +
    plot_annotation(
      title = paste0("Regional stage matrix | Observed year-end ", data$period_label),
      theme = epi_theme(12)
    ) &
    theme(
      plot.background = element_rect(fill = "transparent", colour = NA),
      panel.background = element_blank()
    )
}

build_regional_fingerprint_board <- function(data) {
  years <- sort(as.integer(unlist(data$years)))
  if (!length(years)) return(NULL)
  latest_year <- max(years, na.rm = TRUE)
  latest_rows <- bind_rows(lapply(data$rows_by_year[[as.character(latest_year)]], as_tibble))
  if (!nrow(latest_rows)) return(NULL)

  latest_rows <- latest_rows %>%
    mutate(mean_gap = as.numeric(mean_gap))

  latest_burden <- function(region) {
    history_rows <- bind_rows(lapply(data$region_histories[[region]]$cascade, as_tibble))
    if (!nrow(history_rows)) return(0)
    if (!"leakage_burden" %in% names(history_rows)) return(0)
    history_rows <- history_rows %>%
      mutate(
        year = as.integer(year),
        burden = as.numeric(leakage_burden)
      ) %>%
      filter(!is.na(burden)) %>%
      arrange(year)
    if (!nrow(history_rows)) return(0)
    tail(history_rows$burden, 1)
  }

  best_region <- latest_rows %>% arrange(mean_gap) %>% slice(1) %>% pull(region)
  worst_region <- latest_rows %>% arrange(desc(mean_gap)) %>% slice(1) %>% pull(region)
  burden_region <- latest_rows %>%
    mutate(latest_burden = vapply(region, latest_burden, numeric(1))) %>%
    arrange(desc(latest_burden)) %>%
    slice(1) %>%
    pull(region)

  chosen_regions <- unique(c(best_region, burden_region, worst_region))
  if (!length(chosen_regions)) return(NULL)

  cascade_rows <- bind_rows(lapply(chosen_regions, function(region) {
    history <- bind_rows(lapply(data$region_histories[[region]]$cascade, as_tibble))
    if (!nrow(history)) return(NULL)
    history <- history %>%
      mutate(year = as.integer(year)) %>%
      arrange(year)
    latest <- history %>% slice_tail(n = 1)
    previous <- if (nrow(history) > 1) history %>% slice_tail(n = 2) %>% slice_head(n = 1) else NULL
    tibble(
      region = region,
      year = latest$year,
      stage = c("Diagnosed", "On ART", "Suppressed"),
      value = c(as.numeric(latest$diagnosis), as.numeric(latest$treatment), as.numeric(latest$suppression)),
      delta = c(
        if (!is.null(previous)) as.numeric(latest$diagnosis) - as.numeric(previous$diagnosis) else NA_real_,
        if (!is.null(previous)) as.numeric(latest$treatment) - as.numeric(previous$treatment) else NA_real_,
        if (!is.null(previous)) as.numeric(latest$suppression) - as.numeric(previous$suppression) else NA_real_
      ),
      gap = c(
        95 - as.numeric(latest$diagnosis),
        95 - as.numeric(latest$treatment),
        95 - as.numeric(latest$suppression)
      )
    )
  }))
  if (!nrow(cascade_rows)) return(NULL)

  role_labels <- c(
    setNames("Closest to target", best_region),
    setNames("Largest leakage burden", burden_region),
    setNames("Widest gap", worst_region)
  )
  cascade_rows <- cascade_rows %>%
    mutate(
      stage = factor(stage, levels = c("Diagnosed", "On ART", "Suppressed")),
      role = unname(role_labels[region]),
      region_panel = paste0(region, "\n", role)
    )

  summary_rows <- cascade_rows %>%
    group_by(region, role, region_panel) %>%
    summarise(
      mean_gap = mean(gap, na.rm = TRUE),
      max_value = max(value, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      burden = vapply(region, latest_burden, numeric(1)),
      summary_label = sprintf("Mean gap %.1f pts\nLeakage burden %s", mean_gap, label_number(scale_cut = cut_short_scale())(burden))
    )

  ggplot(cascade_rows, aes(x = stage, y = value, colour = stage, group = 1)) +
    geom_hline(yintercept = 95, linewidth = 0.7, linetype = "22", colour = "#f0a030") +
    geom_col(aes(fill = stage), alpha = 0.18, width = 0.56, colour = NA) +
    geom_line(linewidth = 1.2, colour = "#1e2f4c", show.legend = FALSE) +
    geom_point(size = 4.3, stroke = 1.0, fill = "#101d33", shape = 21) +
    geom_text(
      aes(label = sprintf("%.0f%%", value)),
      nudge_y = 4.0,
      size = 4.0,
      fontface = "bold",
      colour = "#e0e6f0",
      show.legend = FALSE
    ) +
    geom_text(
      data = cascade_rows %>% filter(!is.na(delta)),
      aes(label = sprintf("%+.0f", delta)),
      nudge_y = -6.8,
      size = 3.4,
      colour = "#7889a0",
      show.legend = FALSE
    ) +
    geom_text(
      data = summary_rows,
      aes(x = 2.85, y = 26.5, label = summary_label),
      hjust = 1,
      vjust = 0,
      inherit.aes = FALSE,
      size = 3.6,
      colour = "#a0b0c0"
    ) +
    facet_wrap(~region_panel, nrow = 1) +
    scale_colour_manual(values = stage_palette) +
    scale_fill_manual(values = stage_palette) +
    scale_y_continuous(limits = c(24, 100), breaks = c(30, 40, 50, 60, 70, 80, 90, 95, 100), labels = label_percent(scale = 1)) +
    labs(
      x = NULL,
      y = "Coverage"
    ) +
    epi_theme(12) +
    theme(
      legend.position = "top",
      strip.text = element_text(size = 15, lineheight = 1.05),
      axis.text.x = element_text(face = "bold", size = 12),
      panel.grid.major.x = element_blank()
    )
}

build_anomaly_board <- function(data) {
  perf <- bind_rows(lapply(data$performance_burden_rows, as_tibble))
  if (!nrow(perf)) return(NULL)

  perf <- perf %>%
    mutate(
      leakage_total = leakage_burden,
      leakage_rate = ifelse((alive + leakage_total) > 0, leakage_total / (alive + leakage_total) * 100, NA_real_),
      sign = ifelse(residual >= 0, "Above expected", "Below expected"),
      stage = factor(stage, levels = c("Treatment after diagnosis", "Suppression after treatment"))
    )

  label_df <- perf %>%
    arrange(desc(abs(residual) + leakage_total / max(leakage_total, na.rm = TRUE) * 3)) %>%
    slice_head(n = min(3, nrow(perf)))

  quad <- ggplot(perf, aes(x = residual, y = leakage_rate)) +
    geom_vline(xintercept = 0, linewidth = 0.7, colour = "#7889a0") +
    geom_point(aes(size = leakage_total, fill = stage, colour = sign), shape = 21, stroke = 0.9, alpha = 0.88) +
    ggrepel::geom_text_repel(
      data = label_df,
      aes(label = region),
      size = 3.8,
      min.segment.length = 0,
      box.padding = 0.3,
      max.overlaps = 50,
      colour = "#e0e6f0"
    ) +
    scale_fill_manual(values = c("Treatment after diagnosis" = "#2d9cdb", "Suppression after treatment" = "#e63946")) +
    scale_colour_manual(values = c("Above expected" = "#5b9cf5", "Below expected" = "#f0a030")) +
    scale_size_continuous(range = c(3.8, 10.5), labels = label_number(scale_cut = cut_short_scale())) +
      scale_x_continuous(labels = function(x) sprintf("%+.0f", x)) +
      scale_y_continuous(limits = c(0, NA), labels = label_percent(scale = 1)) +
      labs(
        title = "Performance versus burden quadrant",
        x = "Residual vs expected (percentage points)",
        y = paste0("Loss-from-care rate (", data$period_label, ")"),
        size = "Loss from care",
        fill = "Cascade break",
        colour = "Direction"
      ) +
      epi_theme(12) +
      theme(
        legend.position = "bottom",
        legend.box = "vertical",
        legend.text = element_text(size = 10),
        legend.title = element_text(size = 10)
      )

  quad &
    theme(plot.background = element_rect(fill = "transparent", colour = NA), panel.background = element_blank())
}

build_historical_board <- function(data) {
  panel_specs <- list(
    list(key = "cases", title = "Cumulative reported HIV cases", color = "#2d9cdb", unit = "count"),
    list(key = "plhiv", title = "People living with HIV", color = "#5b9cf5", unit = "count"),
    list(key = "new_infections", title = "New HIV infections", color = "#f0a030", unit = "count"),
    list(key = "aids_deaths", title = "AIDS-related deaths", color = "#e63946", unit = "count")
  )

  plots <- lapply(panel_specs, function(spec) {
    points <- bind_rows(lapply(data[[spec$key]], as_tibble))
    if (!nrow(points)) return(NULL)
    years <- tibble(year = 2015:2025)
    df <- years %>% left_join(points %>% transmute(year = as.integer(year), value = as.numeric(value)), by = "year")
      ggplot(df, aes(x = year, y = value)) +
        geom_area(fill = alpha(spec$color, 0.12), colour = NA, na.rm = TRUE) +
        geom_line(color = spec$color, linewidth = 1.7, na.rm = TRUE) +
        geom_point(data = df %>% filter(!is.na(value)), color = "#db6b2c", fill = "#db6b2c", size = 3.0) +
        scale_x_continuous(breaks = c(2015, 2020, 2025)) +
        scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())) +
        labs(title = spec$title, x = NULL, y = NULL) +
        compact_theme(12)
  })

  wrap_plots(plots, ncol = 2) &
    theme(plot.background = element_rect(fill = "transparent", colour = NA), panel.background = element_blank())
}

build_key_population_board <- function(data) {
  panel_specs <- list(
    list(key = "pregnant_cumulative", title = "Pregnant women diagnosed", color = "#2d9cdb", unit = "count"),
    list(key = "tgw_cumulative", title = "TGW diagnosed", color = "#f0a030", unit = "count"),
    list(key = "ofw_cumulative", title = "OFW cumulative burden", color = "#5b9cf5", unit = "count"),
    list(key = "youth_share", title = "Youth share of reported cases", color = "#e63946", unit = "percent")
  )

  plots <- lapply(panel_specs, function(spec) {
    points <- bind_rows(lapply(data[[spec$key]], as_tibble))
    if (!nrow(points)) return(NULL)
    years <- tibble(year = 2015:2025)
    df <- years %>% left_join(points %>% transmute(year = as.integer(year), value = as.numeric(value)), by = "year")
      p <- ggplot(df, aes(x = year, y = value)) +
        geom_area(fill = alpha(spec$color, 0.12), colour = NA, na.rm = TRUE) +
        geom_line(color = spec$color, linewidth = 1.8, na.rm = TRUE) +
        geom_point(data = df %>% filter(!is.na(value)), color = "#db6b2c", size = 3.0) +
        scale_x_continuous(breaks = c(2015, 2020, 2025)) +
        labs(title = spec$title, x = NULL, y = NULL) +
        compact_theme(12)
    if (spec$unit == "percent") {
      p <- p + scale_y_continuous(labels = label_percent(scale = 1))
    } else {
      p <- p + scale_y_continuous(labels = label_number(scale_cut = cut_short_scale()))
    }
    p
  })

  wrap_plots(plots, ncol = 2) &
    theme(plot.background = element_rect(fill = "transparent", colour = NA), panel.background = element_blank())
}

render_safe <- function(builder, basename, width, height) {
  tryCatch({
    plot <- builder()
    if (!is.null(plot)) {
      save_plot_assets(plot, basename, width, height)
    }
  }, error = function(e) {
    message("Failed to render ", basename, ": ", conditionMessage(e))
  })
}

render_safe(function() build_national_cascade(series$national_cascade), "national_cascade_board", 15.5, 8.6)
render_safe(function() build_regional_matrix(series$regional_ladder), "regional_stage_matrix", 15.5, 8.8)
render_safe(function() build_anomaly_board(series$anomalies), "anomaly_board", 15.5, 8.3)
render_safe(function() build_historical_board(series$historical), "historical_board", 15.5, 9.0)
render_safe(function() build_key_population_board(series$key_populations), "key_populations_board", 15.5, 9.2)

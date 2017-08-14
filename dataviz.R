sessions <- events[, list(valid = "checkin" %in% event),
                   by = c("wiki_id", "date", "session_id")]

ggplot(
  sessions[, list(prop = round(sum(valid)/.N, 4)), by = c("date")],
  aes(x = date, y = prop)
) +
  geom_line() +
  geom_smooth(method = "gam", formula = y ~ s(x, k = 21)) +
  geom_vline(xintercept = as.numeric(as.Date("2017-06-15")), linetype = "dashed") +
  scale_y_continuous(labels = scales::percent_format()) +
  scale_x_date(date_labels = "%a\n%d %b", date_breaks = "2 weeks", date_minor_breaks = "1 week") +
  labs(
    x = "Date", y = "Proportion of sessions",
    title = "Proportion of desktop search sessions that stayed on SRP at least 10s",
    subtitle = "To be included, user must have arrived at search results page from autocomplete search",
    caption = "The post-deployment dip corresponds to the overall dip in our desktop search event logging discovered by Chelsy Xie
    Refer to page 35 of File:Wikimedia Foundation Readers metrics Q4 2016-17 (Apr-Jun 2017).pdf on Commons"
  ) +
  wmf::theme_min(14, "Source Sans Pro")

LD50_avg <- LD50 %>%
  dplyr::mutate(era = dplyr::case_when(
    date == "2017-06-15" ~ "deployment",
    date > "2017-06-15" ~ "post-deployment",
    date < "2017-06-15" ~ "pre-deployment"
  )) %>%
  dplyr::filter(era != "deployment") %>%
  dplyr::group_by(key, era) %>%
  dplyr::arrange(date) %>%
  dplyr::mutate(
    # rolling = c(rep(NA, 3), RcppRoll::roll_mean(seconds, 7, na.rm = TRUE, align = "center"), rep(NA, 3)),
    seconds = median(seconds, na.rm = TRUE)
  ) %>%
  dplyr::ungroup()

p <- ggplot(LD50, aes(x = date, y = seconds)) +
  geom_line(alpha = 0.8) +
  geom_vline(xintercept = as.numeric(as.Date("2017-06-15")), linetype = "dashed") +
  geom_line(data = LD50_avg, aes(color = era), size = 1) +
  # geom_line(data = LD50_avg, aes(y = rolling, color = era), size = 1) +
  facet_wrap(~ key, ncol = 1, scales = "free_y") +
  scale_color_brewer(palette = "Set1") +
  scale_y_continuous(labels = function(x) {
    minutes <- floor(x / 60); seconds <- x - (minutes * 60)
    return(sprintf("%.0fm %.0fs", minutes, seconds))
  }, breaks = c(0, 15, 30, (1:8) * 60), minor_breaks = NULL) +
  scale_x_date(date_labels = "%a\n%d %b", date_breaks = "2 weeks", date_minor_breaks = "1 week") +
  labs(
    x = "Date", y = "Time (seconds)",
    title = "How long Wikipedia searchers stay on full-text search results pages (SRPs) from autocomplete",
    subtitle = "\"Leaving the page\" could be from: clicking a result, making another search, or closing the tab",
    caption = "Since we do not know exact time that user left the page, we estimate using a check-in system and survival analysis."
  ) +
  wmf::theme_facet(14, "Source Sans Pro")
(p)
ggsave(
  plot = p, filename = "survival.png", path = "figures",
  width = 12, height = 10, units = "in", dpi = 300
)


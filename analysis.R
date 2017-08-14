library(magrittr)
library(data.table)
library(ggplot2)

events <- fread("data/tss2.csv", key = c("wiki_id", "session_id", "page_id"))
events$ts <- lubridate::ymd_hms(events$ts)
events$date <- as.Date(events$ts)

## Calculates the median lethal dose (LD50) and other.
## LD50 = the time point at which we have lost 50% of our users.
checkins <- c(0, 10, 20, 30, 40, 50, 60, 90, 120, 150, 180, 210, 240, 300, 360, 420)
# ^ this will be used for figuring out the interval bounds for each check-in
# Treat each individual search session as its own thing, rather than belonging
#   to a set of other search sessions by the same user.
page_visits <- events[, {
  if (all(!is.na(.SD$checkin))) {
      last_checkin <- max(.SD$checkin, na.rm = TRUE)
      idx <- which(checkins > last_checkin)
      if (length(idx) == 0) idx <- 16 # length(checkins) = 16
      next_checkin <- checkins[min(idx)]
      status <- ifelse(last_checkin == 420, 0, 3)
      data.table(
        `last check-in` = as.integer(last_checkin),
        `next check-in` = as.integer(next_checkin),
        status = as.integer(status)
      )
    }
}, by = c("date", "wiki_id", "session_id", "page_id")]

ldn <- function(df, n = 0.5) {
  surv <- survival::Surv(
    time = df$`last check-in`,
    time2 = df$`next check-in`,
    event = df$status,
    type = "interval")
  fit <- survival::survfit(surv ~ 1)
  return(as.numeric(quantile(fit, probs = n)$quantile))
}
LD50 <- page_visits[, list(
  `Time until 10% of users have left SRP` = ldn(.SD, 0.1),
  `Time until 25% of users have left SRP` = ldn(.SD, 0.25),
  `Time until 50% of users have left SRP` = ldn(.SD),
  `Time until 75% of users have left SRP` = ldn(.SD, 0.75)
), by = c("date")] %>%
  tidyr::gather(., key, seconds, -date)

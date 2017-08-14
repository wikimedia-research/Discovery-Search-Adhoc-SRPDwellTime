if (!dir.exists("data")) {
  dir.create("data", recursive = TRUE)
}

if (!grepl("^stat1", Sys.info()["nodename"])) {
  message("Creating an auto-closing SSH tunnel in the background...")
  # See https://gist.github.com/scy/6781836 for more info.
  system("ssh -f -o ExitOnForwardFailure=yes stat1003.eqiad.wmnet -L 3307:analytics-store.eqiad.wmnet:3306 sleep 10")
  library(RMySQL)
  con <- dbConnect(MySQL(), host = "127.0.0.1", group = "client", dbname = "log", port = 3307)
} else {
  con <- wmf::mysql_connect("log")
}

library(glue)
library(magrittr)

query <- "SELECT
  timestamp AS ts, wiki AS wiki_id,
  event_uniqueId AS event_id,
  event_searchSessionId AS session_id,
  event_pageViewId AS page_id,
  event_action AS event,
  event_checkin AS checkin,
  event_scroll AS has_scrolled
FROM TestSearchSatisfaction2_16909631
WHERE
  LEFT(timestamp, 8) = '{yyyymmdd}'
  AND wiki RLIKE 'wiki$'
  AND NOT wiki RLIKE '^(arbcom)|(be_x_old)'
  AND NOT wiki IN('commonswiki', 'mediawikiwiki', 'metawiki', 'checkuserwiki', 'donatewiki', 'collabwiki', 'foundationwiki', 'incubatorwiki', 'legalteamwiki', 'officewiki', 'outreachwiki', 'sourceswiki', 'specieswiki', 'stewardwiki', 'wikidatawiki', 'wikimania2017wiki', 'movementroleswiki', 'internalwiki', 'otrs_wikiwiki', 'projectcomwiki', 'ombudsmenwiki', 'votewiki', 'chapcomwiki', 'nostalgiawiki', 'otrs_wikiwiki')
  AND event_source = 'autocomplete'
  AND event_subTest IS NULL
  AND event_articleId IS NULL
  AND event_action IN('visitPage', 'checkin')

UNION

SELECT
  timestamp AS ts, wiki AS wiki_id,
  event_uniqueId AS event_id,
  event_searchSessionId AS session_id,
  event_pageViewId AS page_id,
  event_action AS event,
  event_checkin AS checkin,
  event_scroll AS has_scrolled
FROM TestSearchSatisfaction2_16270835
WHERE
  LEFT(timestamp, 8) = '{yyyymmdd}'
  AND wiki RLIKE 'wiki$'
  AND NOT wiki RLIKE '^(arbcom)|(be_x_old)'
  AND NOT wiki IN('commonswiki', 'mediawikiwiki', 'metawiki', 'checkuserwiki', 'donatewiki', 'collabwiki', 'foundationwiki', 'incubatorwiki', 'legalteamwiki', 'officewiki', 'outreachwiki', 'sourceswiki', 'specieswiki', 'stewardwiki', 'wikidatawiki', 'wikimania2017wiki', 'movementroleswiki', 'internalwiki', 'otrs_wikiwiki', 'projectcomwiki', 'ombudsmenwiki', 'votewiki', 'chapcomwiki', 'nostalgiawiki', 'otrs_wikiwiki')
  AND event_source = 'autocomplete'
  AND event_subTest IS NULL
  AND event_articleId IS NULL
  AND event_action IN('visitPage', 'checkin');"

results <- do.call(rbind, lapply(seq(as.Date("2017-04-01"), Sys.Date() - 1, "day"), function(date) {
  message("Fetching events from ", format(date, "%d %B %Y"), "...")
  yyyymmdd <- format(date, "%Y%m%d")
  query <- glue(query)
  result <- wmf::mysql_read(query, "log", con) %>%
    dplyr::mutate(
      ts = lubridate::ymd_hms(ts),
      has_scrolled = has_scrolled == 1
    ) %>%
    dplyr::arrange(session_id, event_id, ts) %>%
    dplyr::distinct(session_id, event_id, .keep_all = TRUE) %>%
    dplyr::arrange(wiki_id, session_id, page_id, desc(event), ts) %>%
    dplyr::select(wiki_id, ts, session_id, page_id, event, checkin, has_scrolled) %>%
    dplyr::group_by(session_id, page_id) %>%
    dplyr::filter(
      event == "visitPage" |
        (event == "checkin" & has_scrolled) |
        (event == "checkin" & checkin == max(checkin))
    ) %>%
    dplyr::ungroup()
  # Saving per-day event data:
  for (wiki in unique(result$wiki_id)) {
    if (!dir.exists(file.path("data", "by wiki", wiki))) {
      dir.create(file.path("data", "by wiki", wiki), recursive = TRUE)
    }
    file_path <- file.path("data", "by wiki", wiki, glue("{wiki}-{yyyymmdd}.csv"))
    readr::write_csv(result[result$wiki_id == wiki, ], file_path)
    message("Events from ", format(date, "%d %B %Y"), " from ", wiki, " written to ", file_path)
  }
  return(result)
}))

wmf::mysql_close(con)

message("Converting to data.table...")
results <- data.table::data.table(results, key = "wiki_id")
message("Saving per-wiki datasets...")
for (wiki in unique(results$wiki_id)) {
  data.table::fwrite(results[wiki_id == wiki, ], file.path("data", "by wiki", glue("{wiki}.csv")))
}
message("Saving full dataset...")
data.table::fwrite(results, file.path("data", "tss2.csv"))
message("Done!")

if (!grepl("^stat1", Sys.info()["nodename"])) {
  # Ran script on stat1003; need to download the results:
  system('scp -r stat3:"/home/bearloga/srp-dwelltime/data/tss2.csv" data/')
  system('scp -r stat3:"/home/bearloga/srp-dwelltime/data/by\\ wiki/*.csv" data/by\\ wiki/')
  # For development:
  system('scp -r stat3:"/home/bearloga/srp-dwelltime/data/by\\ wiki/enwiki" data/by\\ wiki/')
  system('scp -r stat3:"/home/bearloga/srp-dwelltime/data/by\\ wiki/frwiki" data/by\\ wiki/')
  system('scp -r stat3:"/home/bearloga/srp-dwelltime/data/by\\ wiki/ruwiki" data/by\\ wiki/')
  system('scp -r stat3:"/home/bearloga/srp-dwelltime/data/by\\ wiki/dewiki" data/by\\ wiki/')
  csvs <- list.files("data/by wiki", "[0-9]{8}.csv$", recursive = TRUE, full.names = TRUE)
  events <- data.table::rbindlist(lapply(csvs, data.table::fread))
  events[, list(days = length(unique(as.Date(lubridate::ymd_hms(ts))))), by = "wiki_id"]
  for (wiki in unique(events$wiki_id)) {
    data.table::fwrite(events[wiki_id == wiki, ], file.path("data", "by wiki", glue("{wiki}.csv")))
  }
  data.table::fwrite(events, file.path("data", "tss2.csv"))
}

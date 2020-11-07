# Ryan Nelson
#
# Exploration of Data towards a graph to show the wait
# times of LOW/HIGH priority calls
#

# HIGH calls add 1 minute, LOW calls add nothing
# The added wait durations are NOT added to the queue
# duration field in the data (i.e. would-be actual wait times)

# To remove everything and start clean in the event of re-running the code
rm(list = ls())
dbDisconnect(con)


library(dplyr)
library(lubridate)
library(RSQLite)
library(ggplot2)

rawdata = readxl::read_excel(path = "rawdata.xlsx",
                                sheet = "Data") %>%
  filter(priority != '(null)')

data = rawdata %>%
  transmute(queue_name,
            priority,
            queue_duration = dseconds(queue_duration),
            artificial_duration = if_else(priority == 'HIGH',
                                          queue_duration + dseconds(60),
                                          queue_duration),
            calldate = format(as.Date(calldate)),
            startqueue = with_tz(as.POSIXct(queue_enqueuetimestamp,
                                            tz = "UTC"),
                                 tzone = "US/Pacific"),
            endqueue = startqueue + queue_duration,
            queue_interval = interval(startqueue,
                                      startqueue + queue_duration),
            startqueue = format(startqueue, "%Y-%m-%d %H:%M:%S"),
            endqueue = format(endqueue, "%Y-%m-%d %H:%M:%S")
                                )
rm(rawdata)
#data.table::fwrite(data, file = "data.csv")



#################################################################
# create time series for SQL join
start = as.POSIXct('2016-12-05')
end = as.POSIXct('2016-12-29')

# Set a time duration by which to summarize (in seconds)
# Smaller time frames drastically increase runtime
interval = 60 * 10 # 10 minutes


times = tibble(intstart = format(seq(start, end, by = interval),
                                 "%Y-%m-%d %H:%M:%S"),
               intend = format(seq(start, end, by = interval) + (interval) - 1,
                               "%Y-%m-%d %H:%M:%S"),
)

#data.table::fwrite(times, file = "times.csv")


#################################################################


# Create an SQLite database for the time series
con = dbConnect(RSQLite::SQLite(), "data-r.db")

# Clear the database of existing data in the event of re-running the code
RSQLite::dbRemoveTable(con, "times")
RSQLite::dbRemoveTable(con, "data")

# store time and data in separate tables in the database
dbWriteTable(con, "times", times)
dbWriteTable(con, "data", data)

# Use the SQLite database to count all calls that had any wait time during the
# interval (and not only if they were answered or queued during the interval)
query_pull = dbGetQuery(con, "SELECT * FROM times t
                              LEFT JOIN data d ON t.intstart <= d.endqueue
                                               AND t.intend >= d.startqueue")

# instead of using each call's final wait time, use how long a call had been
# waiting at the end of the interval (if the call was still waiting then)
sqlmap = query_pull %>%
  select(-queue_interval) %>%
  mutate_at(vars("intstart",
                 "intend",
                 "startqueue",
                 "endqueue"),
            as.POSIXct, tz = "US/Pacific") %>%
  mutate(call_waiting = dseconds(if_else(intend > endqueue,
                                         interval(startqueue, endqueue),
                                         interval(startqueue, intend)
                                         )
                                 ),
         hourminute = format(intstart, "%H:%M")
         ) %>% na.omit()

# filter only for queues that have a high/low distinction
# (this should be done earlier; no reason to keep irrelevant data from the start)
sqlmap = sqlmap %>% filter(queue_name %in% c("queueAA1","queueAA2","queueAA3"))

# Create a priority category to include the additional/fake wait time
highonly = sqlmap %>%
  filter(priority == "HIGH") %>%
  mutate(priority = "HIGH + artificial aging",
         call_waiting = call_waiting + dseconds(60))

# and combine it into the original table
sqlmap = sqlmap %>%
  rbind.data.frame(highonly) %>%
  mutate(priority = if_else(priority == "HIGH",
                            "HIGH (actual)",
                            priority))

# set time periods for each graph
singledate = '2016-12-15'
wkstart ='2016-12-19'
wkend = '2016-12-23'

# graph for single day
sqlmap %>%
  filter(as.POSIXct(calldate) == singledate,
         priority != "HIGH + artificial aging") %>% # this was included for
                        # comparison purposes, but left out of final graphs
  group_by(hourminute,
           priority) %>%
  summarise(longest_wait = max(call_waiting)) %>%
  ungroup() %>%
  ggplot(aes(x = hourminute, y = longest_wait, group = priority, color = priority)) +
  geom_line() +
  labs(y = "Longest Wait (seconds)",
       x = "Time in 10-minute intervals",
       title = paste("Longest Wait Times by Priority Level: ",wkstart))

# grapht for week (Mon-Fri)
sqlmap %>%
  filter(as.POSIXct(calldate) %within% interval(wkstart, wkend),
         priority != "HIGH + artificial aging") %>% # this was included for
                        # comparison purposes, but left out of final graphs
  group_by(hourminute,
           priority) %>%
  summarise(longest_wait = max(call_waiting)) %>%
  ungroup() %>%
  ggplot(aes(x = hourminute, y = longest_wait, group = priority, color = priority)) +
  geom_line() +
  labs(y = "Longest Wait (seconds)",
       x = "Time in 10-minute intervals",
       title = paste("Longest Wait Times by Priority Level: Week of ",wkstart))

# faceted graphs for week, grouped by queue
sqlmap %>%
  filter(as.POSIXct(calldate) %within% interval('2016-12-19', '2016-12-24')) %>%
  group_by(hourminute,
           priority,
           queue_name) %>%
  summarise(longest_wait = max(call_waiting)) %>%
  ungroup() %>%
  ggplot(aes(x = hourminute, y = longest_wait, group = priority, color = priority)) +
  geom_line() +
  facet_grid(~queue_name)

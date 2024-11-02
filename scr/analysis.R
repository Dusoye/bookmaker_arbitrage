library(data.table)
library(dplyr)
library(magrittr)
library(ggplot2)
library(gridExtra)
library(scales)
library(stringr)
library(tidyr)
library(lubridate)

polymarket <- read.csv("~/Projects/bookmaker_abritrage/output/polymarket_data.csv")
betfair <- read.csv("~/Projects/bookmaker_abritrage/output/betfair_data.csv")
oddschecker <- read.csv("~/Projects/bookmaker_abritrage/output/oddschecker_data.csv")


oddschecker %>%
  mutate(timestamp = as.POSIXct(timestamp, origin = "1970-01-01", format = "%Y-%m-%dT%H:%M:%S")) %>%
  filter(bet_name %in% c('Donald Trump','Kamala Harris')) %>%
  pivot_longer(cols = -c('bet_name','timestamp')) %>%
  filter(name %in% c('B3','SK','WH')) %>%
  mutate(source = paste0(bet_name,"_",name)) %>%
  select(-bet_name,-name) -> prices

betfair %>% 
  mutate(timestamp = as.POSIXct(timestamp, origin = "1970-01-01", format = "%Y-%m-%dT%H:%M:%S")) %>%
  mutate(source = paste0(bet_name, "_BFX")) %>%
  select(timestamp, value = back_price, source) %>%
  rbind(., prices) -> prices

polymarket %>%
  mutate(timestamp = as.POSIXct(timestamp, origin = "1970-01-01", format = "%Y-%m-%dT%H:%M:%S")) %>%
  mutate(source = if_else(bet_id == 253591, "Donald Trump_PM", "Kamala Harris_PM"),
         value = 1/yes_price) %>%
  select(timestamp, value, source) %>%
  rbind(., prices) -> prices
  
prices %>%
  ggplot(aes(x = timestamp, y = value, colour = source)) +
  geom_step() +
  theme_minimal()

prices %>% 
  mutate(percentage = 1/value) %>%
  mutate(venue = str_split(source, "_", simplify = TRUE)[, 2]) %>%
  select(-value, -source) %>%
  group_by(timestamp, venue) %>%
  summarise(percentage = sum(percentage)) %>%
  ggplot(aes(x = timestamp, y = percentage, colour = venue)) +
  geom_step() +
  theme_minimal()
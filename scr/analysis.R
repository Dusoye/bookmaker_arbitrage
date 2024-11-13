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
predictit <- read.csv("~/Projects/bookmaker_abritrage/output/predictit_data.csv")

oddschecker %>%
  mutate(timestamp = as.POSIXct(timestamp, origin = "1970-01-01", format = "%Y-%m-%dT%H:%M:%S")) %>%
  filter(bet_name %in% c('Donald Trump','Kamala Harris')) %>% select(-WH) %>%
  pivot_longer(cols = -c('bet_name','timestamp')) %>% 
  filter(name %in% c('B3','SK','BF')) %>%
  mutate(market = bet_name,
         source = name) %>%
  select(timestamp, value, market, source) -> prices

betfair %>% 
  mutate(timestamp = as.POSIXct(timestamp, origin = "1970-01-01", format = "%Y-%m-%dT%H:%M:%S")) %>%
  mutate(market = bet_name,
         source = "betfairx") %>%
  select(timestamp, value = back_price, market, source) %>%
  rbind(., prices) -> prices

polymarket %>%
  mutate(timestamp = as.POSIXct(timestamp, origin = "1970-01-01", format = "%Y-%m-%dT%H:%M:%S")) %>%
  mutate(market = if_else(bet_id == 253591, "Donald Trump", "Kamala Harris"),
         source = 'polymarket',
         value = 1/yes_price) %>%
  select(timestamp, value, market, source) %>%
  rbind(., prices) -> prices

predictit %>%
  mutate(timestamp = as.POSIXct(timestamp, origin = "1970-01-01", format = "%Y-%m-%dT%H:%M:%S")) %>%
  mutate(market = bet_name,
         source = 'predictit',
         value = 1/buy_yes_price) %>%
  select(timestamp, value, market, source) %>%
  rbind(., prices) -> prices
  
prices %>%
  filter(timestamp >= '2024-11-03',
         source %like% 'Donald') %>%
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
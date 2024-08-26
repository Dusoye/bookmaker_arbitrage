### Arbitraging fixed-odds sports

One of the best way to make consistent returns through sports betting is to identify mispriced betting opportunities before a bookmaker is able to update their price. For highly liquid markets such as win/lose/draw on a Premier League game, any new information that can have an impact on the probability of a result is quickly filtered into the prices. As such, even though the bookies edge (overround) is the smallest in these markets, due to the volume they see, prices are quick to update and they days of slow bookies are long gone. One of the issues is that betting on prices that shorten quickly after your money is on is a good way to get restricted.

I still believe there is an edge to be had in markets that have fewer eyeballs on them and much more sporadic and varied sources of new information that may have an impact on the probabilities, in particular betting on political markets. However, the likelihood of being restricted in betting on political markets is probably far higher, due to both less volume going through leaving your bets more visible, but also that the bookmakers know they are potentially exposed due to lack of knowledge.

Either way, this takes a look at arbitraging political betting markets in the run up to the US Presidential Elections in November; taking a look at prices changes on Betfair Exchange (the most liquid venue), and then scrapes oddschecker for the fixed-odds offered by each bookmaker in order to identify any mispricing opportunities.

Due to the nature of oddschecker's website, the script uses Selenium, loading each odds table in a concurrent thread, and comparing them to the Betfair odds.

Cosine similarity has been used in an attempt of automating matching the Betfair market name to the oddschecker name, but the matching isn't perfect and so a manual markets.csv has been created and can be amended going forward to include/exclude more markets.

The betting tables for Betfair and Oddschecker are then matched by the markets mapping and then the betting selections are matched using fuzzy match due to the names not always exactly aligning. 

One of the arbitrage methods with this data is to simply find occasions when you're able to lay on betfair at a lower price than you can back the bet through a bookmaker on oddschecker. An example of this is below where you're able to bet on Republicans in Pennsylvania and Arizona at 2.2 and 2 and lay at 2.04 and 1.91 respectively. We're able to extract the size available to lay at the best price from Betfair (assuming that this will generally be the liquidity limiting factor rather than the amount available to back at the bookmaker) and then determine the amount we should back and the guaranteed profit from making the bet.


| Market Name   | Selection Name   |   Odds to Lay Ratio |   Best Back Price |   Best Lay Price |   Best Lay Size |   Best Odds | Best Bookmaker   | URL                                                                            |
|:--------------|:-----------------|--------------------:|------------------:|-----------------:|----------------:|------------:|:-----------------|:-------------------------------------------------------------------------------|
| Pennsylvania  | Republicans      |             1.04762 |              2.04 |             2.1  |           31.48 |         2.2 | S6               | https://www.oddschecker.com/politics/us-politics/us-state-betting/pennsylvania |
| Arizona       | Republicans      |             1.04712 |              1.9  |             1.91 |           11.95 |         2   | S6               | https://www.oddschecker.com/politics/us-politics/us-state-betting/arizona      |

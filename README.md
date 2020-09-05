# Telegram-Corona-Bot

Why another Corona-Bot? Basically the reason is that it provides an easy access to the most up-to-date German data. Mainly, Risklayer and the Center for Disaster Management and Risk Reduction Technology (CEDIM) have created a Google Spreadsheet, in which they and other users collect the case numbers. The advantage of this approach is that they source the data directly from the districts, and are therefore often 2 days ahead of the RKI numbers. However it is often quite slow to open the [Document](https://docs.google.com/spreadsheets/d/1wg-s4_Lz2Stil6spQEYFdZaBEp8nWW26gVyfHqvcl8s/edit#gid=0).

This is where the bot comes into play. It requests the data via API and periodically inserts it into a database. This makes the bot very fast, and it can provide meaningful updates at any time of the day. The bot Can be accessed by: @CoronaRisklayerBot in Telegram.

## Demo

![demo](bot.gif)


## How to start
First, install the dependencies via `pip install -r requirements.txt`. 
Before the bot can be started, you need to create a postgres database, a telegram bot, and a google account first. Then you need to supply the resepective credentials as environment variables (see the `start_bot.py` script). The database scheme will be generated automatically. However, you need to initialize the `kreise` table by yourself, by uploading the csv `kreise_table.csv` to it (this table contains data about id and bundesland of each kreis) (sorry for the mismatch of German and English, but I find the German domain names more intuitive :D). After this table is initialized, the bot can be started with the `start_bot.py` script. The main functions of the bot only work after it has collected one week worth of data, since it always compares the result for the current day with the result of the day the week before.






# Telegram-Corona-Bot

Why another Corona-Bot? Basically the reason is that it provides an easy access to the most up-to-date German data. Mainly, Risklayer and the Center for Disaster Management and Risk Reduction Technology (CEDIM) have created a Google Spreadsheet, in which they collect the newest case numbers. The advantage of this approach is that they source the data directly from the districts, and are therefore often 2 days ahead of the RKI numbers. However it is often quite slow to open the [Document](https://docs.google.com/spreadsheets/d/1wg-s4_Lz2Stil6spQEYFdZaBEp8nWW26gVyfHqvcl8s/edit#gid=0).

This is where the bot comes into play. It requests the data via API and periodically inserts it into a database. This makes the bot very fast, and it can provide meaningful updates at any time of the day.






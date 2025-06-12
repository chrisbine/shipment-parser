# shipment-parser
Parsing shipment data

Calling main.py will download the last 3 days of data from https://twtransfer.energytransfer.com/ipost/TW/capacity/operationally-available

It will then parse, validate, and insert the data into the database "energy_data.db"

If needed, change the parameter of the call in main.py to set a different number of days back to download

## Fetch history logic flow
```json
{"range": 100, "start": 20, "end": 3061014649, "current_id": 20}
```
1. `search_history.json` has only one config
2. `search_history.json` will update to the latest tweet id, after each fetch tweets by tweets id

python twitter_streamer.py -c ../twittertracker-config/config_i0mf0rmer01.json -o /mnt/data2/twitter/sample/ -cmd sample

python twitter_streamer.py -c ../twittertracker-config/config_i0mf0rmer02.json -o /mnt/data2/twitter/US_BY_STATE -cmd locations -cc test_data/geo/US_BY_STATE_1.json

python twitter_tracker.py -c ../twittertracker-config/config_i0mf0rmer08.json -cmd history -o /mnt/data2/twitter/twitter.historical/0000M_0500M/ -cc search_history.0000M_0500M.json

python twitter_tracker.py -c ../twittertracker-config/config_i0mf0rmer09.json -cmd history -o /mnt/data2/twitter/twitter.historical/0500M_1000M/ -cc search_history.0500M_1000M.json
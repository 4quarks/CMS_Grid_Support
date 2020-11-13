# CMS_Grid_Support
Tools too speed monitoring in CMS 

## Getting Started

### Prerequisites
- Python 3
- Grafana key to query ElasticSearch. e.g. "Bearer FNJZ0gyS..." 

### Installing

TODO on the command line:

- Clone repo:
```
git clone git@github.com:paucutrina/CMS_Grid_Support.git
```
- Setup virtual environment
```
cd CMS_Grid_Support
sudo apt-get install python3-venv
python3 -m venv venv
source venv/bin/activate
```
- Install Python libraries on the venv:
```
pip install -r requirements.txt
```
- Install mongodb e.g. Ubuntu:
```
wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | sudo apt-key add -
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/4.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.4.list
sudo apt-get update
sudo apt-get install -y mongodb-org
echo "mongodb-org hold" | sudo dpkg --set-selections
echo "mongodb-org-server hold" | sudo dpkg --set-selections
echo "mongodb-org-shell hold" | sudo dpkg --set-selections
echo "mongodb-org-mongos hold" | sudo dpkg --set-selections
echo "mongodb-org-tools hold" | sudo dpkg --set-selections
sudo systemctl start mongod
sudo systemctl status mongod
mongo
```

### Add environmental variables:
- export MONGODB_HOST=localhost:27017
- export GRAFANA_KEY=Bearer FNJZ0gyS...

e.g. in Pycharm https://stackoverflow.com/questions/42708389/how-to-set-environment-variables-in-pycharm
## First steps to execute FTS analysis
1. Run vofeed.py to dump all data to mongodb
2. Run transfers.py setting the desired configuration



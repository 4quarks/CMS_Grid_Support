# CMS_Grid_Support
Tools too speed monitoring in CMS 

## Getting Started

### Prerequisites
- Python 3
- Grafana key to query ElasticSearch. e.g. "Bearer FNJZ0gyS..." 

### Mandatory installation

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
#### Add environmental variables:
```
export GRAFANA_KEY="Bearer FNJZ0gyS..."
```

### Optional installation
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
export MONGODB_HOST="localhost:27017"
mongo
```
- Download NLP model:
```
python -m spacy download en_core_web_lg
```
## First steps to execute FTS analysis
1. Run vofeed.py to dump all data to mongodb
2. Select the time scope of your search. e.g. Time(hours=24)
3. Define the elements that you do not want to find on the source/destination URL. e.g. BLACKLIST_PFN = ["se3.itep.ru", "LoadTest"] 
4. Introduce the site name or the host name. e.g. fts.analyze_site(site="T2_HU_Budapest")
5. Run transfers.py

## First steps to execute Site Status analysis
1. Run vofeed.py to dump all data to mongodb
2. Define the sites that you are not interested in. e.g. BLACKLIST_SITES = ["T2_PL_Warsaw", "T2_RU_ITEP"]
3. Introduce site or the scope to analyze. AbstractSiteStatus(time_ss, site="T1|T2")
4. Run site_status.py


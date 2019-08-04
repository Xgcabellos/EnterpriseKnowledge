#!/usr/bin/env bash
#sudo add-apt-repository ppa:mystic-mirage/pycharm
#sudo apt-get update
#sudo apt-get install pycharm-community
sudo apt-get install python3
sudo apt install python3-pip
python3 --version
sudo apt update
sudo apt install git
git --version
mkdir input
mkdir EKnowedge
git init
git clone https://github.com/Xgcabellos/EKnowedge.git
cd EKnowedge
mkdir output
mkdir json
mkdir logs
cd ..
cd
pip3 install --user -U nltk
pip3 install --user -U numpy
python3 -m nltk.downloader popular
pip install neo4j
python -m pip install pymongo
pip3 install libpff-python
pip3 install unicodecsv
pip3 install py-dateutil
pip3 install jinja2
pip3 install psutil
pip3 install py
pip3 install contractions
pip3 install inflect
pip3 install nltk
#pip install EmailReplyParser
pip3 install htmllaundry
pip3 install contractions

sudo apt-get install build-essential debhelper fakeroot autotools-dev zlib1g-dev python-all-dev python3-all-dev
git clone https://github.com/libyal/libpff.git
cd libpff/
./synclibs.sh
./autogen.sh
./configure --prefix /usr/ --with-python --with-python3
sudo make install


sudo su
wget --no-check-certificate -O - https://debian.neo4j.org/neotechnology.gpg.key | sudo apt-key add -
echo 'deb http://debian.neo4j.org/repo stable/' > /etc/apt/sources.list.d/neo4j.list
apt update
apt install neo4j
cd /var/lib/neo4j/plugins
wget https://github.com/neo4j-contrib/neo4j-graph-algorithms/releases/download/3.5.4.0/graph-algorithms-algo-3.5.4.0.jar
sudo nano /etc/neo4j/neo4j.conf
# add dbms.security.procedures.unrestricted=algo.*,apoc.*
#dbms.security.auth_enabled=false
pip install -U spacy
python -m spacy download en
python -m spacy download es
python -m spacy download xx
pip install spacy-langdetect


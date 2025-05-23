apt update
apt install zip
apt install unzip
unzip ./data/quality_data/kghb_output/quality_data.zip -d ./data/quality_data/kghb_output/
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cd src
python3 main.py
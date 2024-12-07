#!/bin/bash
sudo apt-get update -y
sudo apt update; sudo apt install build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev curl \
libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev -y

GITHUB_TOKEN="" # Add your github token here
REPO_URL="https://github.com/USC-ACE-Lab/SciSO.git"
git clone https://${GITHUB_TOKEN}@${REPO_URL#https://}

curl https://pyenv.run | bash
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init --path)"
echo -e "\nexport PYENV_ROOT=\"$HOME/.pyenv\"" >> ~/.bashrc
echo "export PATH=\"\$PYENV_ROOT/bin:\$PATH\"" >> ~/.bashrc
echo 'eval "$(pyenv init --path)"' >> ~/.bashrc
source ~/.bashrc
pyenv install 3.11
cd SciSO
pyenv local 3.11
pip install aiohttp
pip install aiolimiter
pip install bs4
pip install certifi
pip install nltk
pip install numpy
pip install pandas
pip install python-dotenv
pip install strsimpy
pip install tqdm
pip install urllib3
pip install wayback
pip install lmdb
pip install lxml

# Install Grobid-Client
cd ~
git clone https://github.com/kermitt2/grobid_client_python
cd grobid_client_python
pyenv local 3.11
python3 setup.py install
pip uninstall grobid-client -y
cd ~

# Install Docker
sudo apt-get install ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update -y
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin -y

# Install Grobid Image
sudo docker pull grobid/grobid:0.8.1

# Install GCP SDK
sudo apt-get install apt-transport-https ca-certificates gnupg curl sudo -y
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
sudo apt-get update -y && sudo apt-get install google-cloud-cli -y

gcloud config configurations create sciso-config
gcloud config set project soreference
gcloud config set compute/zone us-west1-b
gcloud config set compute/region us-west1

gcloud storage cp gs://sciso/pdfs.jsonl .
# gcloud storage cp gs://sciso/comments_pdf.jsonl . &

git config --global user.name "itsrun"
git config --global user.email "runhuang@usc.edu"

cd ~
nohup sudo docker run --rm --init --ulimit core=0 -p 8070:8070 grobid/grobid:0.8.1 &

cd ~/SciSO
# nohup python3 sciso/harvest/orchestrate.py ../pdfs.jsonl -p &
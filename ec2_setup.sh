#!/bin/bash
# Update the system
sudo apt-get update -y
sudo apt-get upgrade -y

# Install Git
sudo apt-get install -y git
sudo apt-get install -y gh


# Install Miniconda
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh
bash /tmp/miniconda.sh -b -p $HOME/miniconda
rm /tmp/miniconda.sh
# Configure Miniconda
$HOME/miniconda/bin/conda init bash
echo 'export PATH="/home/ubuntu/miniconda/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc

# Install Docker
sudo apt-get install -y docker.io
sudo systemctl start docker
sudo systemctl enable docker

# Add current user to the Docker group
sudo usermod -aG docker ubuntu

sudo git clone https://github.com/UW-Climate-Risk-Lab/climate-risk-map.git /home/ubuntu/climate-risk-map

# Install GDAL and Python development packages
sudo apt-get install -y gdal-bin libgdal-dev python3-gdal

# Install additional Python libraries that will be useful
sudo apt-get install -y python3-pip


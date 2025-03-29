#!/bin/bash
# Update the system
sudo apt-get update -y
sudo apt-get upgrade -y

# Install Git
sudo apt-get install -y git
sudo apt-get install -y gh

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

# Install Miniconda for the ubuntu user
su - ubuntu -c "wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh"
su - ubuntu -c "bash /tmp/miniconda.sh -b -p /home/ubuntu/miniconda"
rm /tmp/miniconda.sh

# Initialize conda in the user's bashrc
su - ubuntu -c "/home/ubuntu/miniconda/bin/conda init bash"

# Install AWS CLI
sudo apt-get install -y unzip
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
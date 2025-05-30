#!/bin/bash
# Update the system
sudo apt-get update -y
sudo apt-get upgrade -y
sudo apt-get curl -y

# Install Git
sudo apt-get install -y git
sudo apt-get install -y gh

# Install uv
sudo curl -LsSf https://astral.sh/uv/install.sh | sh

# Add Docker's official GPG key:
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Docker: Add the repository to Apt sources and install Docker:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin


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
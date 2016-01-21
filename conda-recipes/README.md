## Building conda packages

Conda packages for knit and its dependencies can be built from Ubuntu 14.04
using the following commands:

```
export CONDA_DIR=~/miniconda2

sudo apt-get update
sudo apt-get install -y -q git

curl http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -o ~/miniconda.sh
bash ~/miniconda.sh -b -p $CONDA_DIR
$CONDA_DIR/bin/conda install conda-build anaconda-client -y

git clone https://github.com/blaze/knit.git ~/knit
cd ~/knit/conda-recipes
$CONDA_DIR/bin/conda build knit --python 2.7 --python 3.4 --python 3.5

$CONDA_DIR/bin/anaconda login
$CONDA_DIR/bin/anaconda upload ~/$CONDA_DIR/conda-bld/linux-64/{FILES} -u blaze
```

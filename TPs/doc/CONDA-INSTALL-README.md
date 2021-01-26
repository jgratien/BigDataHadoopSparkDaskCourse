# ML4CFD

## I/ INTRODUCTION
to run the code main.py in ./GEN_DATA you need:

 1) multiprocessing
 2) pygmsh pip install pygmsh==6.0.2 (no longer true, you can use last version instead)
 3) pymesh pip install pymesh2==0.2.1
 
other non standard libraries:
 1) pickle
 2) scikit-learn
 3) time
 4) copy
 
## II/ RAPPEL GESTION ENVIRONNEMENT CONDA

Conda permet de gérer précisément son envitonnement python et de l'exporter facilement sur des clusters.

Il necessite d'etre en bash

Les environnements conda prennent beacoup de place. le mieux est de ne pas les laisser dans zones avec des quotas limités.

Pour cela on peut setter cette zones dans des zones avec de la place de préférences visible de tous neouds du cluster
 
```bash
export CONDA_ENVS_PATH=<path_to_shared_unlimited_disk>/conda/env
export CONDA_PKGS_DIRS=<path_to_shared_unlimited_disk>/conda/pkgs
```

En général le plus pratique et d'initiliser bash avec conda en rajoutant dans son bashrc :
```bash
# >>> conda initialize >>>
# !! Contents within this block are managed by 'conda init' !!
__conda_setup="$('<path_to_anaconda>/anaconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "<path_to_anaconda>/anaconda3/etc/profile.d/conda.sh" ]; then
        . "<path_to_anaconda>/anaconda3/etc/profile.d/conda.sh"
    else
        export PATH="<path_to_anaconda>/anaconda3/bin:$PATH"
    fi
fi
unset __conda_setup
# <<< conda initialize <<<

export CONDA_ENVS_PATH=<path_to_shared_unlimited_disk>/conda/env
export CONDA_PKGS_DIRS=<path_to_shared_unlimited_disk>/conda/pkgs
```

### Rappels des commandes utiles

```bash
> conda env list # liste des environnment disponible

> conda activate my-env # activate a specific environnement with name "my-env"
> conda deactivate my-env # activate a specific environnement with name "my-env"
```

### Creation et mise à jours des environnements
Les environnements peuvent etre clonés, puis updatés
```bash
conda create --name my-env-p37 python=3.7          # create env with python 3.7
conda update -n my-env-p37 toto=2.1                # add to my-env-p37 package toto version 2.1
conda create --name myclone-env --clone my-env-p37 # clone my-env-p37 to new env myclone-env
```

### Packaging des environnements pour export

conda-pack is a command line tool for creating relocatable conda environments. 
This is useful for deploying code in a consistent environment, potentially in a 
location where python/conda isn’t already installed.

```bash
> conda install -c conda-forge conda-pack
```

Utilisation:

- Sur la machine source
```bash
# Pack environment my_env into my_env.tar.gz
$ conda pack -n my_env

# Pack environment my_env into out_name.tar.gz
$ conda pack -n my_env -o out_name.tar.gz

# Pack environment located at an explicit path into my_env.tar.gz
$ conda pack -p /explicit/path/to/my_envh
```

- Sur la machine cible
```bash
# Unpack environment into directory `my_env`
$ mkdir -p my_env
$ tar -xzf my_env.tar.gz -C my_env

# Use python without activating or fixing the prefixes. Most python
# libraries will work fine, but things that require prefix cleanups
# will fail.
$ ./my_env/bin/python

# Activate the environment. This adds `my_env/bin` to your path
$ source my_env/bin/activate

# Run python from in the environment
(my_env) $ python

# Cleanup prefixes from in the active environment.
# Note that this command can also be run without activating the environment
# as long as some version of python is already installed on the machine.
(my_env) $ conda-unpack

# At this point the environment is exactly as if you installed it here
# using conda directly. All scripts should work fine.
(my_env) $ ipython --version

# Deactivate the environment to remove it from your path
(my_env) $ source my_env/bin/deactivate
```

## III/ MISE EN PLACE DE l ENVIRONNEMENT CONDA ML4CFD-ENV 

- Fichier ml4cfd-env.yml :
 
 ```bash
 name: ml4cfd-env
channels:
- conda-forge/label/cf202003
- pytorch
dependencies:
- python=3.6
- multiprocess
- matplotlib
- cudatoolkit=10.0
- numpy
- scipy
- pytorch
 ```
 
 - Fichier ml4cfd-env-requirement.txt
 ```bash
 pymesh2==0.2.1
 pygmsh==6.0.2
 ```
- Creation d un environnement conda avec les commandes suivantes
 ```bash
 conda env create -f ml4cfd-env.yml
 pip install -r ml4cfd-env-requirement.txt
 ```

- Activation de l'environnement :
```bash
conda activate ml4cfd-env
```

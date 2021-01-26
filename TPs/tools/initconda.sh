# >>> conda initialize >>>
# !! Contents within this block are managed by 'conda init' !!
__conda_setup="$('/work/gratienj/local/anaconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/work/gratienj/local/anaconda3/etc/profile.d/conda.sh" ]; then
        . "/work/gratienj/local/anaconda3/etc/profile.d/conda.sh"
    else
        export PATH="/work/gratienj/local/anaconda3/bin:$PATH"
    fi
fi
unset __conda_setup
export PATH=/work/gratienj/local/pycharm/pycharm-community-2020.3/bin:$PATH
export CONDA_ENVS_PATH=/work/gratienj/local/conda/env
export CONDA_PKGS_DIRS=/work/gratienj/local/conda/pkgs


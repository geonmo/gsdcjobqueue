#!/usr/bin/env bash

cat <<EOF > shell
#!/usr/bin/env bash


if [ "\$1" == "" ]; then
  export COFFEA_IMAGE=/cms/container_images/coffea-dask-cc7.sif
else
  export COFFEA_IMAGE=\$1
fi

APPTAINER_SHELL=\$(which bash) apptainer exec -B \${PWD}:/srv -B /cvmfs -B /cms_scratch -B /etc/condor --pwd /srv \${COFFEA_IMAGE} \\
  /bin/bash --rcfile /srv/.bashrc
EOF

cat <<EOF > .bashrc
install_env() {
  set -e
  echo "Installing shallow virtual environment in \$PWD/.env..."
  python -m venv --without-pip --system-site-packages .env
  unlink .env/lib64  # HTCondor can't transfer symlink to directory and it appears optional
  # work around issues copying CVMFS xattr when copying to tmpdir
  export TMPDIR=\$(mktemp -d -p .)
  .env/bin/python -m ipykernel install --user
  rm -rf \$TMPDIR && unset TMPDIR
  .env/bin/python -m pip install -q git+https://github.com/geonmo/gsdcjobqueue.git
  echo "done."
}

export JUPYTER_PATH=/srv/.jupyter
export JUPYTER_RUNTIME_DIR=/srv/.local/share/jupyter/runtime
export JUPYTER_DATA_DIR=/srv/.local/share/jupyter
export IPYTHONDIR=/srv/.ipython
unset GREP_OPTIONS

[[ -d .env ]] || install_env
source .env/bin/activate
alias pip="python -m pip"
pip show gsdcjobqueue 2>/dev/null | grep -q "Version:" || pip install -q git+https://github.com/geonmo/gsdcjobqueue.git
EOF

chmod u+x shell .bashrc
echo "Wrote shell and .bashrc to current directory. You can delete this file. Run ./shell to start the singularity shell"

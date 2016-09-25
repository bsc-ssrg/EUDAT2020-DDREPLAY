# Dataset Replayer

The Dataset Replayer is a web repository with a REST API capable of storing and retrieving versions of research datasets and associate them to PIDs. The code contained here in is a proof-of-concept prototype for the EUDAT2020 project.

## First time setup
Requirements: python3, virtualenvwrapper
  ```
  mkvirtualenv --python=`which python3` dataset-replayer
  pip install -r requirements.txt
  server/run.py
  ```

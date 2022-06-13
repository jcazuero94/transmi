# Transmilenio

## Project setup
The following instructions where tested in a MacOS Monterrey<br>
To setup the project first creade conda enviroment and activate it<br>
`conda create -n transmi python=3.8`<br>
`conda activate transmi`<br>
Then install kedro and its dependencies<br>
`conda install hdf5`<br>
`pip install -r src/requirements.lock`<br>
`pip install pystan==2.19`<br>
`pip install fbprophet`<br>
Finally install java sdk 11 for pyspark to work <br>

## Project dependencies

To generate or update the dependency requirements for your project:

```
kedro build-reqs
```

This will copy the contents of `src/requirements.txt` into a new file `src/requirements.lock` which will be used as the source for `pip-compile`. You can see the output of the resolution by opening `src/requirements.lock`.

After this, if you'd like to update your project requirements, please update `src/requirements.txt` and re-run `kedro build-reqs`.

[Further information about project dependencies](https://kedro.readthedocs.io/en/stable/04_kedro_project_setup/01_dependencies.html#project-specific-dependencies)

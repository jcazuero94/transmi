# Transmilenio

## Project setup
To setup the project first creade conda enviroment and activate it
`conda create -n transmilenio python==3.8`
`conda activate transmilenio`
Then install kedro and its dependencies
`pip install 'kedro[all]'`
## Project dependencies

To generate or update the dependency requirements for your project:

```
kedro build-reqs
```

This will copy the contents of `src/requirements.txt` into a new file `src/requirements.in` which will be used as the source for `pip-compile`. You can see the output of the resolution by opening `src/requirements.txt`.

After this, if you'd like to update your project requirements, please update `src/requirements.in` and re-run `kedro build-reqs`.

[Further information about project dependencies](https://kedro.readthedocs.io/en/stable/04_kedro_project_setup/01_dependencies.html#project-specific-dependencies)

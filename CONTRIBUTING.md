# Contributing to pygeosys


## Styleguides

This project follows the PEP8 style guide. You can use flake8/black for linting and formatting. 

We have defined a couple of rules to ignore.
In `.vscode` directory, add the following in your `settings.json` file :
```
{
    "python.linting.flake8Enabled": true,
    "python.linting.enabled": true,
    "python.linting.flake8Args": ["--ignore=E501"]
}
```

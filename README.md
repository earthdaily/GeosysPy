# Analytics Generation Pipeline


## Configure project locally

### 1. .env file
You need a .env file to run the example

```
API_CLIENT_ID=
API_CLIENT_SECRET=
API_USERNAME=
API_PASSWORD=
```

### 2. Settings.json for linting/formatting (for developers)

This projet uses flake8/black for linting and formatting.
We have defined a couple of rules to ignore.
```
{
    "python.linting.flake8Enabled": true,
    "python.linting.enabled": true,
    "python.linting.flake8Args": ["--ignore=E501"]
}
```



# Python Azure functions

## Python Azure functions setup.

Init azure function project:

```console
$ func init
$ func init <project-name> --worker-runtime python --model V2
```

Create azure function:

```console
$ func new
$ func new --template "Http Trigger" --name MyHttpTrigger
```

Create python virtual environment:

```console
$ py -m venv .venv
```

Activate python virtual environment:

```console
$ .venv\scripts\activate
```

Add the following requirements to "requirements.txt" file:

- azure-functions
- azure-functions-durable

Install the requirements:

```console
$ python -m pip install -r requirements.txt
```

Start Azurite:

```console
azurite --silent --location c:\azurite
```

Start the Azure functions:

```console
$ func start
```

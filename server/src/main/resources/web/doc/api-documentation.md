## API Documentation for PolyfierServer Functions

### Overview

| clientCode | messageCode      | Description                                                                                         |
|------------|------------------|-----------------------------------------------------------------------------------------------------|
| PCTRL      | PCTRL_SIGN_IN    | Registers a new PolyphenyControl client as an active Client.                                       |
| PCTRL      | PCTRL_SIGN_OUT   | Removes a PolyphenyControl client from the active Clients.                                         |
| PCTRL      | PCTRL_STATUS_UPD | Updates the status of the PolyphenyControl Instance on the server and is sent periodically to keep the connection open.  |
| PCTRL      | PCTRL_REQ_JOB    | Requests a Job for processing on the side of PolyphenyControl.                                     |
| PDB        | PDB_SIGN_IN      | Activates a PolyphenyDB Client that was registered when the associated PolyphenyControl client requested a job.                |
| PDB        | PDB_SIGN_OUT     | Concludes a session with a PolyphenyDB Client, sets the status of the client to inactive.                                    |
| PDB        | PDB_RESULT_DEP   | Deposits a DQL query-result in the server database.                                                |
| PDB        | PDB_STATUS_UPD   | Updates the status of the PolyphenyDB Client on the PolyfierServer and is sent periodically to keep the connection open.       |
| BROWSER    | BROWSER_LOG      | Periodically sent by Browser client to keep connection open and receive log-updates.                                          |
| BROWSER    | BROWSER_SYS      | Periodically sent by Browser client to keep connection open and receive system-updates.                                       |


### PolyphenyControl Client

#### PCTRL_SIGN_IN

Registers a new PolyphenyControl client as an active Client. The branch of the PolyphenyControl instance has to be given by the client.

```json
{
    "apiKey": "<API_KEY>",
    "clientCode": "PCTRL",
    "branch": "<BRANCH>", 
    "messageCode": "PCTRL_SIGN_IN",
    "body": {
        "key": "<PCTRL_KEY>"
    }
}
```

#### PCTRL_SIGN_OUT

Removes a PolyphenyControl client from the active Clients.

```json
{
    "apiKey": "<API_KEY>",
    "clientCode": "PCTRL",
    "messageCode": "PCTRL_SIGN_OUT", 
    "body": {
        "key": "<PCTRL_KEY>"
    }
}
```

#### PCTRL_STATUS_UPD

Updates the status of the PolyphenyControl Instance on the server and is sent 
periodically to keep the connection open.

```json
{
    "apiKey": "<API_KEY>",
    "clientCode": "PCTRL",
    "messageCode": "PCTRL_STATUS_UPD",
    "body": {
        "key": "<PCTRL_KEY>",
        "status": "<STATUS>"
    }
}
```
#### PCTRL_REQ_JOB

Requests a Job for processing on the side of PolyphenyControl. The PolyphenyControl instance provides a unique identifier for the associated PolyphenyDB instance, which the server expects on its job request.

```json
{
    "apiKey": "<API_KEY>",
    "clientCode": "PCTRL",
    "messageCode": "PCTRL_REQ_JOB", 
    "body": {
        "key1": "<PCTRL_KEY>",
        "key2": "<PDB_KEY>"
    }
}
```

### PolyphenyDB Client

#### PDB_SIGN_IN

Activates a PolyphenyDB Client that was registered when the associated PolyphenyControl client requested
a job. 

```json
{
    "apiKey": "<API_KEY>",
    "clientCode": "PDB",
    "messageCode": "PDB_SIGN_IN",
    "body": {
        "key": "<PDB_KEY>"
    }
}
```

#### PDB_SIGN_OUT

Concludes a session with a PolyphenyDB Client, sets the status of the client to inactive.

```json
{
    "apiKey": "<API_KEY>",
    "clientCode": "PDB",
    "messageCode": "PDB_SIGN_OUT",
    "body": {
        "key": "<PDB_KEY>"
    }
}
```

#### PDB_RESULT_DEP

Deposits a DQL query-result in the server database. 

```json
{
    "apiKey": "<API_KEY>",
    "clientCode": "PDB",
    "messageCode": "PDB_RESULT_DEP",
    "body": {
        "pdbKey": "<PDB_KEY>",
        "seed": "<SEED>",
        "resultSetHash": "<RESULT_SET_HASH>",
        "success": "<SUCCESS>",
        "error": "<ERROR>",
        "logical": "<LOGICAL>",
        "physical": "<PHYSICAL>",
        "actual": "<ACTUAL>"
    }
}
```

#### PDB_STATUS_UPD

Updates the status of the PolyphenyDB Client on the PolyfierServer and is sent periodically to keep the connection open.

```json
{
    "apiKey": "<API_KEY>",
    "clientCode": "PDB",
    "messageCode": "PDB_STATUS_UPD",
    "body": {
        "key": "<PDB_KEY>",
        "status": "<STATUS>"
    }
}
```

### Browser Client

#### BROWSER_LOG

Periodically sent by Browser client to keep connection open and receive log-updates.

```json
{
    "clientCode": "BROWSER",
    "messageCode": "BROWSER_LOG"
}
```

#### BROWSER_SYS

Periodically sent by Browser client to keep connection open and receive system-updates.

```json
{
    "clientCode": "BROWSER",
    "messageCode": "BROWSER_SYS"
}
```

# DAS HTTP 
[![License](https://img.shields.io/:license-BSL%201.1-blue.svg)](/licenses/BSL.txt)

[Data Access Service](https://github.com/raw-labs/protocol-das) for making HTTP requests.


## Overview


This DAS plugin defines **one** table called `net_http_request`.
**The `url` must be specified** in your query’s **WHERE** clause, or else an error will be thrown.
Optionally, you can also provide `method`, `request_headers`, and `request_body`.

A typical query looks like:

```sql
SELECT
  response_status_code,
  response_body
FROM
  net_http_request
WHERE
  url = 'https://httpbin.org/post'
  AND method = 'POST'
  AND request_headers = '{"Content-Type": "application/json"}'::jsonb
  AND request_body = '{"hello":"world"}'
```

## Table Schema


| Column Name            | Description                                                                                                               |
|------------------------|---------------------------------------------------------------------------------------------------------------------------|
| `url`                  | The HTTP URL to call (e.g. https://httpbin.org/get). **Must be provided** in the WHERE clause or an exception is thrown.  |                                                                           |
| `method`               | The HTTP method, e.g. GET/POST/PUT. Default GET if not specified                                                          |
| `request_headers`      | The request headers as a JSON object e.g. `{"Accept": "application/json", "User-Agent": "Foo"}`. Defaults to empty object |
| `url_args`             | The URL arguments as a JSON object e.g.`{"debug": "true"}`. Defaults to empty object                                      |
| `request_body`         | The raw request body string. Defaults to empty string                                                                     |
|`follow_redirects`      | Whether the HTTP call should automatically follow redirects. Defaults to false                                            |
| `response_status_code` | The integer status code returned by the HTTP call, as a string (e.g. "200")                                               |
| `response_boody`       | The full response body as a string.                                                                                       |

## How to use

First you need to build the project:
```bash
$ sbt "project docker" "docker:publishLocal"
```

This will create a docker image with the name `das-http`.

Then you can run the image with the following command:
```bash
$ docker run -p 50051:50051 <image_id>
```
... where `<image_id>` is the id of the image created in the previous step.
This will start the server, typically on port 50051.

You can find the image id by looking at the sbt output or by running:
```bash
$ docker images
```

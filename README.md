# DAS Excel
[![License](https://img.shields.io/:license-BSL%201.1-blue.svg)](/licenses/BSL.txt)

[Data Access Service](https://github.com/raw-labs/protocol-das) for making HTTP requests.

## Options



| Name               | Description                                                                               | Default                 | Required |
|--------------------|-------------------------------------------------------------------------------------------|-------------------------|----------|
| `nr_tables`        | TThe number of HTTP endpoints (tables) to expose                                          | 1                       | Yes      |
| `table0_name`      | The name for the first table                                                              | net_http_request_0      | Yes      |
| `table0_url`       | The URL to fetch for the first table                                                      | http://httpbin.org/get  | Yes      |
| `table0_method`    | The region where the first table is defined, e.g."A1:D100"                                |                         | Yes      |
| `table0_headers`   | A comma-separated list of Key:Value headers to send, e.g. `User-Agent:Test,Accept:text/*` |                         | Yes      |
| `table1_name`      | The name for the second table                                                             |                         | Yes      |
| `table1_url`       | The URL for the second table                                                              |                         | Yes      |
| `table1_method`    | The HTTP method for the second table                                                      |                         | Yes      |
| `table1_headers`   | Comma-separated list of HTTP headers for the second table                                 |                         | Yes      |
| `...`              | ... (add more settings for the remainder of the tables) ...                               |                         | Yes      |

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

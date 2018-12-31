# Overview

Forward [ActivityWatch](https://activitywatch.net/) data to
[Riemann](http://riemann.io/). Currently this always exports the
entire history.

# Usage

Assuming everything is running locally, no configuration is needed.

    $ clojure -m aw-riemann.client

If you have a non-standard configuration:

    $ clojure -J-Dapi.url=http://aw-server:5600/api/ \
        -J-Driemann.host=riemann-server \
        -m aw-riemann.client

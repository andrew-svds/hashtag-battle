# Hashtag Battle with Spark Streaming

This is an example Spark streaming app that gives sliding window counts of hashtags and terms on Twitter. It has several available outputs that can be used concurrently: kafka, socket, graphite, local file, and stdout. This usage guide should allow you to build from source, or run the included docker image.

Build with:

    sbt assembly

Usage:

    [ --hashtags list,of,hashtags ] [ --terms more,stuff ]
    [ --duration sec ] [ --window sec ] [ --partitions num ]
    [ --output-stdout ] [ --output-kafka brokers topic ] [ --output-socket host port ]
    [ --output-graphite host port path ] [ --output-file path ]

* Defaults
 - Duration is 5 sec, window is 10 min (600 sec), and number of partitions is 3.
* Notes
 - Requires a twitter4j.properties file in the pwd or classpath.
 - Multiple outputs can be specified, minimum one.
 - Hashtags and/or terms are required

For example to run with spark-submit and output to stdout for the hashtags #love and #twitter:

    spark-submit --class com.svds.hashtag.Battle target/scala-2.10/hashtag-battle-assembly-1.0.jar\
     --hashtags love,twitter --output-stdout

When the app is first started (or restarted) all counts start at 0, it takes 10 minutes (or whatever your window is) to have a full moving window sum.

##Docker

This is also available as a docker image at [svds/hashtag-battle](https://hub.docker.com/r/svds/hashtag-battle/). 
Running requires a valid [Twitter API key](https://dev.twitter.com/apps/new) in the file `twitter4j.properties` (see the [template](twitter4j.properties.template) for formatting details). 

    docker run -it -v $(pwd)/twitter4j.properties:/twitter4j.properties svds/hashtag-battle --hashtags love,twitter --output-stdout

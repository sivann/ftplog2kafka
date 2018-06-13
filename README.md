## About ##
  Monitors a proftpd ExtendedLog file and publishes new files to a kafka topic

## Compile ##

1. install dependencies: 
```govend -v```

2. Build
go build 

## Configuration ##
Configuration file specified at command line in toml format (-c config.toml)

### Proftpd Configuration ### 

We must define a log file appropriate for monitoring.

The following lines must be present in proftpd.conf:

```
ExtendedLog                     /var/log/proftpd/flare-xfer.log WRITE flare
LogFormat                       flare   "%{%Y-%m-%dT%H:%M:%S}t|%a|%u|%F|%f|%b|%{file-size}|%r|%s"
```


### Values ###

* KafkaFlushTimeout: message produce timeout (ms), override with env KAFKA_FLUSH_TIMEOUT
* KafkaMaxPending:   with that many undelivered messages, exit. Override with env KAFKA_MAX_PENDING
* FtpHomePrefix: Prepend this prefix to uploaded filenames, so we can read line samples
* FtpLogFile: FTP Log file to monitor
* Topic:  Kafka topic to submit
* Brokers: Kafka Brokers like localhost:9092"
* StateFile: This file will store the read position and inode of FtpLogFile so on restart we can resume from we left of

### Defaults ###

* FtpLogFile = "/var/log/proftpd/flare-ftp.log"
* StateFile = "/opt/inaccess/var/lib/ftplog2kafka/ftplog2kafka.dat"
* KafkaFlushTimeout = 15000
* KafkaMaxPending = 10
* Topic = "mytopic"

### Workflow ###
 * tailFile goroutine, monitors ftp log file; on successfull log file addition calls kafkaSend to submit data (and related logfile offset) to kafka
 * kafkaProdEvtMon goroutine monitors kafka events; on successfull data submission to kafka saves logfile offset of submitted event



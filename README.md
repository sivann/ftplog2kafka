## About ##
  Monitors a proftpd ExtendedLog file and publishes new files to a kafka topic

## Compile ##

1. install dependencies: 
```govend -v```

2. Build
```go build```

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

## Kafka message format ##

The Sample field includes 10 lines and is  hard-limited to 1500KB.
After uploading export.csv in bbb/ ftp directory, the following gets submitted to kafka:


```
{
  "Type": "FileWrite",
  "Timestamp": "2018-06-12T12:38:00",
  "FileName": "/bbb/export.csv",
  "FileNameFull": "/bbb/export.csv",
  "BytesTransfered": "2533",
  "FileSize": "2533",
  "RemoteIP": "195.46.28.226",
  "Username": "ftp-admin",
  "FTPCommand": "STOR export.csv",
  "FTPResponse": "226",
  "Sample": ";Rosh Pinah-Reference yield (daily)\r\n2018-05-16 00:15:00;6.00\r\n2018-05-16 00:30:00;6.00\r\n2018-05-16 00:45:00;6.00\r\n2018-05-16 01:00:00;6.00\r\n2018-05-16 01:15:00;0.00\r\n2018-05-16 01:30:00;0.00\r\n2018-05-16 01:45:00;0.00\r\n2018-05-16 02:00:00;0.00\r\n2018-05-16 02:15:00;0.00\r\n",
  "LogFileOffset": 1068,
  "LogFileInode": 707122
}
```


### Workflow ###
 * tailFile goroutine, monitors ftp log file; on successfull log file addition calls kafkaSend to submit data (and related logfile offset) to kafka
 * kafkaProdEvtMon goroutine monitors kafka events; on successfull data submission to kafka saves logfile offset of submitted event



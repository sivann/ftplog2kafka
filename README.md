## About ##
  Monitors a proftpd ExtendedLog file and publishes file write events to a kafka topic

## Compile ##

1. install dependencies: 
```govend -v```

2. Build:
```go build```

## Configuration ##
Configuration file specified at command line in toml format (-c config.toml)

### Configuration file Values ###
Configuration file is in toml format. The following settings are recognized:

* KafkaFlushTimeout: message produce timeout (ms)
* KafkaMaxPending:  after that many undelivered messages (in local queue), exit
* FtpHomePrefix: Prepend this prefix to uploaded filenames, so we can read line samples
* FtpPasswd: proftpd AuthUserFile. Used to enrich kafka message with ftp user home dir basenbame.
* FtpLogFile: FTP Log file to monitor
* Topic:  Kafka topic to submit
* Brokers: Kafka Brokers like localhost:9092"
* StateFile: This file will store the read position and inode of FtpLogFile so on restart we can resume from we left of

### Configuration Defaults ###

* FtpLogFile = "/var/log/proftpd/myformat-ftp.log"
* StateFile = "/opt/inaccess/var/lib/ftplog2kafka/ftplog2kafka.dat"
* KafkaFlushTimeout = 15000
* KafkaMaxPending = 10
* Topic = "mytopic"
* FtpPasswd = "/etc/proftpd/ftpd.passwd"


### Proftpd Configuration ### 

You must define a log file of specific format.  The following lines must be present in proftpd.conf:

```
ExtendedLog                     /var/log/proftpd/myformat-xfer.log WRITE myformat
LogFormat                       myformat   "%{%Y-%m-%dT%H:%M:%S}t|%a|%u|%F|%f|%b|%{file-size}|%r|%s"
```

Prevent mkdir:

```
<Directory />
	<Limit MKD XMKD MKDIR RMD XRMD>
		DenyGroup myformat-users
	</Limit>
</Directory>
```

/etc/proftpd/ftpd.group:

```
myformat-users:x:10000:
```




## Kafka message format ##

After uploading export.csv in bbb/ ftp directory, the following gets submitted to kafka:


```json
{
  "Type": "FileWrite",
  "Timestamp": "2018-06-18T15:03:02",
  "FileName": "/accounts.csv",
  "BytesTransfered": "7599",
  "FileSize": "7599",
  "RemoteIP": "195.46.28.226",
  "Username": "bob",
  "HomeBasedir": "c100497a-9854-4c70-bd7e-51ac5d2350f2",
  "FTPCommand": "STOR accounts.csv",
  "FTPResponse": "226",
  "Sample": "Email,Firstname,Lastname,Username,OU,Suspended,ChangePassword,AgreedToTerms,Admin,Aliases,Groups\nxxx@inaccess.com,...",
  "LogFileOffset": 548,
  "LogFileInode": 499451
}
```

* The Sample field includes 10 lines and is  hard-limited to 1500KB.

### Workflow ###
 * tailFile goroutine, monitors ftp log file; on successfull log file addition calls kafkaSend to submit data (and related logfile offset) to kafka
 * kafkaProdEvtMon goroutine monitors kafka events; on successfull data submission to kafka saves logfile offset of submitted event



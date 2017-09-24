Heimdal KDC log analysis code for hadoop

When complete, this project will include analysis jonbs for hadoop for processing large numbers of heimdal
kdc log files. Possible queries include:

- number of authentications per user
- first and last authentication per user
- number of tgs requests for a service
- first and last use of a service
- Top N hosts, users, services, etc
- most common errors
- identify users who only use a small number of services.

At this time, none of these are done, but a single job that does the first four is in progress. ~python~perl scripts that do soemthing similar (as a mapreduce streaming job) are in the ~python~perl subdirectory

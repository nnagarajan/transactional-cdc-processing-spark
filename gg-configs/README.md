## Extract Scipts

```sh
dblogin useridalias c##ggadmin       
register extract ext04 database container (ORCLPDB);
add extract ext04, integrated translog, begin now;
ALTER EXTTRAIL /opt/oracle/gg/gg_home/dirdat/e4, EXTRACT EXT04, MEGABYTES 100

add trandata orclpdb.appuser.ORDERS
add trandata orclpdb.appuser.ORDER_DETAILS
add trandata orclpdb.appuser.ORDER_LINE_ITEMS
```


## Replicat Scripts

```sh
delete replicat kc
add replicat KC,exttrail /opt/oracle/gg/gg_home/dirdat/e4
alter replicat KC extseqno 14 extrba 110968
```


## Troubleshooting

A
SEND REPLICAT REP2 GETLAG                                                                                
SEND REPLICAT REP2 GETLOGEND

SEND EXTRACT EXT04 GETLAG
SEND EXTRACT EXT04 LOGEND
```
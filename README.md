Project focus
------------
This project provides access and several dashboards (tables) for street-level crime in UK.  

Requirements
------------
* Docker >= 20.x.x
* Docker compose >=  1.15.x
* GNU Make > 3.x

Installation
------------
Download the source repository from GitHub:
```shell
git clone 
```
Put CSV files with UK  street-level crime data into:
```
trg-test-1/data
```
The directory with data should have the following structure:
```
trg-test-1
└───data
│   └───2021-05
│       │   2021-05-avon-and-somerset-outcomes.csv
│       │   2021-05-avon-and-somerset-street.csv
```

Application usage
-------------
### Start and stop application.
Start application. This command the starting the docker containers and the seeding the application with data. 
```shell
make start
```

Stop application.
```shell
make stop
```

### The querying data using application.
Get data from the Spark query.
```shell
make query query="SELECT districtName, COUNT(1) FROM data GROUP BY  districtName"
```

### Working with KPIs.
Get data from the Spark using different KPS`s. 

Possible parameters:
* district_name - Name of the district.
* top - The limit for the number of the object.

<ins>__crimes_in_district__</inc>

Displays the distribution of crimes in specific district. Configuration: *district_name*.

Example of command:

```shell
make kpi kpi=crimes_in_district district_name="cambridgeshire"
```

Expected output:
```
Report: crimes_in_district.  District: cambridgeshire.
╒══════════════════════════════╤═══════╕
│ crimeType                    │ cn    │
╞══════════════════════════════╪═══════╡
│ Bicycle theft                │ 3.9%  │
├──────────────────────────────┼───────┤
│ Public order                 │ 11.2% │
├──────────────────────────────┼───────┤
│ Drugs                        │ 2.8%  │
├──────────────────────────────┼───────┤
│ Other crime                  │ 2.6%  │
├──────────────────────────────┼───────┤
│ Robbery                      │ 1.0%  │
├──────────────────────────────┼───────┤
│ Criminal damage and arson    │ 10.1% │
├──────────────────────────────┼───────┤
│ Theft from the person        │ 1.0%  │
├──────────────────────────────┼───────┤
│ Shoplifting                  │ 5.2%  │
├──────────────────────────────┼───────┤
│ Burglary                     │ 4.7%  │
├──────────────────────────────┼───────┤
│ Other theft                  │ 8.4%  │
├──────────────────────────────┼───────┤
│ Possession of weapons        │ 1.1%  │
├──────────────────────────────┼───────┤
│ Violence and sexual offences │ 41.7% │
├──────────────────────────────┼───────┤
│ Vehicle crime                │ 6.3%  │
╘══════════════════════════════╧═══════╛
```


<ins>__top_crimes_loc_in_district__</inc> 

Displays the location with crimes (lat, long) in specific district. Configuration: *district_name*, *top*.

Example of command:
```shell
make kpi kpi=top_crimes_loc_in_district district_name="cambridgeshire" top=3
```

Expected output:
```
Report: top_crimes_loc_in_district.  District: cambridgeshire. Top results: 3.
╒════════════╤═════════════╤═══════╕
│   latitude │   longitude │ cn    │
╞════════════╪═════════════╪═══════╡
│       52.2 │         0.1 │ 18.6% │
├────────────┼─────────────┼───────┤
│       52.6 │        -0.2 │ 17.2% │
├────────────┼─────────────┼───────┤
│       52.6 │        -0.3 │ 11.3% │
╘════════════╧═════════════╧═══════╛
```


<ins>__top_crimes_for_each_district__</ins>

Displays the TOP N types of crimes for each district. Configuration: *top*.

Example of command:
```shell
make kpi kpi=top_crimes_for_each_district top=3
```

Expected output:
```
Report: top_crimes_for_each_district.  District: . Top results: 3.
╒════════════════════╤══════════════════════════════════════╤═══════════════════════════════════╤═══════════════════════════════════╕
│ districtName       │ place:1                              │ place:2                           │ place:3                           │
╞════════════════════╪══════════════════════════════════════╪═══════════════════════════════════╪═══════════════════════════════════╡
│ avon and somerset  │ Violence and sexual offences (42.6%) │ Public order (14.7%)              │ Criminal damage and arson (10.6%) │
├────────────────────┼──────────────────────────────────────┼───────────────────────────────────┼───────────────────────────────────┤
│ bedfordshire       │ Violence and sexual offences (39.9%) │ Public order (11.0%)              │ Criminal damage and arson (9.5%)  │
├────────────────────┼──────────────────────────────────────┼───────────────────────────────────┼───────────────────────────────────┤
│ cambridgeshire     │ Violence and sexual offences (41.7%) │ Public order (11.2%)              │ Criminal damage and arson (10.1%) │

.....................................................................................................................................

│ west mercia        │ Violence and sexual offences (48.9%) │ Criminal damage and arson (9.7%)  │ Public order (9.3%)               │
├────────────────────┼──────────────────────────────────────┼───────────────────────────────────┼───────────────────────────────────┤
│ west midlands      │ Violence and sexual offences (47.2%) │ Public order (9.7%)               │ Vehicle crime (9.2%)              │
├────────────────────┼──────────────────────────────────────┼───────────────────────────────────┼───────────────────────────────────┤
│ west yorkshire     │ Violence and sexual offences (45.9%) │ Public order (14.3%)              │ Criminal damage and arson (9.5%)  │
├────────────────────┼──────────────────────────────────────┼───────────────────────────────────┼───────────────────────────────────┤
│ wiltshire          │ Violence and sexual offences (45.9%) │ Criminal damage and arson (12.7%) │ Public order (9.7%)               │
╘════════════════════╧══════════════════════════════════════╧═══════════════════════════════════╧═══════════════════════════════════╛
```


<ins>__top_outcomes_for_crimes_in_district__</ins>

Displays the TOP N outcomes for each crimes in specific district. Configuration: *district_name*, *top*.
Example of command:
```shell
make kpi kpi=top_outcomes_for_crimes_in_district  district_name="cambridgeshire" top=3
```

Expected output:
```
Report: top_outcomes_for_crimes_in_district.  District: cambridgeshire. Top results: 3.
╒══════════════════════════════╤═══════════════════════════════════════════════════════╤═══════════════════════════════════════════════════════╤═══════════════════════════════════════════════════════╕
│ crimeType                    │ place:1                                               │ place:2                                               │ place:3                                               │
╞══════════════════════════════╪═══════════════════════════════════════════════════════╪═══════════════════════════════════════════════════════╪═══════════════════════════════════════════════════════╡
│ Bicycle theft                │ Investigation complete; no suspect identified (92.3%) │ Unable to prosecute suspect (4.7%)                    │ Suspect charged (1.3%)                                │
├──────────────────────────────┼───────────────────────────────────────────────────────┼───────────────────────────────────────────────────────┼───────────────────────────────────────────────────────┤
│ Burglary                     │ Investigation complete; no suspect identified (76.3%) │ Unable to prosecute suspect (11.0%)                   │ Under investigation (5.7%)                            │
├──────────────────────────────┼───────────────────────────────────────────────────────┼───────────────────────────────────────────────────────┼───────────────────────────────────────────────────────┤
│ Criminal damage and arson    │ Investigation complete; no suspect identified (57.8%) │ Unable to prosecute suspect (27.7%)                   │ Under investigation (4.3%)                            │

.......................................................................................................................................................................................................

│ Vehicle crime                │ Investigation complete; no suspect identified (86.8%) │ Unable to prosecute suspect (8.3%)                    │ Under investigation (2.1%)                            │
├──────────────────────────────┼───────────────────────────────────────────────────────┼───────────────────────────────────────────────────────┼───────────────────────────────────────────────────────┤
│ Violence and sexual offences │ Unable to prosecute suspect (60.8%)                   │ Investigation complete; no suspect identified (12.9%) │ Under investigation (10.4%)                           │
╘══════════════════════════════╧═══════════════════════════════════════════════════════╧═══════════════════════════════════════════════════════╧═══════════════════════════════════════════════════════╛
```


Contributing
------------
For installing addition Python packages:
* Add package to the Dockerfile
```
RUN pip install tabulate==0.8.10
```
* Build new image

```shell
docker build -t . someuser/bitnami-spark:3.3
```
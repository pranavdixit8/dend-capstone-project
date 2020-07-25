class SqlQueries:    
    create_table_immigration_events = ("""

        CREATE TABLE IF NOT EXISTS public.immigration_events(
            cicid int primary key,
            i94port varchar(256),
            year int,
            month int,
            arrival date,
            departure date,
            visaCategory varchar(256),
            gender varchar(256),
            age int,
            duration  int
        )

        """)                                                                                                 
    create_table_ports = ("""
        CREATE TABLE IF NOT EXISTS public.ports (
            i94port varchar(256) primary key,
            dt date,
            city varchar(256),
            country varchar(256),
            lattitude varchar(256),
            longitude varchar(256),
            averageTemperature numeric(18,0),
            averageTemperatureUncertainty numeric(18,0)
            
        )
        """)
    
    
# Fonction qui prend en entrée une question ex. Question 1, ensuite renvoie la requette SQL finale ou intermédaire pour traiter la question. 
def questions_sql_query(question:str):
    questions_dict = { 
        "Question 1" : """SELECT A.Name AS Airline_Name, COUNT(*) AS Number_of_Flights
                        FROM flights AS F
                        JOIN airlines AS A
                        ON F.airline_icao = A.ICAO 
                        --AND F.airline_iata = A.Code
                        WHERE F.airline_icao <> "" AND F.airline_iata <> "" 
                        AND F.ground_speed > 0
                        AND F.time >= (SELECT MAX(min) FROM flights_latest_update_time)
                        GROUP BY A.Name
                        ORDER BY Number_of_Flights DESC
                        LIMIT 1 """ , 
        
        "Question 2" : """
                        WITH RegionalFlights AS (
                            SELECT 
                                A3.name AS airline_name,
                                A2.continent AS origin_continent,
                                COUNT(*) AS number_of_regional_flights,
                                ROW_NUMBER() OVER (PARTITION BY A2.continent ORDER BY COUNT(*) DESC) AS row_num
                            FROM 
                                flights AS F
                            LEFT JOIN 
                                airports_with_continent AS A1 ON F.destination_airport_iata = A1.iata
                            LEFT JOIN 
                                airports_with_continent AS A2 ON F.origin_airport_iata = A2.iata
                            LEFT JOIN 
                                airlines AS A3 ON F.airline_icao = A3.icao
                            WHERE 
                                F.ground_speed > 0
                                AND
                                A2.continent = A1.continent
                                AND
                                F.time >= (SELECT MAX(min) FROM flights_latest_update_time)
                            GROUP BY 
                                A3.name, A2.continent
                        )
                        SELECT 
                            origin_continent as continent,
                            airline_name,
                            number_of_regional_flights
                        FROM 
                            RegionalFlights
                        WHERE 
                            row_num = 1
                        ORDER BY 
                            number_of_regional_flights DESC
                        
                        """, 
        "Question 3" : """
                        SELECT 
                        F.id, 
                        F.registration, 
                        F.airline_icao, 
                        F.destination_airport_iata, 
                        F.origin_airport_iata, 
                        F.ground_speed, 
                        A1.name AS origin_airport_name, 
                        A1.country AS origin_country, 
                        A1.latitude AS origin_latitude, 
                        A1.longitude AS origin_longitude,
                        A1.altitude AS origin_altitude,
                        A1.icao AS origin_airport_icao,
                        A2.name AS destination_airport_name, 
                        A2.country AS destination_country, 
                        A2.latitude AS destination_latitude, 
                        A2.longitude AS destination_longitude,
                        A2.altitude AS destination_altitude, 
                        A2.icao AS destination_airport_icao
                    FROM 
                        flights AS F
                    LEFT JOIN 
                        airports AS A1 ON F.origin_airport_iata = A1.iata
                    LEFT JOIN 
                        airports AS A2 ON F.destination_airport_iata = A2.iata
                    WHERE 
                        F.destination_airport_iata <> '' 
                        AND F.origin_airport_iata <> '' 
                        AND F.ground_speed > 0
                        AND F.time >= (SELECT MAX(min) FROM flights_latest_update_time)
                    """ ,
        "Question 4" : """
                    SELECT DISTINCT
                        F.id, 
                        F.registration, 
                        F.airline_icao, 
                        F.destination_airport_iata, 
                        F.origin_airport_iata, 
                        F.ground_speed, 
                        A1.name AS origin_airport_name, 
                        A1.country AS origin_country, 
                        A1.latitude AS origin_latitude, 
                        A1.longitude AS origin_longitude,
                        A1.altitude AS origin_altitude,
                        A1.icao AS origin_airport_icao,
                        A1.continent AS origin_airport_continent, 
                        A2.name AS destination_airport_name, 
                        A2.country AS destination_country, 
                        A2.latitude AS destination_latitude, 
                        A2.longitude AS destination_longitude,
                        A2.altitude AS destination_altitude, 
                        A2.icao AS destination_airport_icao, 
                        A2.continent AS destination_airport_continent
                    FROM 
                        flights AS F
                    LEFT JOIN 
                        airports_with_continent AS A1 ON F.origin_airport_iata = A1.iata
                    LEFT JOIN 
                        airports_with_continent AS A2 ON F.destination_airport_iata = A2.iata
                    WHERE 
                        F.destination_airport_iata <> '' 
                        AND F.origin_airport_iata <> '' 
                        AND A2.continent = A1.continent
                    """ , 
        "Question 5" : """
                    SELECT
                        SUBSTRING_INDEX(A.Model, ' ', 1) AS Model,
                        COUNT(*) AS TotalFlights
                    FROM flights AS F
                    LEFT JOIN aircrafts_wikipedia AS A
                    ON F.aircraft_code = A.ICAO
                    WHERE F.ground_speed > 0
                    AND F.time >= (SELECT MAX(min) FROM flights_latest_update_time)
                    GROUP BY SUBSTRING_INDEX(A.Model, ' ', 1)
                    ORDER BY TotalFlights DESC
                    LIMIT 1
                    """, 
        "Question 6" : """

                    WITH TopAirlines AS (
                        SELECT
                            AC.`Country/Region` AS Country,
                            A.Name AS Airline_Name,
                            COUNT(*) AS Flight_Count,
                            ROW_NUMBER() OVER (PARTITION BY AC.`Country/Region` ORDER BY COUNT(*) DESC) AS Rank
                        FROM flights AS F
                        LEFT JOIN airlines AS A ON F.airline_icao = A.ICAO
                        LEFT JOIN airlines_by_country_wikipedia AS AC ON A.ICAO = AC.ICAO AND A.Code = AC.IATA
                        WHERE F.ground_speed > 0 AND AC.`Country/Region` IS NOT NULL
                        GROUP BY AC.`Country/Region`, A.Name
                    )
                    SELECT
                        Country,
                        Airline_Name AS Top_Airline,
                        Flight_Count
                    FROM TopAirlines
                    WHERE Rank <= 3
                    ORDER BY Country, Rank
                    
                    """ , 
        "Question bonus" : """
                    WITH OutgoingFlights AS (
                        SELECT 
                            origin_airport_iata AS airport_iata,
                            COUNT(*) AS outgoing_flights
                        FROM flights
                        GROUP BY origin_airport_iata
                    ),
                    IncomingFlights AS (
                        SELECT 
                            destination_airport_iata AS airport_iata,
                            COUNT(*) AS incoming_flights
                        FROM flights
                        GROUP BY destination_airport_iata
                    ),
                    TotalFlights AS (
                        SELECT
                            COALESCE(o.airport_iata, i.airport_iata) AS airport_iata,
                            COALESCE(outgoing_flights, 0) AS outgoing_flights,
                            COALESCE(incoming_flights, 0) AS incoming_flights
                        FROM OutgoingFlights o
                        FULL OUTER JOIN IncomingFlights i
                        ON o.airport_iata = i.airport_iata
                    )
                    SELECT 
                        a.name AS airport_name,
                        t.airport_iata,
                        t.outgoing_flights,
                        t.incoming_flights,
                        ABS(t.outgoing_flights - t.incoming_flights) AS difference
                    FROM TotalFlights t
                    JOIN airports a
                    ON t.airport_iata = a.iata
                    ORDER BY difference DESC
                    LIMIT 1;
                    
                    """
                    }
    return questions_dict[question]
        
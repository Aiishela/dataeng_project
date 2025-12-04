// Country nodes
LOAD CSV WITH HEADERS FROM 'file:///Olympics_Country.csv' AS row
MERGE (c:Country {noc: row.noc})
SET c.country = row.country;

// Athlete nodes
LOAD CSV WITH HEADERS FROM 'file:///Olympic_Athlete_Bio.csv' AS row
MATCH (c:Country) WHERE row.country_noc = c.noc
MERGE (a:Athlete {athlete_id: toInteger(row.athlete_id)})
SET a.name = row.name, a.sex = row.sex
MERGE (a)-[:REPRESENTS]->(c);

// Olympic_Edition nodes
LOAD CSV WITH HEADERS FROM 'file:///Olympic_Athlete_Event_Results.csv' AS row
MERGE (o:Olympic_Edition {edition_id: toInteger(row.edition_id)})
SET o.title = row.edition, o.country_noc = row.country_noc;

// Olympic_Sport_Event nodes
LOAD CSV WITH HEADERS FROM 'file:///Olympic_Athlete_Event_Results.csv' AS row
MERGE (e:Olympic_Sport_Event {sport: row.sport, event: row.event})
MATCH (a:Athlete) WHERE a.athlete_id = toInteger(row.athlete_id)
MERGE (a)-[r:PARTICIPATED_IN]->(e)
SET r.medal = row.medal;







// Athlete -> Country
MATCH (a:Athlete), (c:Country)
WHERE a.country_noc = c.noc
MERGE (a)-[:REPRESENTS]->(c);

// Athlete -> Olympic_Sport_Event
MATCH (a:Athlete), (e:Olympic_Sport_Event)
WHERE a.athlete_id = toInteger(row.athlete_id)
MERGE (a)-[r:PARTICIPATED_IN]->(e)
SET r.medal = row.medal;

// Olympic_Sport_Event -> Olympic_Edition
MATCH (e:Olympic_Sport_Event), (o:Olympic_Edition)
WHERE row.sport = e.sport AND row.event = e.event AND toInteger(row.edition_id) = o.edition_id
MERGE (e)-[:HELD_IN]->(o);

// Athlete -> Olympic_Edition
MATCH (a:Athlete), (o:Olympic_Edition)
WHERE a.athlete_id = toInteger(row.athlete_id) AND o.edition_id = toInteger(row.edition_id)
MERGE (a)-[:COMPETED_AT]->(o);

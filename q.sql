#746 (18.74%): db error: ERROR: column  [...]  does not exist
#208 (5.22%): db error: ERROR: column  [...]  must appear in the GROUP BY clause or be used in an aggregate function
#75 (1.88%): db error: ERROR: operator does not exist:  [...]  =  [...] 
#14 (0.35%): db error: ERROR: function sum(character varying) does not exist
#11 (0.28%): db error: ERROR: syntax error at or near "WHERE"
#10 (0.25%): db error: ERROR: operator does not exist:  [...]  >  [...] 
#8 (0.20%): db error: ERROR: function avg(character varying) does not exist
#8 (0.20%): db error: ERROR: function avg(text) does not exist
#6 (0.15%): db error: ERROR: for SELECT DISTINCT, ORDER BY expressions must appear in select list
#6 (0.15%): db error: ERROR: operator does not exist:  [...]  <  [...] 
#4 (0.10%): db error: ERROR: syntax error at or near "GROUP"
#4 (0.10%): db error: ERROR: subquery in FROM must have an alias
#3 (0.08%): db error: ERROR: function avg(character) does not exist
#3 (0.08%): db error: ERROR: function sum(character) does not exist
#2 (0.05%): db error: ERROR: operator does not exist:  [...]  >=  [...] 
#1 (0.03%): db error: ERROR: relation  [...]  does not exist
#1 (0.03%): db error: ERROR: syntax error at or near "INTERSECT"
#1 (0.03%): db error: ERROR: missing FROM-clause entry for table 
#1 (0.03%): db error: ERROR: syntax error at or near ";"

#14 (0.35%): db error: ERROR: column  [...]  does not exist
#208 (5.22%): db error: ERROR: column  [...]  must appear in the GROUP BY clause or be used in an aggregate function
#75 (1.88%): db error: ERROR: operator does not exist:  [...]  =  [...] 
#14 (0.35%): db error: ERROR: function sum(character varying) does not exist
#11 (0.28%): db error: ERROR: syntax error at or near "WHERE"
#10 (0.25%): db error: ERROR: operator does not exist:  [...]  >  [...] 
#8 (0.20%): db error: ERROR: function avg(character varying) does not exist
#8 (0.20%): db error: ERROR: function avg(text) does not exist
#6 (0.15%): db error: ERROR: for SELECT DISTINCT, ORDER BY expressions must appear in select list
#6 (0.15%): db error: ERROR: operator does not exist:  [...]  <  [...] 
#4 (0.10%): db error: ERROR: syntax error at or near "GROUP"
#4 (0.10%): db error: ERROR: subquery in FROM must have an alias
#3 (0.08%): db error: ERROR: function avg(character) does not exist
#3 (0.08%): db error: ERROR: function sum(character) does not exist
#2 (0.05%): db error: ERROR: operator does not exist:  [...]  >=  [...] 
#1 (0.03%): db error: ERROR: relation  [...]  does not exist
#1 (0.03%): db error: ERROR: syntax error at or near "INTERSECT"
#1 (0.03%): db error: ERROR: missing FROM-clause entry for table 
#1 (0.03%): db error: ERROR: syntax error at or near ";"


#7 (0.18%): db error: ERROR: column  [...]  does not exist
#4 (0.10%): db error: ERROR: column  [...]  must appear in the GROUP BY clause or be used in an aggregate function
#75 (1.88%): db error: ERROR: operator does not exist:  [...]  =  [...] 
#14 (0.35%): db error: ERROR: function sum(character varying) does not exist
#11 (0.28%): db error: ERROR: syntax error at or near "WHERE"
#10 (0.25%): db error: ERROR: operator does not exist:  [...]  >  [...] 
#8 (0.20%): db error: ERROR: function avg(text) does not exist
#8 (0.20%): db error: ERROR: function avg(character varying) does not exist
#6 (0.15%): db error: ERROR: operator does not exist:  [...]  <  [...] 
#6 (0.15%): db error: ERROR: for SELECT DISTINCT, ORDER BY expressions must appear in select list
#4 (0.10%): db error: ERROR: subquery in FROM must have an alias
#4 (0.10%): db error: ERROR: syntax error at or near "GROUP"
#3 (0.08%): db error: ERROR: function sum(character) does not exist
#3 (0.08%): db error: ERROR: function avg(character) does not exist
#2 (0.05%): db error: ERROR: operator does not exist:  [...]  >=  [...] 
#1 (0.03%): db error: ERROR: missing FROM-clause entry for table 
#1 (0.03%): db error: ERROR: syntax error at or near "INTERSECT"
#1 (0.03%): db error: ERROR: syntax error at or near ";"
#1 (0.03%): db error: ERROR: relation  [...]  does not exist



#19 (0.48%): db error: ERROR: operator does not exist:  [...]  =  [...] 
#14 (0.35%): db error: ERROR: function sum(character varying) does not exist
#11 (0.28%): db error: ERROR: syntax error at or near "WHERE"
#10 (0.25%): db error: ERROR: operator does not exist:  [...]  >  [...] 
#8 (0.20%): db error: ERROR: function avg(text) does not exist
#7 (0.18%): db error: ERROR: column  [...]  does not exist
#6 (0.15%): db error: ERROR: for SELECT DISTINCT, ORDER BY expressions must appear in select list
#6 (0.15%): db error: ERROR: operator does not exist:  [...]  <  [...] 
#6 (0.15%): db error: ERROR: function avg(character varying) does not exist
#4 (0.10%): db error: ERROR: syntax error at or near "GROUP"
#4 (0.10%): db error: ERROR: column  [...]  must appear in the GROUP BY clause or be used in an aggregate function
#3 (0.08%): db error: ERROR: function sum(character) does not exist
#3 (0.08%): db error: ERROR: function avg(character) does not exist
#3 (0.08%): db error: ERROR: subquery in FROM must have an alias
#2 (0.05%): db error: ERROR: operator does not exist:  [...]  >=  [...] 
#1 (0.03%): db error: ERROR: relation  [...]  does not exist
#1 (0.03%): db error: ERROR: missing FROM-clause entry for table 
#1 (0.03%): db error: ERROR: syntax error at or near "INTERSECT"
#1 (0.03%): db error: ERROR: syntax error at or near ";"



-- SELECT T1.name ,  T1.date ,  T2.name FROM race AS T1 JOIN track AS T2 ON T1.track_id  =  T2.track_id;
-- SELECT T2.name, T2.location FROM race AS T1 JOIN track AS T2 ON T1.track_id = T2.track_id GROUP BY T1.track_id, t2.name, t2.location HAVING count(*) = 1;
-- SELECT T2.name, count(*) FROM race AS T1 JOIN track AS T2 ON T1.track_id = T2.track_id GROUP BY T1.track_id, t2.name;
-- SELECT T2.name FROM race AS T1 JOIN track AS T2 ON T1.track_id = T2.track_id GROUP BY T1.track_id, t2.name ORDER BY count(*) DESC LIMIT 1;
-- SELECT name FROM track EXCEPT SELECT T2.name FROM race AS T1 JOIN track AS T2 ON T1.track_id  =  T2.track_id WHERE T1.class  =  'GT';
-- SELECT name FROM track WHERE track_id NOT IN (SELECT track_id FROM race);
-- SELECT Name FROM phone WHERE Phone_id NOT IN (SELECT Phone_ID FROM phone_market);
-- SELECT T2.Name ,  sum(T1.Num_of_stock) FROM phone_market AS T1 JOIN phone AS T2 ON T1.Phone_ID  =  T2.Phone_ID GROUP BY T2.Name;
-- SELECT T2.Name FROM phone_market AS T1 JOIN phone AS T2 ON T1.Phone_ID  =  T2.Phone_ID GROUP BY T2.Name HAVING sum(T1.Num_of_stock)  >=  2000 ORDER BY sum(T1.Num_of_stock) DESC;
-- SELECT T3.Name ,  T2.District FROM phone_market AS T1 JOIN market AS T2 ON T1.Market_ID  =  T2.Market_ID JOIN phone AS T3 ON T1.Phone_ID  =  T3.Phone_ID;
-- SELECT T3.Name ,  T2.District FROM phone_market AS T1 JOIN market AS T2 ON T1.Market_ID  =  T2.Market_ID JOIN phone AS T3 ON T1.Phone_ID  =  T3.Phone_ID ORDER BY T2.Ranking;
-- SELECT T3.Name FROM phone_market AS T1 JOIN market AS T2 ON T1.Market_ID  =  T2.Market_ID JOIN phone AS T3 ON T1.Phone_ID  =  T3.Phone_ID WHERE T2.Num_of_shops  >  50;
-- SELECT T1.city FROM city AS T1 JOIN hosting_city AS T2 ON T1.city_id = T2.host_city GROUP BY T2.host_city, t1.city ORDER BY count(*) DESC LIMIT 1;
-- SELECT T1.city FROM city AS T1 JOIN hosting_city AS T2 ON T1.city_id = T2.host_city WHERE T2.year  >  2010;
-- SELECT T1.city FROM city AS T1 JOIN temperature AS T2 ON T1.city_id  =  T2.city_id WHERE T2.Feb  >  T2.Jun UNION SELECT T3.city FROM city AS T3 JOIN hosting_city AS T4 ON T3.city_id  =  T4.host_city;
-- SELECT T1.city FROM city AS T1 JOIN temperature AS T2 ON T1.city_id  =  T2.city_id WHERE T2.Mar  <  T2.Dec EXCEPT SELECT T3.city FROM city AS T3 JOIN hosting_city AS T4 ON T3.city_id  =  T4.host_city;
-- SELECT T1.city FROM city AS T1 JOIN temperature AS T2 ON T1.city_id  =  T2.city_id WHERE T2.Mar  <  T2.Jul INTERSECT SELECT T3.city FROM city AS T3 JOIN hosting_city AS T4 ON T3.city_id  =  T4.host_city;
-- SELECT T2.year FROM city AS T1 JOIN hosting_city AS T2 ON T1.city_id  =  T2.host_city WHERE T1.city  =  'Taizhou ( Zhejiang )';
-- SELECT T3.venue FROM city AS T1 JOIN hosting_city AS T2 ON T1.city_id = T2.host_city JOIN MATCH AS T3 ON T2.match_id = T3.match_id WHERE T1.city = 'Nanjing ( Jiangsu )' AND T3.competition = '1994 FIFA World Cup qualification';
-- SELECT t1.gdp, t1.Regional_Population FROM city AS T1 JOIN hosting_city AS T2 ON T1.city_id = T2.host_city GROUP BY t2.Host_City, t1.gdp, t1.regional_population HAVING count(*) > 1;
-- SELECT T2.name FROM membership_register_branch AS T1 JOIN branch AS T2 ON T1.branch_id  =  T2.branch_id JOIN member AS T3 ON T1.member_id  =  T3.member_id WHERE T3.Hometown  =  'Louisville ,  Kentucky' INTERSECT SELECT T2.name FROM membership_register_branch AS T1 JOIN branch AS T2 ON T1.branch_id  =  T2.branch_id JOIN member AS T3 ON T1.member_id  =  T3.member_id WHERE T3.Hometown  =  'Hiram ,  Georgia';
-- SELECT T3.name ,  T2.name FROM membership_register_branch AS T1 JOIN branch AS T2 ON T1.branch_id  =  T2.branch_id JOIN member AS T3 ON T1.member_id  =  T3.member_id ORDER BY T1.register_year;
-- SELECT name ,  city FROM branch WHERE branch_id NOT IN (SELECT branch_id FROM membership_register_branch);
-- SELECT T1.member_name ,  T2.party_name FROM Member AS T1 JOIN party AS T2 ON T1.party_id  =  T2.party_id;
-- SELECT T1.member_name FROM Member AS T1 JOIN party AS T2 ON T1.party_id  =  T2.party_id WHERE T2.Party_name != 'Progress Party';
-- SELECT T2.party_name, count(*) FROM Member AS T1 JOIN party AS T2 ON T1.party_id = T2.party_id GROUP BY T1.party_id, t2.party_name;
-- SELECT T2.party_name FROM Member AS T1 JOIN party AS T2 ON T1.party_id = T2.party_id GROUP BY T1.party_id, t2.party_name ORDER BY count(*) DESC LIMIT 1;
-- SELECT member_name FROM member WHERE party_id  =  3 INTERSECT SELECT member_name FROM member WHERE party_id  =  1;
-- SELECT party_name FROM party WHERE party_id NOT IN (SELECT party_id FROM Member);
-- SELECT T1.company_name FROM culture_company AS T1 JOIN book_club AS T2 ON T1.book_club_id  =  T2.book_club_id WHERE T2.publisher  =  'Alyson';
-- SELECT T1.title ,  T3.book_title FROM movie AS T1 JOIN culture_company AS T2 ON T1.movie_id  =  T2.movie_id JOIN book_club AS T3 ON T3.book_club_id  =  T2.book_club_id WHERE T2.incorporated_in  =  'China';
-- SELECT T2.company_name FROM movie AS T1 JOIN culture_company AS T2 ON T1.movie_id  =  T2.movie_id WHERE T1.year  =  1999;
-- SELECT Aircraft FROM aircraft WHERE Aircraft_ID NOT IN (SELECT Winning_Aircraft FROM MATCH);
-- SELECT T1.Aircraft, COUNT(*) FROM aircraft AS T1 JOIN MATCH AS T2 ON T1.Aircraft_ID = T2.Winning_Aircraft GROUP BY T2.Winning_Aircraft, t1.aircraft;
-- SELECT T1.Aircraft FROM aircraft AS T1 JOIN MATCH AS T2 ON T1.Aircraft_ID = T2.Winning_Aircraft GROUP BY T2.Winning_Aircraft, t1.aircraft HAVING COUNT(*) >= 2;
-- SELECT T1.Aircraft FROM aircraft AS T1 JOIN MATCH AS T2 ON T1.Aircraft_ID = T2.Winning_Aircraft GROUP BY T2.Winning_Aircraft, t1.aircraft ORDER BY COUNT(*) DESC LIMIT 1;
-- SELECT T2.Location ,  T1.Aircraft FROM aircraft AS T1 JOIN MATCH AS T2 ON T1.Aircraft_ID  =  T2.Winning_Aircraft;
-- SELECT name FROM pilot WHERE pilot_id NOT IN (SELECT Winning_Pilot  FROM MATCH WHERE country  =  'Australia');
-- SELECT t1.name ,  t1.age FROM pilot AS t1 JOIN MATCH AS t2 ON t1.pilot_id  =  t2.winning_pilot ORDER BY t1.age LIMIT 1;
-- SELECT t1.name, t1.age FROM pilot AS t1 JOIN MATCH AS t2 ON t1.pilot_id = t2.winning_pilot WHERE t1.age < 30 GROUP BY t2.winning_pilot, t1.name, t1.age ORDER BY count(*) DESC LIMIT 1;
-- SELECT Name FROM wrestler WHERE Wrestler_ID NOT IN (SELECT Wrestler_ID FROM elimination);
-- SELECT T1.Time FROM elimination AS T1 JOIN wrestler AS T2 ON T1.Wrestler_ID  =  T2.Wrestler_ID ORDER BY T2.Days_held DESC LIMIT 1;
-- SELECT T2.Name ,  T1.Elimination_Move FROM elimination AS T1 JOIN wrestler AS T2 ON T1.Wrestler_ID  =  T2.Wrestler_ID;
-- SELECT T2.Name ,  T1.Team FROM elimination AS T1 JOIN wrestler AS T2 ON T1.Wrestler_ID  =  T2.Wrestler_ID ORDER BY T2.Days_held DESC;


-- SELECT DISTINCT
--     T4.*,
--     (
--         SELECT
--             T4.*
--         FROM
--             REGION
--         LIMIT
--             1
--     )
-- FROM
--     (
--         SELECT
--             1 AS C4
--         FROM
--             LINEITEM
--     ) AS T4
-- ORDER BY
--     C4;

-- select
--     (
--         case
--             when false then date '01.01.2124'
--             else date '01.01.2024'
--         end
--     ) - date (
--         case
--             when false then date '01.01.2123'
--             else date '01.01.2023'
--         end
--     );
-- SELECT
--     1,
--     T1.*
-- FROM
--     (
--         SELECT
--             CAST(NULL AS TEXT)
--         FROM
--             NATION
--     ) AS T1
-- INTERSECT
-- SELECT
--     1.2,
--     CAST(NULL AS TEXT)
-- FROM
--     NATION
-- SELECT
--     1 AS C2,
--     T1.*
-- FROM
--     (
--         SELECT
--             CAST(NULL AS TEXT)
--         FROM
--             CUSTOMER
--     ) AS T1
-- INTERSECT
-- (
--     SELECT
--         4.993609530061892,
--         CAST(NULL AS TEXT) AS C3
--     FROM
--         NATION
-- )
-- SELECT
--     T1.*
-- FROM
--     (
--         SELECT
--             CAST(NULL AS BIGINT)
--         FROM
--             NATION
--     ) AS T1
-- INTERSECT
-- SELECT
--     T2.*
-- FROM
--     (
--         SELECT
--             CAST(NULL AS NUMERIC)
--         FROM
--             NATION
--     ) AS T2
-- SELECT
--     T1.*
-- FROM
--     (
--         SELECT
--             CAST(NULL AS INTEGER)
--         FROM
--             NATION
--     ) AS T1
-- INTERSECT
-- SELECT
--     CAST(NULL AS NUMERIC)
-- FROM
--     NATION
-- SELECT
--     CAST(NULL AS INTEGER)
-- FROM
--     NATION
-- INTERSECT
-- SELECT
--     CAST(NULL AS NUMERIC)
-- FROM
--     NATION
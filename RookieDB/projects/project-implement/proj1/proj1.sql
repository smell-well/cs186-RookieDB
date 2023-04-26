-- Before running drop any existing views
DROP VIEW IF EXISTS q0;
DROP VIEW IF EXISTS q1i;
DROP VIEW IF EXISTS q1ii;
DROP VIEW IF EXISTS q1iii;
DROP VIEW IF EXISTS q1iv;
DROP VIEW IF EXISTS q2i;
DROP VIEW IF EXISTS q2ii;
DROP VIEW IF EXISTS q2iii;
DROP VIEW IF EXISTS q3i;
DROP VIEW IF EXISTS q3ii;
DROP VIEW IF EXISTS q3iii;
DROP VIEW IF EXISTS q4i;
DROP VIEW IF EXISTS q4ii;
DROP VIEW IF EXISTS q4iii;
DROP VIEW IF EXISTS q4iv;
DROP VIEW IF EXISTS q4v;
DROP VIEW IF EXISTS lslgtable;

-- Question 0
CREATE VIEW q0(era)
AS
  SELECT max(era)
  FROM pitching
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
    SELECT namefirst, namelast, birthyear
    FROM  people
    WHERE weight > 300
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
    SELECT namefirst, namelast, birthyear
    FROM people
    WHERE namefirst like '% %'
    ORDER BY namefirst ASC
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
    SELECT birthyear, avg(height), count(playerID)
    FROM people
    GROUP BY birthyear
    ORDER BY birthyear ASC
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
    SELECT birthyear, avg(height), count(playerID)
    FROM people
    GROUP BY birthyear
    HAVING avg(height) > 70
    ORDER BY birthyear ASC
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
    SELECT namefirst, namelast, people.playerID as playerid, yearid
    FROM people, halloffame
    WHERE people.playerID = halloffame.playerID and halloffame.inducted = 'Y'
    ORDER BY yearid DESC, people.playerID ASC
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
    SELECT namefirst, namelast, q2i.playerid, schoolid, yearid
    FROM q2i JOIN (
            SELECT playerid, schools.schoolid
            FROM collegeplaying, schools
            WHERE schools.schoolid = collegeplaying.schoolid
              and schoolState = 'CA'
        ) as t ON q2i.playerid = t.playerid
    ORDER BY yearid DESC, q2i.playerid ASC
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
    SELECT q2i.playerid, namefirst, namelast, schoolid
    FROM q2i LEFT JOIN (
        SELECT playerid, schools.schoolid
        FROM collegeplaying, schools
        WHERE schools.schoolid = collegeplaying.schoolid
    ) as t ON q2i.playerid = t.playerid
    ORDER BY q2i.playerid DESC, schoolid ASC
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
    SELECT playerid, namefirst, namelast, yearid, slg
    FROM people NATURAL JOIN (
            SELECT playerid, yearid, ((H + H2B + 2*H3B + 3*HR + 0.0) / (AB + 0.0)) as slg
            FROM batting
            WHERE AB > 50
            ORDER BY slg DESC
            LIMIT 10
        )
    ORDER BY slg DESC, yearid, playerid
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
    SELECT t.playerid, namefirst, namelast, ((sum(H) + sum(H2B) + 2*sum(H3B) + 3*sum(HR) + 0.0) / (sum(AB) + 0.0)) as lslg
    FROM people NATURAL JOIN (
        SELECT playerid, H, H2B, H3B, HR, AB
        FROM batting
    ) as t
    GROUP BY t.playerid
    HAVING sum(AB) > 50
    ORDER BY lslg DESC, playerid ASC
    LIMIT 10
;

-- Question 3iii
CREATE VIEW lslgtable(playerid, lslg)
AS
    SELECT playerid, ((sum(H) + sum(H2B) + 2*sum(H3B) + 3*sum(HR) + 0.0) / (sum(AB) + 0.0)) as lslg
    FROM batting
    GROUP BY playerid
    HAVING sum(AB) > 50
;

CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
    SELECT namefirst, namelast, lslg
    FROM people NATURAL JOIN lslgtable
    WHERE lslg > (
        SELECT lslg
        FROM lslgtable
        WHERE playerid = 'mayswi01'
    )
    ORDER BY lslg DESC, playerid
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg)
AS
    SELECT yearid, MIN(salary), MAX(salary), AVG(salary)
    FROM Salaries
    GROUP BY yearid
    ORDER BY yearid
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
    SELECT binid, 507500 + binid * 3249250.0, 507500.0 + (binid + 1) * 3249250.0, count(*)
    FROM binids, (
            SELECT *
            FROM salaries
            WHERE yearid = '2016'
        ) as t
    WHERE salary >= 507500 + binid * 3249250.0 and salary <= 507500.0 + (binid + 1) * 3249250.0
    GROUP BY binid
    ORDER BY binid
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
    SELECT s2.yearid, s2.min - s1.min, s2.max - s1.max, s2.avg - s1.avg
    FROM q4i s1
        INNER JOIN q4i s2
        ON s2.yearid - 1 = s1.yearid
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  SELECT 1, 1, 1, 1, 1 -- replace this line
;
-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
  SELECT 1, 1 -- replace this line
;


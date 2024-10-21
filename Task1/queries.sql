
-- Список комнат и количество студентов в каждой из них
SELECT room,COUNT(id) 
FROM Campus.students
GROUP BY room;

-- 5 комнат, где самый маленький средний возраст студентов
SELECT room, AVG(DATEDIFF(CURDATE(), birthday) / 365) as age 
FROM Campus.students
GROUP BY room
ORDER BY age
LIMIT 5;

-- 5 комнат с самой большой разницей в возрасте студентов
SELECT room, MAX(DATEDIFF(CURDATE(), birthday)/365)  - MIN(DATEDIFF(CURDATE(), birthday)/365) diff
FROM Campus.students 
GROUP BY room
ORDER BY diff DESC
LIMIT 5;

-- Список комнат где живут разнополые студенты
SELECT room 
FROM Campus.students
GROUP BY room
HAVING COUNT(DISTINCT sex) = 2;

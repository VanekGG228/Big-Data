
--Task1
--Вывести количество фильмов в каждой категории, отсортировать по убыванию.

SELECT category_id,COUNT(*) FROM film_category
GROUP BY category_id;

--Task2
--Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

SELECT a.actor_id, a.first_name, a.last_name, COUNT(r.rental_id) AS rental_count
FROM actor a
JOIN film_actor fa ON a.actor_id = fa.actor_id
JOIN inventory i ON fa.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
GROUP BY a.actor_id
ORDER BY rental_count DESC
LIMIT 10;


--Task3
--Вывести категорию фильмов, на которую потратили больше всего денег.

SELECT c.name, SUM(p.amount) AS total_spent
FROM category c
JOIN film_category fc ON fc.category_id = c.category_id
JOIN film f ON f.film_id = fc.film_id
JOIN inventory i ON f.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
JOIN payment p ON r.rental_id = p.rental_id
GROUP BY c.category_id
ORDER BY total_spent DESC
LIMIT 1;


--Task4
--Вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.

SELECT m.title
FROM film m
LEFT JOIN inventory i ON m.film_id = i.film_id
WHERE i.film_id IS NULL;


--Task5
--Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”.
--Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.

SELECT a.actor_id, a.first_name, a.last_name, COUNT(fa.film_id) AS film_count
FROM actor a
JOIN film_actor fa ON a.actor_id = fa.actor_id
JOIN film m ON fa.film_id = m.film_id
JOIN film_category mc ON m.film_id = mc.film_id
JOIN category c ON mc.category_id = c.category_id
WHERE c.name = 'Children'
GROUP BY a.actor_id
ORDER BY film_count DESC
LIMIT 3;



--Task6
--Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).
--Отсортировать по количеству неактивных клиентов по убыванию.

SELECT ct.city,
    SUM(CASE WHEN c.active = 1 THEN 1 ELSE 0 END) AS active_customers,
    SUM(CASE WHEN c.active = 0 THEN 1 ELSE 0 END) AS inactive_customers
FROM customer c
JOIN address a ON c.address_id = a.address_id
JOIN city ct ON a.city_id = ct.city_id
GROUP BY ct.city_id
ORDER BY inactive_customers DESC;

--Task7
--Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city),
--и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”.
--Написать все в одном запросе.

WITH RentalHours AS (
SELECT c.city, cat.name AS category_name,
        SUM(EXTRACT(EPOCH FROM (r.return_date - r.rental_date)) / 3600) AS total_hours
FROM rental r
JOIN customer cust ON r.customer_id = cust.customer_id
JOIN address a ON cust.address_id = a.address_id
JOIN city c ON a.city_id = c.city_id
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film f ON i.film_id = f.film_id
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category cat ON fc.category_id = cat.category_id
WHERE c.city LIKE '%-%'  or UPPER(c.city) LIKE 'A%'
GROUP BY c.city_id, cat.name
ORDER BY c.city
)

SELECT
    city,
    category_name,
    total_hours
FROM
   ( SELECT
        city,
        category_name,
        total_hours,
        MAX(total_hours) OVER (PARTITION BY city) AS max_hours
    FROM
        RentalHours)
WHERE
    total_hours = max_hours
ORDER BY
    city,
    category_name;



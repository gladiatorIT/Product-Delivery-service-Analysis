--1
with t1 as (SELECT date,
                   count(distinct user_id) as new_users,
                   (sum(count(distinct user_id)) OVER(ORDER BY date))::integer as total_users
            FROM   user_actions 
                    join (SELECT user_id,
                                min(time::date) as date
                          FROM   user_actions
                          GROUP BY user_id) t2 using(user_id)
            GROUP BY date), 
    t3 as (SELECT date,
                  count(distinct courier_id) as new_couriers,
                  (sum(count(distinct courier_id)) OVER(ORDER BY date))::integer as total_couriers
           FROM   courier_actions 
                    join (SELECT courier_id,
                               min(time::date) as date
                          FROM   courier_actions
                          GROUP BY courier_id) t4 using(courier_id)
           GROUP BY date)

SELECT date,
       new_users,
       new_couriers,
       total_users,
       total_couriers
FROM   t1 full join t3 using(date);



--2
with t1 as (SELECT date,
                   count(distinct user_id) as new_users,
                   (sum(count(distinct user_id)) OVER(ORDER BY date))::integer as total_users
            FROM   user_actions 
                   join (SELECT user_id,
                                min(time::date) as date
                         FROM   user_actions
                         GROUP BY user_id) t2 using(user_id)
            GROUP BY date), 
    t3 as (SELECT date,
           count(distinct courier_id) as new_couriers,
           (sum(count(distinct courier_id)) OVER(ORDER BY date))::integer as total_couriers
           FROM   courier_actions 
                  join (SELECT courier_id,
                               min(time::date) as date
                        FROM   courier_actions
                        GROUP BY courier_id) t4 using(courier_id)
           GROUP BY date)
           
SELECT date,
       new_users,
       new_couriers,
       total_users,
       total_couriers,
       round((new_users::numeric / lag(new_users, 1) OVER())*100,
             2) - 100 as new_users_change,
       round((new_couriers::numeric / lag(new_couriers, 1) OVER())*100,
             2) - 100 as new_couriers_change,
       round((total_users::numeric / lag(total_users, 1) OVER())*100,
             2) - 100 as total_users_growth,
       round((total_couriers::numeric / lag(total_couriers, 1) OVER())*100,
             2) - 100 as total_couriers_growth
FROM   t1 full join t3 using(date);


--3
with t1 as (SELECT time::date as date,
                   count(distinct user_id) filter (WHERE order_id not in (SELECT order_id
                                                                          FROM   user_actions
                                                                          WHERE  action = 'cancel_order')) as paying_users
            FROM   user_actions
            GROUP BY time::date
            ORDER BY time::date), 
    t2 as (SELECT time::date as date,
                count(distinct courier_id) filter (WHERE order_id in (SELECT order_id
                                                                      FROM   courier_actions
                                                                      WHERE  action = 'deliver_order')) as active_couriers
           FROM   courier_actions
           GROUP BY time::date
           ORDER BY time::date), 
    t3 as (SELECT date,
                (sum(count(distinct user_id)) OVER(ORDER BY date))::integer as total_users
           FROM   user_actions 
                  join (SELECT user_id,
                              min(time::date) as date
                       FROM   user_actions
                       GROUP BY user_id) t5 using(user_id)
            GROUP BY date), 
    t4 as (SELECT date,
                  (sum(count(distinct courier_id)) OVER(ORDER BY date))::integer as total_couriers
           FROM   courier_actions 
                    join (SELECT courier_id,
                                 min(time::date) as date
                          FROM   courier_actions
                          GROUP BY courier_id) t6 using(courier_id)
           GROUP BY date)
           
SELECT date,
       paying_users,
       active_couriers,
       round((paying_users::numeric / total_users)*100, 2) as paying_users_share,
       round((active_couriers::numeric / total_couriers)*100, 2) as active_couriers_share
FROM   t1 join t2 using(date) join t3 using(date) join t4 using(date)
ORDER BY date;


--4
with t1 as (SELECT time::date as date,
                   count(distinct user_id) filter (WHERE order_id not in (SELECT order_id
                                                                          FROM   user_actions
                                                                          WHERE  action = 'cancel_order')) as paying_users
            FROM   user_actions
            GROUP BY time::date
            ORDER BY time::date), 
    t2 as (SELECT time::date as date,
                user_id,
                count(order_id) as cnt_ord
           FROM   user_actions
           WHERE  order_id not in (SELECT order_id
                                   FROM   user_actions
                                   WHERE  action = 'cancel_order')
                                   GROUP BY date, user_id
                                   ORDER BY date), 
    t3 as (SELECT date,
                  count(user_id) as one_order
           FROM   t2
           WHERE  cnt_ord = 1
           GROUP BY date
           ORDER BY date), 
    t4 as (SELECT date,
                  count(user_id) as over_one_order
           FROM   t2
           WHERE  cnt_ord > 1
           GROUP BY date
           ORDER BY date)
           
SELECT date,
       round((one_order / paying_users::numeric)*100, 2) as single_order_users_share,
       round((over_one_order / paying_users::numeric)*100,
             2) as several_orders_users_share
FROM   t1 join t3 using (date) join t4 using (date)
ORDER BY date;


--5
with t1 as (SELECT time::date as date,
                   count(distinct user_id) as paying_users
            FROM   user_actions
            WHERE  order_id not in (SELECT order_id
                                    FROM   user_actions
                                    WHERE  action = 'cancel_order')
            GROUP BY date), 
     t2 as (SELECT time::date as date,
                   count(distinct courier_id) as active_couriers
            FROM   courier_actions
            WHERE  order_id in (SELECT order_id
                               FROM   courier_actions
                               WHERE  action = 'deliver_order')
            GROUP BY date), 
     t3 as (SELECT creation_time::date as date,
                   count(orders.order_id) as cnt_orders
            FROM  orders 
                join (SELECT order_id
                      FROM   user_actions
                      WHERE  order_id not in (SELECT order_id
                                              FROM   user_actions
                                              WHERE  action = 'cancel_order')) t4
                   ON orders.order_id = t4.order_id
            GROUP BY date
            ORDER BY date)
            
SELECT date,
       round(paying_users / active_couriers::numeric, 2) as users_per_courier,
       round(cnt_orders / active_couriers::numeric, 2) as orders_per_courier
FROM   t1 join t2 using (date) join t3 using (date);


--6
with t as (SELECT max(time)::date as date,
                  order_id,
                  max(time) - min(time) as diff
           FROM   courier_actions
           WHERE  order_id not in (SELECT order_id
                                   FROM   user_actions
                                   WHERE  action = 'cancel_order')
           GROUP BY order_id)

SELECT date,
       (avg(extract(epoch FROM diff)) / 60)::int as minutes_to_deliver
FROM   t
GROUP BY date
ORDER BY date;


--7
with successful_orders as (select extract(hour from creation_time) as hour, count(order_id) as successful_orders
                           from orders
                           where order_id not in (select order_id from user_actions where action='cancel_order')
                           group by hour),
     canceled_orders as (select extract(hour from creation_time) as hour, count(order_id) as canceled_orders
                         from orders
                         where order_id in (select order_id from user_actions where action='cancel_order')
                         group by hour)
                         
select hour::int, successful_orders, canceled_orders,
       round(canceled_orders::numeric / (successful_orders + canceled_orders), 3)  as cancel_rate
from successful_orders join canceled_orders using(hour)
order by hour;

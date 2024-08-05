--1
with o1 as (SELECT creation_time::date as date,
                   order_id,
                   unnest(product_ids) as product_id
            FROM   orders)
SELECT date,
       sum(price) as revenue,
       sum(sum(price)) OVER(ORDER BY date rows between unbounded preceding and current row) as total_revenue,
       round(((sum(price) - lag(sum(price)) OVER(ORDER BY date)) / lag(sum(price)) OVER(ORDER BY date)) * 100,
             2) as revenue_change
FROM   o1 join products p
        ON o1.product_id = p.product_id
WHERE  order_id not in (SELECT order_id
                        FROM   user_actions
                        WHERE  action = 'cancel_order')
GROUP BY date;


--2
with o1 as (SELECT creation_time::date as date,
                   order_id,
                   unnest(product_ids) as product_id
            FROM   orders), 
     o2 as (SELECT time::date as date,
                   count(distinct user_id) as cnt_user,
                   count(distinct user_id) filter (WHERE order_id not in (SELECT order_id
                                                                          FROM   user_actions
                                                                          WHERE  action = 'cancel_order')) as p_user
            FROM   user_actions
            GROUP BY date), 
     o3 as (SELECT date,
                  sum(price) as itog,
                  count(distinct order_id) as cnt_orders
            FROM   o1 join products p
                   ON o1.product_id = p.product_id
            WHERE  order_id not in (SELECT order_id
                                    FROM   user_actions
                                    WHERE  action = 'cancel_order')
            GROUP BY date)
SELECT date,
       round(itog / cnt_user, 2) as arpu,
       round(itog / p_user, 2) as arppu,
       round(itog / cnt_orders, 2) as aov
FROM   o3 join o2 using(date);


--3
with o1 as (SELECT creation_time::date as date,
                   order_id,
                   unnest(product_ids) as product_id
            FROM   orders), 
     o2 as (SELECT date,
                   count(user_id) as cnt_user
            FROM (SELECT min(time)::date as date,
                         user_id
                  FROM user_actions
                  GROUP BY user_id) d
            GROUP BY date),
     o2_2 as (SELECT date,
                     count(user_id) as p_user
              FROM   (SELECT min(time)::date as date,
                             user_id
                      FROM   user_actions
                      WHERE  order_id not in (SELECT order_id
                                              FROM   user_actions
                                              WHERE  action = 'cancel_order')
                      GROUP BY user_id) v
              GROUP BY date), 
     o3 as (SELECT date,
                   sum(price) as itog,
                   count(distinct order_id) as cnt_orders
            FROM   o1 join products p
                   ON o1.product_id = p.product_id
            WHERE  order_id not in (SELECT order_id
                                   FROM   user_actions
                                   WHERE  action = 'cancel_order')
            GROUP BY date),
     o4 as (SELECT o2.date,
                   sum(cnt_user) OVER(ORDER BY date) as count_users,
                   sum(p_user) OVER(ORDER BY date) as paying_users,
                   sum(itog) OVER(ORDER BY date) as itog_total,
                   sum(cnt_orders) OVER(ORDER BY date) as total_orders
            FROM o2 join o2_2 using (date) join o3 using(date))

SELECT date,
       round(itog_total / count_users, 2) as running_arpu,
       round(itog_total / paying_users, 2) as running_arppu,
       round(itog_total / total_orders, 2) as running_aov
FROM o4;


--4
with o1 as (SELECT to_char(creation_time, 'Day') as weekday,
                   date_part('isodow', creation_time) as weekday_number,
                   order_id,
                   unnest(product_ids) as product_id
            FROM   orders
            WHERE  creation_time between '2022-08-26 00:00:00' and '2023-09-08 23:59:59.999'),
     o2 as (SELECT to_char(time, 'Day') as weekday,
                   date_part('isodow', time) as weekday_number,
                   count(distinct user_id) as cnt_user,
                   count(distinct user_id) filter (WHERE order_id not in (SELECT order_id
                                                                          FROM   user_actions
                                                                          WHERE  action = 'cancel_order')) as p_user
            FROM   user_actions
            WHERE  time between '2022-08-26 00:00:00' and '2023-09-08 23:59:59.999'
            GROUP BY weekday, weekday_number),
     o3 as (SELECT weekday,
                   weekday_number,
                   sum(price) as itog,
                   count(distinct order_id) as cnt_orders
            FROM   o1 join products p
                   ON o1.product_id = p.product_id
            WHERE  order_id not in (SELECT order_id
                                    FROM   user_actions
                                    WHERE  action = 'cancel_order')
            GROUP BY weekday, weekday_number)

SELECT weekday,
       o2.weekday_number,
       round(itog / cnt_user, 2) as arpu,
       round(itog / p_user, 2) as arppu,
       round(itog / cnt_orders, 2) as aov
FROM   o3 join o2 using(weekday)
ORDER BY o2.weekday_number;


--5
with t1 as (select creation_time::date as date, order_id, unnest(product_ids) as product_id
            from orders),
     t2 as (select order_id, sum(price) as revenue_order
            from t1 join products using(product_id)
            group by order_id),
     t3 as (select order_id, time::date as date, user_id, revenue_order
            from user_actions join t2 using(order_id)
            where order_id not in (select order_id from user_actions where action = 'cancel_order')),
     t4 as (select user_id, min(time::date) as start_date
            from user_actions
            group by user_id
            order by user_id),
     t5 as (select date, user_id, sum(revenue_order) as itog
           from t3
           group by date, user_id),
     t6 as (select date, sum(itog) as new_users_revenue
           from t4 join t5 on t4.user_id = t5.user_id and t4.start_date = t5.date
           group by date
           order by date),
     t7 as (select date, sum(price) as revenue
            from t1 join products using(product_id)
            where order_id not in (select order_id from user_actions where action = 'cancel_order')
            group by date)
    
select date, 
       revenue, 
       new_users_revenue, 
       round(new_users_revenue * 100 / revenue, 2) as new_users_revenue_share,
       100 - round(new_users_revenue * 100 / revenue, 2) as old_users_revenue_share
from t7 join t6 using(date)
order by date;


--6
with t1 as (select creation_time::date as date, order_id, unnest(product_ids) as product_id
            from orders
            where order_id not in (select order_id from user_actions where action = 'cancel_order')),
     t2 as (select date, order_id, product_id, name, price 
            from t1 join products using(product_id)),
     t3 as (select name as product_name, sum(price) as revenue,
                   sum(price) * 100 / (select sum(price) from t2) as share_in_revenue
            from t2
            group by product_name
            order by revenue desc)
            
select product_name, sum(revenue) as revenue, sum(share_in_revenue) as share_in_revenue
from (select case when share_in_revenue < 0.5 then 'ДРУГОЕ'
            else product_name
            end product_name,
        revenue,
        share_in_revenue
     from t3) d
group by product_name
order by revenue desc;


--7
with t1 as (SELECT creation_time::date as date,
                   order_id,
                   unnest(product_ids) as product_id
            FROM   orders
            WHERE  order_id not in (SELECT order_id
                                    FROM   user_actions
                                    WHERE  action = 'cancel_order')), 
     total_revenue as (SELECT date,
                              sum(price) as revenue
                      FROM   t1 join products using(product_id)
                      GROUP BY date),
     t3 as (SELECT date,
                   sum(pay_day_couriers) as total_pay_day_couriers
            FROM (SELECT time::date as date,
                         courier_id,
                         case 
                         when count(order_id) >= 5 and time::date between '2022-08-01' and '2022-08-31' then 150*count(order_id)+400
                         when count(order_id) >= 5 and time::date between '2022-09-01' and '2022-09-30' then 150*count(order_id)+500
                         else 150 * count(order_id) end pay_day_couriers
                  FROM   courier_actions
                  WHERE  action = 'deliver_order'
                  GROUP BY courier_id, date
                  ORDER BY date) d
            GROUP BY date),
     t4 as (SELECT time::date as date,
                   sum(case when time::date between '2022-08-01' and '2022-08-31' then 140
                            when time::date between '2022-09-01' and '2022-09-30' then 115 end) pay_day_sborka,
                   case when time::date between '2022-08-01' and '2022-08-31' then 120000
                        when time::date between '2022-09-01' and '2022-09-30' then 150000 end izdergki_day
            FROM   user_actions
            WHERE  order_id not in (SELECT order_id
                                    FROM   user_actions
                                    WHERE  action = 'cancel_order')
            GROUP BY date
            ORDER BY time::date),
     total_zatrat as (SELECT date,
                      pay_day_sborka::numeric + izdergki_day::numeric + total_pay_day_couriers::numeric as costs
                      FROM   t4 full join t3 using(date)),
     total_tax as (SELECT date,
                          sum(case when name in ('сахар', 'сухарики', 'сушки', 'семечки', 'масло льняное', 'виноград', 
                          'масло оливковое', 'арбуз', 'батон', 'йогурт', 'сливки', 'гречка', 'овсянка', 'макароны', 
                          'баранина', 'апельсины', 'бублики', 'хлеб', 'горох', 'сметана', 'рыба копченая', 'мука', 
                          'шпроты', 'сосиски', 'свинина', 'рис', 'масло кунжутное', 'сгущенка', 'ананас', 'говядина',
                          'соль', 'рыба вяленая', 'масло подсолнечное', 'яблоки', 'груши', 'лепешка',
                          'молоко', 'курица', 'лаваш', 'вафли', 'мандарины') then round(price * 10/110, 2)
                          else round(price * 20/120, 2) end) tax
                   FROM   t1 join products using(product_id)
                   GROUP BY date
                   ORDER BY date), 
     t6 as (SELECT date,
                   revenue,
                   costs,
                   tax,
                   (revenue - costs - tax) as gross_profit
            FROM   total_zatrat join total_revenue using(date) join total_tax using(date))

SELECT date,
       revenue,
       costs,
       tax,
       (revenue - costs - tax) as gross_profit,
       sum(revenue) OVER(ORDER BY date) as total_revenue,
       sum(costs) OVER(ORDER BY date) as total_costs,
       sum(tax) OVER(ORDER BY date) as total_tax,
       sum(revenue - costs - tax) OVER(ORDER BY date) as total_gross_profit,
       round((revenue - costs - tax) * 100 / revenue, 2) as gross_profit_ratio,
       round(sum(revenue - costs - tax) OVER(ORDER BY date) * 100 / sum(revenue) OVER(ORDER BY date),
             2) as total_gross_profit_ratio
FROM   t6
GROUP BY revenue, costs, tax, date;


--8
with t1 as (select 'Кампания №1' as ads_campaign, round(250000 / count(distinct user_id)::numeric, 2) as cac
from user_actions
where user_id in (8631, 8632, 8638, 8643, 8657, 8673, 8706, 8707, 8715, 8723, 8732, 8739, 8741, 
8750, 8751, 8752, 8770, 8774, 8788, 8791, 8804, 8810, 8815, 8828, 8830, 8845, 
8853, 8859, 8867, 8869, 8876, 8879, 8883, 8896, 8909, 8911, 8933, 8940, 8972, 
8976, 8988, 8990, 9002, 9004, 9009, 9019, 9020, 9035, 9036, 9061, 9069, 9071, 
9075, 9081, 9085, 9089, 9108, 9113, 9144, 9145, 9146, 9162, 9165, 9167, 9175, 
9180, 9182, 9197, 9198, 9210, 9223, 9251, 9257, 9278, 9287, 9291, 9313, 9317, 
9321, 9334, 9351, 9391, 9398, 9414, 9420, 9422, 9431, 9450, 9451, 9454, 9472, 
9476, 9478, 9491, 9494, 9505, 9512, 9518, 9524, 9526, 9528, 9531, 9535, 9550, 
9559, 9561, 9562, 9599, 9603, 9605, 9611, 9612, 9615, 9625, 9633, 9652, 9654, 
9655, 9660, 9662, 9667, 9677, 9679, 9689, 9695, 9720, 9726, 9739, 9740, 9762, 
9778, 9786, 9794, 9804, 9810, 9813, 9818, 9828, 9831, 9836, 9838, 9845, 9871, 
9887, 9891, 9896, 9897, 9916, 9945, 9960, 9963, 9965, 9968, 9971, 9993, 9998, 
9999, 10001, 10013, 10016, 10023, 10030, 10051, 10057, 10064, 10082, 10103, 
10105, 10122, 10134, 10135) and order_id not in (select order_id from user_actions where action = 'cancel_order')
group by ads_campaign),

t2 as (select 'Кампания №2' as ads_campaign, round(250000 / count(distinct user_id)::numeric, 2) as cac
from user_actions
where user_id in (8629, 8630, 8644, 8646, 8650, 8655, 8659, 8660, 8663, 8665, 8670, 8675, 8680, 8681, 
8682, 8683, 8694, 8697, 8700, 8704, 8712, 8713, 8719, 8729, 8733, 8742, 8748, 8754, 
8771, 8794, 8795, 8798, 8803, 8805, 8806, 8812, 8814, 8825, 8827, 8838, 8849, 8851, 
8854, 8855, 8870, 8878, 8882, 8886, 8890, 8893, 8900, 8902, 8913, 8916, 8923, 8929, 
8935, 8942, 8943, 8949, 8953, 8955, 8966, 8968, 8971, 8973, 8980, 8995, 8999, 9000, 
9007, 9013, 9041, 9042, 9047, 9064, 9068, 9077, 9082, 9083, 9095, 9103, 9109, 9117, 
9123, 9127, 9131, 9137, 9140, 9149, 9161, 9179, 9181, 9183, 9185, 9190, 9196, 9203, 
9207, 9226, 9227, 9229, 9230, 9231, 9250, 9255, 9259, 9267, 9273, 9281, 9282, 9289, 
9292, 9303, 9310, 9312, 9315, 9327, 9333, 9335, 9337, 9343, 9356, 9368, 9370, 9383, 
9392, 9404, 9410, 9421, 9428, 9432, 9437, 9468, 9479, 9483, 9485, 9492, 9495, 9497, 
9498, 9500, 9510, 9527, 9529, 9530, 9538, 9539, 9545, 9557, 9558, 9560, 9564, 9567, 
9570, 9591, 9596, 9598, 9616, 9631, 9634, 9635, 9636, 9658, 9666, 9672, 9684, 9692, 
9700, 9704, 9706, 9711, 9719, 9727, 9735, 9741, 9744, 9749, 9752, 9753, 9755, 9757, 
9764, 9783, 9784, 9788, 9790, 9808, 9820, 9839, 9841, 9843, 9853, 9855, 9859, 9863, 
9877, 9879, 9880, 9882, 9883, 9885, 9901, 9904, 9908, 9910, 9912, 9920, 9929, 9930, 
9935, 9939, 9958, 9959, 9961, 9983, 10027, 10033, 10038, 10045, 10047, 10048, 10058, 
10059, 10067, 10069, 10073, 10075, 10078, 10079, 10081, 10092, 10106, 10110, 10113, 10131) and order_id not in (select order_id from user_actions where action = 'cancel_order')
group by ads_campaign)

select * from t1
union 
select * from t2;


--9
with t1 as (select order_id, unnest(product_ids) as product_id
            from orders),
    t2 as (select order_id, product_id, price
            from t1 join products using(product_id)),
    t3 as (select user_id, order_id, product_id, price
            from t2 join user_actions using(order_id)
            where order_id not in (select order_id from user_actions where action = 'cancel_order')),
    t4 as (SELECT 'Кампания № 1' as ads_campaign, round((sum(price) - 250000) * 100 / 250000, 2) as roi
            from t3
            where user_id in (8631, 8632, 8638, 8643, 8657, 8673, 8706, 8707, 8715, 8723, 8732, 8739, 8741, 
            8750, 8751, 8752, 8770, 8774, 8788, 8791, 8804, 8810, 8815, 8828, 8830, 8845, 
            8853, 8859, 8867, 8869, 8876, 8879, 8883, 8896, 8909, 8911, 8933, 8940, 8972, 
            8976, 8988, 8990, 9002, 9004, 9009, 9019, 9020, 9035, 9036, 9061, 9069, 9071, 
            9075, 9081, 9085, 9089, 9108, 9113, 9144, 9145, 9146, 9162, 9165, 9167, 9175, 
            9180, 9182, 9197, 9198, 9210, 9223, 9251, 9257, 9278, 9287, 9291, 9313, 9317, 
            9321, 9334, 9351, 9391, 9398, 9414, 9420, 9422, 9431, 9450, 9451, 9454, 9472, 
            9476, 9478, 9491, 9494, 9505, 9512, 9518, 9524, 9526, 9528, 9531, 9535, 9550, 
            9559, 9561, 9562, 9599, 9603, 9605, 9611, 9612, 9615, 9625, 9633, 9652, 9654, 
            9655, 9660, 9662, 9667, 9677, 9679, 9689, 9695, 9720, 9726, 9739, 9740, 9762, 
            9778, 9786, 9794, 9804, 9810, 9813, 9818, 9828, 9831, 9836, 9838, 9845, 9871, 
            9887, 9891, 9896, 9897, 9916, 9945, 9960, 9963, 9965, 9968, 9971, 9993, 9998, 
            9999, 10001, 10013, 10016, 10023, 10030, 10051, 10057, 10064, 10082, 10103, 
            10105, 10122, 10134, 10135)),
    t5 as (SELECT 'Кампания № 2' as ads_campaign, round((sum(price) - 250000) * 100 / 250000, 2) as roi
            from t3
            where user_id in (8629, 8630, 8644, 8646, 8650, 8655, 8659, 8660, 8663, 8665, 8670, 8675, 8680, 8681, 
            8682, 8683, 8694, 8697, 8700, 8704, 8712, 8713, 8719, 8729, 8733, 8742, 8748, 8754, 
            8771, 8794, 8795, 8798, 8803, 8805, 8806, 8812, 8814, 8825, 8827, 8838, 8849, 8851, 
            8854, 8855, 8870, 8878, 8882, 8886, 8890, 8893, 8900, 8902, 8913, 8916, 8923, 8929, 
            8935, 8942, 8943, 8949, 8953, 8955, 8966, 8968, 8971, 8973, 8980, 8995, 8999, 9000, 
            9007, 9013, 9041, 9042, 9047, 9064, 9068, 9077, 9082, 9083, 9095, 9103, 9109, 9117, 
            9123, 9127, 9131, 9137, 9140, 9149, 9161, 9179, 9181, 9183, 9185, 9190, 9196, 9203, 
            9207, 9226, 9227, 9229, 9230, 9231, 9250, 9255, 9259, 9267, 9273, 9281, 9282, 9289, 
            9292, 9303, 9310, 9312, 9315, 9327, 9333, 9335, 9337, 9343, 9356, 9368, 9370, 9383, 
            9392, 9404, 9410, 9421, 9428, 9432, 9437, 9468, 9479, 9483, 9485, 9492, 9495, 9497, 
            9498, 9500, 9510, 9527, 9529, 9530, 9538, 9539, 9545, 9557, 9558, 9560, 9564, 9567, 
            9570, 9591, 9596, 9598, 9616, 9631, 9634, 9635, 9636, 9658, 9666, 9672, 9684, 9692, 
            9700, 9704, 9706, 9711, 9719, 9727, 9735, 9741, 9744, 9749, 9752, 9753, 9755, 9757, 
            9764, 9783, 9784, 9788, 9790, 9808, 9820, 9839, 9841, 9843, 9853, 9855, 9859, 9863, 
            9877, 9879, 9880, 9882, 9883, 9885, 9901, 9904, 9908, 9910, 9912, 9920, 9929, 9930, 
            9935, 9939, 9958, 9959, 9961, 9983, 10027, 10033, 10038, 10045, 10047, 10048, 10058, 
            10059, 10067, 10069, 10073, 10075, 10078, 10079, 10081, 10092, 10106, 10110, 10113, 10131))

SELECT *
from t4
union 
select *
from t5;


--10
with t1 as (SELECT order_id,
                   unnest(product_ids) as product_id
            FROM   orders), 
     t2 as (SELECT order_id,
                   product_id,
                   price
            FROM   t1 join products using(product_id)),
     t3 as (SELECT time::date as date,
                   user_id,
                   order_id,
                   product_id,
                   price
            FROM   t2 join user_actions using(order_id)
            WHERE  order_id not in (SELECT order_id
                                    FROM   user_actions
                                    WHERE  action = 'cancel_order')),
     t4 as (SELECT 'Кампания № 1' as ads_campaign,
                    user_id,
                    order_id,
                    sum(price) as check
            FROM   t3
            WHERE  user_id in (8631, 8632, 8638, 8643, 8657, 8673, 8706, 8707, 8715, 8723, 8732, 8739, 8741,
                                8750, 8751, 8752, 8770, 8774, 8788, 8791, 8804, 8810, 8815, 8828, 8830, 8845,
                                8853, 8859, 8867, 8869, 8876, 8879, 8883, 8896, 8909, 8911, 8933, 8940, 8972,
                                8976, 8988, 8990, 9002, 9004, 9009, 9019, 9020, 9035, 9036, 9061, 9069, 9071,
                                9075, 9081, 9085, 9089, 9108, 9113, 9144, 9145, 9146, 9162, 9165, 9167, 9175, 
                                9180, 9182, 9197, 9198, 9210, 9223, 9251, 9257, 9278, 9287, 9291, 9313, 9317,
                                9321, 9334, 9351, 9391, 9398, 9414, 9420, 9422, 9431, 9450, 9451, 9454, 9472,
                                9476, 9478, 9491, 9494, 9505, 9512, 9518, 9524, 9526, 9528, 9531, 9535, 9550, 
                                9559, 9561, 9562, 9599, 9603, 9605, 9611, 9612, 9615, 9625, 9633, 9652, 9654,
                                9655, 9660, 9662, 9667, 9677, 9679, 9689, 9695, 9720, 9726, 9739, 9740, 9762, 
                                9778, 9786, 9794, 9804, 9810, 9813, 9818, 9828, 9831, 9836, 9838, 9845, 9871,
                                9887, 9891, 9896, 9897, 9916, 9945, 9960, 9963, 9965, 9968, 9971, 9993, 9998, 
                                9999, 10001, 10013, 10016, 10023, 10030, 10051, 10057, 10064, 10082, 
                                10103, 10105, 10122, 10134, 10135)
                                and date between '2022-09-01' and '2022-09-07'
            GROUP BY user_id, order_id),
     t5 as (SELECT 'Кампания № 2' as ads_campaign,
                    user_id,
                    sum(price) as check
            FROM   t3
            WHERE  user_id in (8629, 8630, 8644, 8646, 8650, 8655, 8659, 8660, 8663, 8665, 8670, 8675, 8680,
                                8681, 8682, 8683, 8694, 8697, 8700, 8704, 8712, 8713, 8719, 8729, 8733, 8742,
                                8748, 8754, 8771, 8794, 8795, 8798, 8803, 8805, 8806, 8812, 8814, 8825, 8827,
                                8838, 8849, 8851, 8854, 8855, 8870, 8878, 8882, 8886, 8890, 8893, 8900, 8902,
                                8913, 8916, 8923, 8929, 8935, 8942, 8943, 8949, 8953, 8955, 8966, 8968, 8971,
                                8973, 8980, 8995, 8999, 9000, 9007, 9013, 9041, 9042, 9047, 9064, 9068, 9077,
                                9082, 9083, 9095, 9103, 9109, 9117, 9123, 9127, 9131, 9137, 9140, 9149, 9161, 
                                9179, 9181, 9183, 9185, 9190, 9196, 9203, 9207, 9226, 9227, 9229, 9230, 9231,
                                9250, 9255, 9259, 9267, 9273, 9281, 9282, 9289, 9292, 9303, 9310, 9312, 9315,
                                9327, 9333, 9335, 9337, 9343, 9356, 9368, 9370, 9383, 9392, 9404, 9410, 9421,
                                9428, 9432, 9437, 9468, 9479, 9483, 9485, 9492, 9495, 9497, 9498, 9500, 9510,
                                9527, 9529, 9530, 9538, 9539, 9545, 9557, 9558, 9560, 9564, 9567, 9570, 9591,
                                9596, 9598, 9616, 9631, 9634, 9635, 9636, 9658, 9666, 9672, 9684, 9692, 9700,
                                9704, 9706, 9711, 9719, 9727, 9735, 9741, 9744, 9749, 9752, 9753, 9755, 9757,
                                9764, 9783, 9784, 9788, 9790, 9808, 9820, 9839, 9841, 9843, 9853, 9855, 9859,
                                9863, 9877, 9879, 9880, 9882, 9883, 9885, 9901, 9904, 9908, 9910, 9912, 9920,
                                9929, 9930, 9935, 9939, 9958, 9959, 9961, 9983, 10027, 10033, 10038, 10045,
                                10047, 10048, 10058, 10059, 10067, 10069, 10073, 10075, 10078, 10079, 10081,
                                10092, 10106, 10110, 10113, 10131)
                           and date between '2022-09-01' and '2022-09-07'
            GROUP BY user_id, order_id),
     t6 as (SELECT ads_campaign,
                   user_id,
                   avg(t4.check) as check
            FROM t4
            GROUP BY ads_campaign, user_id),
     t7 as (SELECT ads_campaign,
                   user_id,
                   avg(t5.check) as check
            FROM t5
            GROUP BY ads_campaign, user_id)


SELECT ads_campaign,
       round(avg(d.check), 2) as avg_check
FROM   (SELECT *
        FROM   t6
        UNION
SELECT *
        FROM   t7) d
GROUP BY ads_campaign
ORDER BY avg_check desc;


--11
with t1 as (select user_id, min(time::date) over(partition by user_id) as start_date, time::date as date 
from user_actions)

select date_trunc('month', start_date)::date as start_month, start_date, date - start_date as day_number,
    round(count(distinct user_id) / (max(count(distinct user_id)) OVER(PARTITION BY  start_date))::numeric, 2) as retention
from t1
group by start_date, date;


--12
with t1 as (SELECT 'Кампания № 1' as ads_campaign,
                   user_id,
                   min(time::date) OVER(PARTITION BY user_id) as start_date,
                   time::date as date
            FROM   user_actions
            WHERE  user_id in (8631, 8632, 8638, 8643, 8657, 8673, 8706, 8707, 8715, 8723, 8732, 8739, 8741, 
                                8750, 8751, 8752, 8770, 8774, 8788, 8791, 8804, 8810, 8815, 8828, 8830, 8845, 
                                8853, 8859, 8867, 8869, 8876, 8879, 8883, 8896, 8909, 8911, 8933, 8940, 8972, 
                                8976, 8988, 8990, 9002, 9004, 9009, 9019, 9020, 9035, 9036, 9061, 9069, 9071, 
                                9075, 9081, 9085, 9089, 9108, 9113, 9144, 9145, 9146, 9162, 9165, 9167, 9175, 
                                9180, 9182, 9197, 9198, 9210, 9223, 9251, 9257, 9278, 9287, 9291, 9313, 9317, 
                                9321, 9334, 9351, 9391, 9398, 9414, 9420, 9422, 9431, 9450, 9451, 9454, 9472, 
                                9476, 9478, 9491, 9494, 9505, 9512, 9518, 9524, 9526, 9528, 9531, 9535, 9550, 
                                9559, 9561, 9562, 9599, 9603, 9605, 9611, 9612, 9615, 9625, 9633, 9652, 9654, 
                                9655, 9660, 9662, 9667, 9677, 9679, 9689, 9695, 9720, 9726, 9739, 9740, 9762, 
                                9778, 9786, 9794, 9804, 9810, 9813, 9818, 9828, 9831, 9836, 9838, 9845, 9871, 
                                9887, 9891, 9896, 9897, 9916, 9945, 9960, 9963, 9965, 9968, 9971, 9993, 9998, 
                                9999, 10001, 10013, 10016, 10023, 10030, 10051, 10057, 10064, 10082, 10103, 
                                10105, 10122, 10134, 10135)
                  and order_id not in (SELECT order_id
                                     FROM   user_actions
                                     WHERE  action = 'cancel_order')),
     t2 as (SELECT 'Кампания № 2' as ads_campaign,
                    user_id,
                    min(time::date) OVER(PARTITION BY user_id) as start_date,
                    time::date as date
            FROM   user_actions
            WHERE  user_id in (8629, 8630, 8644, 8646, 8650, 8655, 8659, 8660, 8663, 8665, 8670, 8675, 8680, 8681, 
                                8682, 8683, 8694, 8697, 8700, 8704, 8712, 8713, 8719, 8729, 8733, 8742, 8748, 8754, 
                                8771, 8794, 8795, 8798, 8803, 8805, 8806, 8812, 8814, 8825, 8827, 8838, 8849, 8851, 
                                8854, 8855, 8870, 8878, 8882, 8886, 8890, 8893, 8900, 8902, 8913, 8916, 8923, 8929, 
                                8935, 8942, 8943, 8949, 8953, 8955, 8966, 8968, 8971, 8973, 8980, 8995, 8999, 9000, 
                                9007, 9013, 9041, 9042, 9047, 9064, 9068, 9077, 9082, 9083, 9095, 9103, 9109, 9117, 
                                9123, 9127, 9131, 9137, 9140, 9149, 9161, 9179, 9181, 9183, 9185, 9190, 9196, 9203, 
                                9207, 9226, 9227, 9229, 9230, 9231, 9250, 9255, 9259, 9267, 9273, 9281, 9282, 9289, 
                                9292, 9303, 9310, 9312, 9315, 9327, 9333, 9335, 9337, 9343, 9356, 9368, 9370, 9383, 
                                9392, 9404, 9410, 9421, 9428, 9432, 9437, 9468, 9479, 9483, 9485, 9492, 9495, 9497, 
                                9498, 9500, 9510, 9527, 9529, 9530, 9538, 9539, 9545, 9557, 9558, 9560, 9564, 9567, 
                                9570, 9591, 9596, 9598, 9616, 9631, 9634, 9635, 9636, 9658, 9666, 9672, 9684, 9692, 
                                9700, 9704, 9706, 9711, 9719, 9727, 9735, 9741, 9744, 9749, 9752, 9753, 9755, 9757, 
                                9764, 9783, 9784, 9788, 9790, 9808, 9820, 9839, 9841, 9843, 9853, 9855, 9859, 9863, 
                                9877, 9879, 9880, 9882, 9883, 9885, 9901, 9904, 9908, 9910, 9912, 9920, 9929, 9930, 
                                9935, 9939, 9958, 9959, 9961, 9983, 10027, 10033, 10038, 10045, 10047, 10048, 10058, 
                                10059, 10067, 10069, 10073, 10075, 10078, 10079, 10081, 10092, 10106, 10110, 10113, 10131)
                    and order_id not in (SELECT order_id
                                         FROM   user_actions
                                         WHERE  action = 'cancel_order')),
     t3 as (SELECT ads_campaign,
                   start_date,
                   date - start_date as day_number,
                   round(count(distinct user_id)::numeric / (max(count(distinct user_id)) OVER(PARTITION BY start_date)), 2) as retention
           FROM   t1
           WHERE  start_date in ('2022-09-01')
           GROUP BY ads_campaign, start_date, date),
     t4 as(SELECT ads_campaign,
                  start_date,
                  date - start_date as day_number,
                  round(count(distinct user_id) / (max(count(distinct user_id)) OVER(PARTITION BY start_date))::numeric, 2) as retention
            FROM   t2
            WHERE  start_date in ('2022-09-01')
            GROUP BY ads_campaign, start_date, date)
            
SELECT ads_campaign,
       start_date,
       day_number,
       case when day_number = 7 then round(retention, 2) 
            else round(retention, 2) end retention
FROM   t3
WHERE  day_number in (0, 1, 7)
UNION
SELECT ads_campaign,
       start_date,
       day_number,
       case when day_number = 1 then round(retention, 2) 
            else round(retention, 2) end retention
FROM   t4
WHERE  day_number in (0, 1, 7)
ORDER BY ads_campaign, day_number;


--13
with t1 as (select creation_time::date as date, order_id, unnest(product_ids) as product_id
            from orders),
     t2 as (select date, order_id, product_id, price
            from t1 left join products using(product_id)
            where order_id not in (select order_id from user_actions where action = 'cancel_order')),
     t3 as (select date, user_id, order_id, product_id, price
            from t2 left join user_actions using(order_id)
            where order_id not in (select order_id from user_actions where action = 'cancel_order')),
     t4 as (select date - min(date) over(partition by user_id) as day, order_id, user_id, sum(price) as itog,
                     case when user_id in (8631, 8632, 8638, 8643, 8657, 8673, 8706, 8707, 8715, 8723, 8732, 8739, 8741, 
                                8750, 8751, 8752, 8770, 8774, 8788, 8791, 8804, 8810, 8815, 8828, 8830, 8845, 
                                8853, 8859, 8867, 8869, 8876, 8879, 8883, 8896, 8909, 8911, 8933, 8940, 8972, 
                                8976, 8988, 8990, 9002, 9004, 9009, 9019, 9020, 9035, 9036, 9061, 9069, 9071, 
                                9075, 9081, 9085, 9089, 9108, 9113, 9144, 9145, 9146, 9162, 9165, 9167, 9175, 
                                9180, 9182, 9197, 9198, 9210, 9223, 9251, 9257, 9278, 9287, 9291, 9313, 9317, 
                                9321, 9334, 9351, 9391, 9398, 9414, 9420, 9422, 9431, 9450, 9451, 9454, 9472, 
                                9476, 9478, 9491, 9494, 9505, 9512, 9518, 9524, 9526, 9528, 9531, 9535, 9550, 
                                9559, 9561, 9562, 9599, 9603, 9605, 9611, 9612, 9615, 9625, 9633, 9652, 9654, 
                                9655, 9660, 9662, 9667, 9677, 9679, 9689, 9695, 9720, 9726, 9739, 9740, 9762, 
                                9778, 9786, 9794, 9804, 9810, 9813, 9818, 9828, 9831, 9836, 9838, 9845, 9871, 
                                9887, 9891, 9896, 9897, 9916, 9945, 9960, 9963, 9965, 9968, 9971, 9993, 9998, 
                                9999, 10001, 10013, 10016, 10023, 10030, 10051, 10057, 10064, 10082, 10103, 
                                10105, 10122, 10134, 10135) then 1
                          when user_id in (8629, 8630, 8644, 8646, 8650, 8655, 8659, 8660, 8663, 8665, 8670, 8675, 8680, 8681, 
                                8682, 8683, 8694, 8697, 8700, 8704, 8712, 8713, 8719, 8729, 8733, 8742, 8748, 8754, 
                                8771, 8794, 8795, 8798, 8803, 8805, 8806, 8812, 8814, 8825, 8827, 8838, 8849, 8851, 
                                8854, 8855, 8870, 8878, 8882, 8886, 8890, 8893, 8900, 8902, 8913, 8916, 8923, 8929, 
                                8935, 8942, 8943, 8949, 8953, 8955, 8966, 8968, 8971, 8973, 8980, 8995, 8999, 9000, 
                                9007, 9013, 9041, 9042, 9047, 9064, 9068, 9077, 9082, 9083, 9095, 9103, 9109, 9117, 
                                9123, 9127, 9131, 9137, 9140, 9149, 9161, 9179, 9181, 9183, 9185, 9190, 9196, 9203, 
                                9207, 9226, 9227, 9229, 9230, 9231, 9250, 9255, 9259, 9267, 9273, 9281, 9282, 9289, 
                                9292, 9303, 9310, 9312, 9315, 9327, 9333, 9335, 9337, 9343, 9356, 9368, 9370, 9383, 
                                9392, 9404, 9410, 9421, 9428, 9432, 9437, 9468, 9479, 9483, 9485, 9492, 9495, 9497, 
                                9498, 9500, 9510, 9527, 9529, 9530, 9538, 9539, 9545, 9557, 9558, 9560, 9564, 9567, 
                                9570, 9591, 9596, 9598, 9616, 9631, 9634, 9635, 9636, 9658, 9666, 9672, 9684, 9692, 
                                9700, 9704, 9706, 9711, 9719, 9727, 9735, 9741, 9744, 9749, 9752, 9753, 9755, 9757, 
                                9764, 9783, 9784, 9788, 9790, 9808, 9820, 9839, 9841, 9843, 9853, 9855, 9859, 9863, 
                                9877, 9879, 9880, 9882, 9883, 9885, 9901, 9904, 9908, 9910, 9912, 9920, 9929, 9930, 
                                9935, 9939, 9958, 9959, 9961, 9983, 10027, 10033, 10038, 10045, 10047, 10048, 10058, 
                              10059, 10067, 10069, 10073, 10075, 10078, 10079, 10081, 10092, 10106, 10110, 10113, 10131) then 2
                            else 0
                            end ads_campaign
            from t3
            group by date, order_id, user_id
            order by date, order_id, user_id),
    t5 as (select concat('Кампания № ', ads_campaign) as ads_campaign, day, 
                  case when ads_campaign = 1 then round(sum(itog) over(partition by ads_campaign order by day)/171::decimal, 2)
                    else round(sum(itog) over(partition by ads_campaign order by day)/234::decimal, 2)
                    end cumulative_arppu,
                    case
                    when ads_campaign = 1 then round(250000/171::decimal, 2)
                    else round(250000/234::decimal, 2)
                    end as cac
           from t4
           where ads_campaign in (1, 2))
    
    
select ads_campaign, day, cumulative_arppu, cac
from t5
group by ads_campaign, day, cumulative_arppu, cac
order by ads_campaign, day;


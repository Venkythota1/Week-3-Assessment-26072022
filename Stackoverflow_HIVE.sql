HIVE:

cd hadoop-2.7.3/
source ~/.bashrc_profile
jps
hive 

create table posts (id int, post_type int, creation_date string, score int, viewcount float, owneruser_id int,title string, 
answercount int, commentcount int ) row format delimited fields terminated by ',';
describe posts;
load local data inpath '/home/venkatesh/desktop/Hive_asses/posts.csv' into table posts;
select * from posts;
select Count(*) from posts;


create table comments (id int, userid int) row format delimited fields terminated by ',';
describe comments;
load local data inpath '/home/venkatesh/desktop/Hive_asses/comments.csv' into table comments;
select * from comments;
select Count(*) from comments;


create table users (id int,reputation int,display_name string,loc string,age int) row format delimited fields terminated by ',';
describe users;
load local data inpath '/home/venkatesh/desktop/Hive_asses/users.csv' into table users;
select * from users;
select Count(*) from users;

create table posttypes (id int, name string) row format delimited fields terminated by ',';
describe posttypes;
load local data inpath '/home/venkatesh/desktop/Hive_asses/posttypes.csv' into table posttypes;
select * from posttypes;
select Count(*) from posttypes;


A)select display_name,viewcount from posts inner join users on (posts.id=users.id) order by viewcount desc limit 5;

B)select avg(users.age) as A from users group by age;

c)select display_name,creation_date from users inner join posts on (users.id=posts.id) order by creation_date asc limit 5;

D)select display_name,commentcount,reputation from users inner join posts on (posts.id=users.id) order by reputation desc limit 5;

E)select display_name from users inner join posts on (posts.id=users.id) order by (answercount+commentcount) desc limit 5;

F)select owneruser_id,display_name,viewcount from users inner join posts on (posts.id=users.id) order by viewcount desc limit 5;

G)select title,owneruser_id,display_name,commentcount from users inner join posts on (posts.id=users.id) order by commentcount desc limit 1;

H)select loc ,count(id) as B from users group by loc order by B desc limit 1;

I)select loc, count(post_type) from users inner join posts on (posts.id=users.id) where loc like '%India%' group by loc;

select loc, count(answercount) from users inner join posts on (posts.id=users.id) where loc like '%India%' group by loc;

select loc, count(commentcount) from users inner join posts on (posts.id=users.id) where loc like '%India%' group by loc;

select loc, count(commentcount+post_type+answercount) from users inner join posts on (posts.id=users.id) where loc like '%India%' group by loc;

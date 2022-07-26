PIG:

cd hadoop-2.7.3/
jps
souce ~/.bashrc_profile
sbin/start-all.sh
sbin/mr-jobhistory-daemon.sh start historyserver
pig


a = load '/home/venkatesh/desktop/Hive_asses/posts.csv' using PigStorage(',') as (id:int, post_type:int, creation_date:chararray, score:int, viewcount:float, owneruser_id:int,title:chararray, answercount:int, commentcount:int );


b = load '/home/venkatesh/desktop/Hive_asses/comments.csv' USING PigStorage(',') as (id:int, userid:int);


c = load '/home/venkatesh/desktop/Hive_asses/users.csv' using PigStorage(',') as (id:int,reputation:int,display_name:chararray,loc:chararray,age:int);


d = load '/home/venkatesh/desktop/Hive_asses/posttypes.csv' USING PigStorage(',') as (id:int, name:chararray);


A) 

e = join a by id, c by id;
f= ORDER e by reputation desc;
g= limit f 1;
h= foreach g generate display_name,post_type;
dump h;

B)
e= group c ALL;
f= foreach e generate AVG(c.age);
dump f;

C)
e = join a by id,c by id;
f = ORDER e by creation_date asc;
g = limit f 1;
h = foreach g generate display_name,creation_date;
dump h;


D)
e = join a by id, c by id;
f= ORDER e by reputation desc;
g= limit f 1;
h= foreach g generate displayname,commentcount;
dump h;

E)
e= join a by id, c by id;
f = ORDER e by score desc;
g= limit f 1;
h= foreach g generate display_name;
dump h;

F)
e = join a by id, c by id;
f= ORDER e by viewcount desc;
g= limit f 1;
h= foreach g generate display_name,owneruser_id;
dump h;

G)
e = join a by id, c by id;
f= ORDER e by commentcount desc;
g= limit f 1;
h= foreach g generate display_name,title;
dump h;

H)
e = join a by id, c by id;
f= group e by loc;
g= foreach f generate group, COUNT(e.owneruser_id );
store g into '/user/pig/';
h= ORDER g by  $01 desc;
i= limit h 1;
dump e;

I)
E = join a by id, c by id;
F= filter E by loc=='India';
G =group F ALL;
H= foreach G generate SUM(F.commentcount),SUM(F.answercount),SUM(F.score);
dump H;




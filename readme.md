Devilfish DBMS System
This is a design for a Database Management System (DBMS) and database. It's licensed under GPL3.0.

Features
Create and use databases
Create, insert into, update, delete from, and drop tables
Add and remove dimensions
Get fuzzy function on two dimensions
Taylor expand a function on two dimensions at a specified point
Import CSV data into a table
Supports various query operations including equality, greater than, less than, greater than or equal to, less than or equal to, and not equal to


Usage
To use this system, you can interact with it through the command line interface. The available commands are:

create database <name>
use <database>
create table <name> (<dimension1>, <dimension2>, ...)
insert into <table> values {<dimension1>: <value1>, <dimension2>: <value2>, ...}
update <table> set {<dimension1>: <value1>, <dimension2>: <value2>, ...} where <key>=<value>
delete from <table> where <key>=<value>
drop table <name>
add dimension <dimension> to <table>
remove dimension <dimension> from <table>
get fuzzy function <table> on <dimension1> and <dimension2>
taylor expand <table> on <dimension1> and <dimension2> at (<x>, <y>) order <n>
import csv <table> from <filepath>
select * from <table> [where <key> <operator> <value>]


Installation
1.
Clone the repository.
2.
Navigate to the project directory.
3.
Run npm install to install the required dependencies.
4.
Run node main.js to start the system.


License
This project is licensed under the GPL3.0 License. See the LICENSE file for details.
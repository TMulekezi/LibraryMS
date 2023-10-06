\i Desktop\assignmnet4\books.sql

SELECT title, publisher, genre, rrp  FROM 


Select title, publisher, genre, rrp, bookid FROM
author INNER JOIN book ON author.authorid = book.authorid WHERE familyname = 'Tolkien';

SELECT bookid, copyid, onloan FROM library INNER JOIN bookcopy ON 
library.libraryid = bookcopy.libraryid WHERE city = 'London';





SELECT title, publisher, genre, rrp, onloan FROM

(Select title, publisher, genre, rrp, bookid FROM
author INNER JOIN book ON author.authorid = book.authorid 
WHERE familyname = 'Tolkien') AS info1

INNER JOIN

(SELECT bookid, copyid, onloan FROM library INNER JOIN bookcopy ON 
library.libraryid = bookcopy.libraryid WHERE city = 'London') AS info2

ON info1.bookid = info2.bookid;

-----------------------------------------------

SELECT title, publisher, genre, rrp, COUNT(title) AS copies FROM

(Select title, publisher, genre, rrp, bookid FROM
author INNER JOIN book ON author.authorid = book.authorid 
WHERE familyname = 'Tolkien') AS info1

INNER JOIN

(SELECT bookid, copyid, onloan FROM library INNER JOIN bookcopy ON 
library.libraryid = bookcopy.libraryid WHERE city = 'London') AS info2

ON info1.bookid = info2.bookid 
WHERE onloan = 'FALSE' GROUP BY (title, publisher, genre, rrp);


----------------------------
SELECT title, publisher, genre, rrp, COUNT(title) AS copies FROM

(SELECT title, publisher, genre, rrp FROM

(Select title, publisher, genre, rrp, bookid FROM
author INNER JOIN book ON author.authorid = book.authorid 
WHERE familyname = 'Tolkien') AS info1

INNER JOIN

(SELECT bookid, copyid, onloan FROM library INNER JOIN bookcopy ON 
library.libraryid = bookcopy.libraryid WHERE city = 'London') AS info2

ON info1.bookid = info2.bookid 
WHERE onloan = 'FALSE') AS final
GROUP BY (title, publisher, genre, rrp);





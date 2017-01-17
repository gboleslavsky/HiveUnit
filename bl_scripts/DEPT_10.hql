DROP TABLE IF EXISTS DEPT_10;

CREATE TABLE DEPT_10 AS 
SELECT e.ename, d.loc, 
		e.deptno as emp_deptno, 
		d.deptno as dept_deptno
FROM 	emp e, dept d
WHERE 	e.deptno = d.deptno
AND 	e.deptno = 10;
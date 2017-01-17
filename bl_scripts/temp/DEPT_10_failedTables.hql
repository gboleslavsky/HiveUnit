drop table if exists dept_10;
CREATE TABLE dept_10 AS 
SELECT e.ename, d.loc, 
		e.deptno as emp_deptno, 
		d.deptno as dept_deptno
FROM 	failed_emp e, failed_dept d
WHERE 	e.deptno = d.deptno
AND 	e.deptno = 10;
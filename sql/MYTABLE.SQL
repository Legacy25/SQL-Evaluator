CREATE TABLE R ( A int, B int );
CREATE TABLE S ( B int, C int );

SELECT R.A, R.B, S.B, S.C FROM R JOIN S ON R.B = S.B;


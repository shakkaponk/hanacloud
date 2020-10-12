SET SCHEMA I340820;

-- テーブルの作成
CREATE TABLE PATIENT (
	ID INTEGER
	, NAME NVARCHAR(8)
	, AGE INTEGER
	, GENDER NVARCHAR(2)
	, ADDRESS NVARCHAR(32)
	, RELIGION NVARCHAR(16)
	, ILLNESS NVARCHAR(16)
);

-- データの投入
INSERT INTO PATIENT VALUES (1, '伊藤', 29, '女', '静岡県浜松市', '神道', 'ガン');
INSERT INTO PATIENT VALUES (2, '黒田', 24, '女', '愛知県豊田市', '無宗教', 'ウイルス感染症');
INSERT INTO PATIENT VALUES (3, '山本', 28, '女', '静岡県浜松市', '仏教', 'ガン');
INSERT INTO PATIENT VALUES (4, '高橋', 27, '男', '岐阜県各務原市', '仏教', '結核');
INSERT INTO PATIENT VALUES (5, '加藤', 24, '女', '愛知県名古屋市', 'キリスト教', '心血管疾患');
INSERT INTO PATIENT VALUES (6, '田中', 23, '男', '岐阜県大垣市', '仏教', '結核');
INSERT INTO PATIENT VALUES (7, '斎藤', 19, '男', '愛知県春日井市', '無宗教', 'ガン');
INSERT INTO PATIENT VALUES (8, '岡田', 29, '男', '岐阜県岐阜市', '無宗教', '心血管疾患');
INSERT INTO PATIENT VALUES (9, '林', 17, '男', '愛知県名古屋市', '無宗教', '心血管疾患');
INSERT INTO PATIENT VALUES (10, '鈴木', 19, '男', '愛知県名古屋市', '仏教', 'ウイルス感染症');

-- 関数の作成
CREATE OR REPLACE FUNCTION AGE_HIERARCHY_FUNC(value INTEGER, levels INTEGER) RETURNS outValue VARCHAR(8)
AS
int_value INTEGER;
range_from INTEGER;
interval INTEGER;
BEGIN
	IF (levels > 0) THEN
		interval := POWER(10, levels);
		range_from := FLOOR(value / interval) * interval;
		outValue := '[' || range_from || '-' || range_from + POWER(10, levels)-1 || ']';
	ELSE
		outValue := value;
	END IF;
END;

-- 住所テーブルの作成
CREATE TABLE ADDRESSES (SUCC NVARCHAR(64), PRED NVARCHAR(64));
-- 住所データの投入（LEVEL 2）
INSERT INTO ADDRESSES VALUES ('日本', NULL);
-- 住所データの投入（LEVEL 1）
INSERT INTO ADDRESSES VALUES ('愛知県', '日本');
INSERT INTO ADDRESSES VALUES ('静岡県', '日本');
INSERT INTO ADDRESSES VALUES ('岐阜県', '日本');
-- 住所データの投入（LEVEL 0）
INSERT INTO ADDRESSES VALUES ('静岡県浜松市', '静岡県');
INSERT INTO ADDRESSES VALUES ('愛知県豊田市', '愛知県');
INSERT INTO ADDRESSES VALUES ('岐阜県各務原市', '岐阜県');
INSERT INTO ADDRESSES VALUES ('愛知県名古屋市', '愛知県');
INSERT INTO ADDRESSES VALUES ('岐阜県大垣市', '岐阜県');
INSERT INTO ADDRESSES VALUES ('愛知県春日井市', '愛知県');
INSERT INTO ADDRESSES VALUES ('岐阜県岐阜市', '岐阜県');
-- 外部階層ビューの作成
CREATE VIEW ADDRESS_HIER AS 
SELECT * FROM HIERARCHY(SOURCE (SELECT SUCC AS NODE_ID, PRED AS PARENT_ID FROM ADDRESSES) SIBLING ORDER BY PARENT_ID, NODE_ID);

-- k-anonymity 匿名化ビューの作成
CREATE VIEW PATIENT_K_ANON (ID, AGE, GENDER, ADDRESS, RELIGION, ILLNESS)
AS SELECT ID, TO_VARCHAR(AGE) AS AGE, GENDER, ADDRESS, RELIGION, ILLNESS FROM PATIENT
WITH ANONYMIZATION (
	ALGORITHM 'K-ANONYMITY' PARAMETERS '{"data_change_strategy": "qualified", "k": 2}'
	COLUMN ID PARAMETERS '{"is_sequence" : true}'
	COLUMN AGE PARAMETERS '{"is_quasi_identifier" : true, "hierarchy" : {"schema" : "I340820", "function" : "AGE_HIERARCHY_FUNC", "levels" : 3}}'
	COLUMN GENDER PARAMETERS '{"is_quasi_identifier" : true, "hierarchy" : {"embedded" : [["男"], ["女"]]}}'
	COLUMN ADDRESS PARAMETERS '{"is_quasi_identifier" : true, "hierarchy" : {"schema" : "I340820", "view" : "ADDRESS_HIER"}}'
	COLUMN RELIGION PARAMETERS '{"is_quasi_identifier" : true, "hierarchy" : {"embedded" : [["仏教"], ["キリスト教"], ["神道"], ["無宗教"]]}}'
);

-- 匿名化ビューのステータス確認
SELECT * FROM M_ANONYMIZATION_VIEWS;
-- 匿名化ビューのリフレッシュ
REFRESH VIEW PATIENT_K_ANON ANONYMIZATION;
-- 元データの取得
SELECT ID, AGE, GENDER, ADDRESS, RELIGION, ILLNESS FROM PATIENT ORDER BY AGE, GENDER, ADDRESS, ILLNESS ;
-- 匿名化データの取得
SELECT ID, AGE, GENDER, ADDRESS, RELIGION, ILLNESS FROM PATIENT_K_ANON ORDER BY AGE, GENDER, ADDRESS, ILLNESS;

-- アクセス権限の付与
GRANT SELECT ON PATIENT_K_ANON TO MAFF;
REVOKE SELECT ON PATIENT_K_ANON FROM MAFF;

-- k-anonymity + l-diversity 匿名化ビューの作成
CREATE VIEW PATIENT_L_DIVE (ID, AGE, GENDER, ADDRESS, RELIGION, ILLNESS)
AS SELECT ID, TO_VARCHAR(AGE) AS AGE, GENDER, ADDRESS, RELIGION, ILLNESS FROM PATIENT
WITH ANONYMIZATION (
	ALGORITHM 'L-DIVERSITY' PARAMETERS '{"data_change_strategy": "qualified", "k": 2, "l" : 2}'
	COLUMN ID PARAMETERS '{"is_sequence" : true}'
	COLUMN AGE PARAMETERS '{"is_quasi_identifier" : true, "hierarchy" : {"schema" : "I340820", "function" : "AGE_HIERARCHY_FUNC", "levels" : 3}}'
	COLUMN GENDER PARAMETERS '{"is_quasi_identifier" : true, "hierarchy" : {"embedded" : [["男"], ["女"]]}}'
	COLUMN ADDRESS PARAMETERS '{"is_quasi_identifier" : true, "hierarchy" : {"schema" : "I340820", "view" : "ADDRESS_HIER"}}'
	COLUMN RELIGION PARAMETERS '{"is_quasi_identifier" : true, "hierarchy" : {"embedded" : [["仏教"], ["キリスト教"], ["神道"], ["無宗教"]]}}'
	COLUMN ILLNESS PARAMETERS '{"is_sensitive" : true}'
);

-- 匿名化ビューのステータス確認
SELECT * FROM M_ANONYMIZATION_VIEWS;
-- 匿名化ビューのリフレッシュ
REFRESH VIEW PATIENT_L_DIVE ANONYMIZATION;
-- 匿名化データの取得
SELECT ID, AGE, GENDER, ADDRESS, RELIGION, ILLNESS FROM PATIENT_L_DIVE ORDER BY AGE, GENDER, ADDRESS, RELIGION;

-- データの追加
INSERT INTO PATIENT VALUES (11, '吉川', 54, '女', '愛知県豊田市', '仏教', '結核');
INSERT INTO PATIENT VALUES (12, '川崎', 57, '女', '愛知県豊田市', '仏教', 'ガン');

-- k-anonymity + l-diversity 匿名化ビューの作成      パラメーター lossあり
DROP VIEW PATIENT_L_DIVE;
CREATE VIEW PATIENT_L_DIVE (ID, AGE, GENDER, ADDRESS, RELIGION, ILLNESS)
AS SELECT ID, TO_VARCHAR(AGE) AS AGE, GENDER, ADDRESS, RELIGION, ILLNESS FROM PATIENT
WITH ANONYMIZATION (
	ALGORITHM 'L-DIVERSITY' PARAMETERS '{"data_change_strategy": "qualified", "k": 2, "l" : 2, "loss":0.3}'
	COLUMN ID PARAMETERS '{"is_sequence" : true}'
	COLUMN AGE PARAMETERS '{"is_quasi_identifier" : true, "hierarchy" : {"schema" : "I340820", "function" : "AGE_HIERARCHY_FUNC", "levels" : 3}}'
	COLUMN GENDER PARAMETERS '{"is_quasi_identifier" : true, "hierarchy" : {"embedded" : [["男"], ["女"]]}}'
	COLUMN ADDRESS PARAMETERS '{"is_quasi_identifier" : true, "hierarchy" : {"schema" : "I340820", "view" : "ADDRESS_HIER"}}'
	COLUMN RELIGION PARAMETERS '{"is_quasi_identifier" : true, "hierarchy" : {"embedded" : [["仏教"], ["キリスト教"], ["神道"], ["無宗教"]]}}'
	COLUMN ILLNESS PARAMETERS '{"is_sensitive" : true}'
);
REFRESH VIEW PATIENT_L_DIVE ANONYMIZATION;
SELECT ID, AGE, GENDER, ADDRESS, RELIGION, ILLNESS FROM PATIENT_L_DIVE ORDER BY AGE, GENDER, ADDRESS, RELIGION;

--匿名化ビューの詳細を取得
CALL GET_ANONYMIZATION_VIEW_STATISTICS('get_names', NULL, 'I340820', 'PATIENT_L_DIVE');
CALL GET_ANONYMIZATION_VIEW_STATISTICS('get_values', NULL, 'I340820', 'PATIENT_L_DIVE');


-- 社員テーブルの作成
CREATE COLUMN TABLE EMPLOYEES (
  ID INTEGER NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  NAME NVARCHAR(50),
  SITE NVARCHAR(20),
  GENDER NVARCHAR(10),
  AGE NVARCHAR(10),
  SALARY DOUBLE
  );
 
INSERT INTO EMPLOYEES VALUES ('Henry Brubaker','Madrid','Male','29',91000);
INSERT INTO EMPLOYEES VALUES ('Jaylene Jennings','Paris','Female','19',67000);
INSERT INTO EMPLOYEES VALUES ('Dean Tavalouris','Boston','Male','20',76000);
INSERT INTO EMPLOYEES VALUES ('Lydia Wong','Dallas','Female','18',81000);
INSERT INTO EMPLOYEES VALUES ('Harvey Denton','Madrid','Male','19',45000);
INSERT INTO EMPLOYEES VALUES ('Pamela Doove','Nantes','Female','21',78000);
INSERT INTO EMPLOYEES VALUES ('Luciana Trujillo','Barcelona','Female','24',92000);
INSERT INTO EMPLOYEES VALUES ('Demarcus Collins','Bordeaux','Male','18',67000);
INSERT INTO EMPLOYEES VALUES ('Edward Tattsyrup','Barcelona','Male','18',78000);
INSERT INTO EMPLOYEES VALUES ('Benjamin Denton','Paris','Male','19',98000);
INSERT INTO EMPLOYEES VALUES ('Joaquin Barry','Madrid','Male','20',34000);
INSERT INTO EMPLOYEES VALUES ('Emily Crane','Bordeaux','Female','21',92000);
INSERT INTO EMPLOYEES VALUES ('Zack Winters','Madrid','Male','20',87000);
INSERT INTO EMPLOYEES VALUES ('Dave Parkes','Madrid','Male','22',65000);
INSERT INTO EMPLOYEES VALUES ('Aryan Lucas','Paris','Male','19',89000);
INSERT INTO EMPLOYEES VALUES ('Pauline Campbell-Jones','Bordeaux','Female','19',91000);
INSERT INTO EMPLOYEES VALUES ('Les McQueen','Barcelona','Male','18',72000);
INSERT INTO EMPLOYEES VALUES ('Sarai Meza','Paris','Female','21',67000);
INSERT INTO EMPLOYEES VALUES ('Geoff Tipps','Boston','Male','20',38000);
INSERT INTO EMPLOYEES VALUES ('Little Don','Barcelona','Male','24',28000);
INSERT INTO EMPLOYEES VALUES ('Marely Strong','Dallas','Male','23',53000);
INSERT INTO EMPLOYEES VALUES ('Papa Reilly','Boston','Male','28',98000);
INSERT INTO EMPLOYEES VALUES ('Stella Hull','Nantes','Female','19',23000);
INSERT INTO EMPLOYEES VALUES ('Hilary Briss','Bordeaux','Male','20',37000);
INSERT INTO EMPLOYEES VALUES ('Ross Gaines','Nantes','Male','18',65000);
INSERT INTO EMPLOYEES VALUES ('Chloe Denton','Paris','Female','20',47000);
INSERT INTO EMPLOYEES VALUES ('Jed Tinsel','Nantes','Male','21',83000);
INSERT INTO EMPLOYEES VALUES ('Stephen Malley','Dallas','Male','24',37000);
INSERT INTO EMPLOYEES VALUES ('Nikki Hollis','Vancouver','Female','23',64000);
INSERT INTO EMPLOYEES VALUES ('Gordon Mikefield','Paris','Male','22',68000);
INSERT INTO EMPLOYEES VALUES ('Tom Booker','Barcelona','Male','24',65000);
INSERT INTO EMPLOYEES VALUES ('Samuel Chignell','Nantes','Male','23',81000);
INSERT INTO EMPLOYEES VALUES ('Tom Logan','Bordeaux','Male','20',83000);
INSERT INTO EMPLOYEES VALUES ('Ulises Villarreal','Paris','Female','19',54000);
INSERT INTO EMPLOYEES VALUES ('Mike King','Nantes','Male','18',56000);
INSERT INTO EMPLOYEES VALUES ('Ira Carlton','Vancouver','Female','23',54000);
INSERT INTO EMPLOYEES VALUES ('Francis Joyce','Nantes','Female','19',76000);
INSERT INTO EMPLOYEES VALUES ('Radclyffe Denton','Paris','Female','21',92000);
INSERT INTO EMPLOYEES VALUES ('Mickey Michaels','Nantes','Male','21',12000);
INSERT INTO EMPLOYEES VALUES ('Judee Levinson','Barcelona','Female','24',87000);
INSERT INTO EMPLOYEES VALUES ('Madrid Hammond','Paris','Male','22',87000);
INSERT INTO EMPLOYEES VALUES ('Christopher Frost','Vancouver','Male','23',75000);
INSERT INTO EMPLOYEES VALUES ('Val Denton','Bordeaux','Female','19',91000);
INSERT INTO EMPLOYEES VALUES ('Terry Lollard','Bordeaux','Male','27',62000);
INSERT INTO EMPLOYEES VALUES ('Reenie Calver','Toronto','Female','18',65000);
INSERT INTO EMPLOYEES VALUES ('Eve Lucero','Bordeaux','Female','21',73000);
INSERT INTO EMPLOYEES VALUES ('Kathleen Estrada','Toronto','Female','23',84000);
INSERT INTO EMPLOYEES VALUES ('Julian Cook','Madrid','Male','22',36000);
INSERT INTO EMPLOYEES VALUES ('Tulip Tattsyrup','Paris','Female','20',43000);
INSERT INTO EMPLOYEES VALUES ('Brian Morgan','Nantes','Male','23',52000);

-- differential privacy 匿名化ビューの作成
CREATE VIEW EMPLOYEES_DIFFERENTIAL_PRIVACY AS
  SELECT ID, SITE, GENDER, AGE, SALARY
  FROM EMPLOYEES
  WITH ANONYMIZATION (
    ALGORITHM 'DIFFERENTIAL_PRIVACY'
    PARAMETERS '{"data_change_strategy": "qualified"}'
    COLUMN ID PARAMETERS '{"is_sequence": true}'
    COLUMN SALARY PARAMETERS '{"is_sensitive": true, "epsilon": 0.5, "sensitivity": 10000}'
  );
 
 -- 匿名化ビューのリフレッシュ
REFRESH VIEW EMPLOYEES_DIFFERENTIAL_PRIVACY ANONYMIZATION;

-- 匿名化データの取得
SELECT * FROM EMPLOYEES_DIFFERENTIAL_PRIVACY;

SELECT E.ID, E.SALARY, A.SALARY FROM EMPLOYEES E INNER JOIN EMPLOYEES_DIFFERENTIAL_PRIVACY A ON (A.ID=E.ID);
SELECT AVG(SALARY) FROM EMPLOYEES UNION SELECT AVG(SALARY) FROM EMPLOYEES_DIFFERENTIAL_PRIVACY;
SELECT 'RAW' AS TYPE, GENDER, AVG(SALARY) AS SALARY FROM EMPLOYEES GROUP BY GENDER UNION SELECT 'ANON' AS TYPE, GENDER, AVG(SALARY) AS SALARY_ANON FROM EMPLOYEES_DIFFERENTIAL_PRIVACY GROUP BY GENDER;

--匿名化ビューの詳細を取得
CALL GET_ANONYMIZATION_VIEW_STATISTICS('get_names', NULL, 'I340820', 'EMPLOYEES_DIFFERENTIAL_PRIVACY');
CALL GET_ANONYMIZATION_VIEW_STATISTICS('get_values', NULL, 'I340820', 'EMPLOYEES_DIFFERENTIAL_PRIVACY');


--初期化
DROP TABLE PATIENT;
DROP TABLE ADDRESSES;
DROP VIEW ADDRESS_HIER;
DROP VIEW PATIENT_K_ANON;
DROP VIEW PATIENT_L_DIVE;
DROP FUNCTION AGE_HIERARCHY_FUNC;
DROP TABLE EMPLOYEES;
DROP VIEW EMPLOYEES_DIFFERENTIAL_PRIVACY;

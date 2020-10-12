--テーブル作成・データ投入
CREATE COLUMN TABLE ILLNESS (
  ID BIGINT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,  -- sequence column
  NAME NVARCHAR(10),   -- identifier
  GENDER NVARCHAR(1) NOT NULL,   -- quasi-identifiers (QIDs) (to be generalized)  
  CITY NVARCHAR(10) NOT NULL,
  ILLNESS NVARCHAR(10) NOT NULL);   -- sensitive data

INSERT INTO ILLNESS VALUES ('山田', 'M', 'Paris', 'BRONCHITIS');
INSERT INTO ILLNESS VALUES ('田中', 'M', 'Munich', 'ANGINA');
INSERT INTO ILLNESS VALUES ('川畑', 'M', 'Nice', 'FLU');
INSERT INTO ILLNESS VALUES ('新井', 'F', 'Munich', 'BROKEN LEG');

--匿名化ビューを作成 k=5
CREATE VIEW ILLNESS_K_ANON (ID, GENDER, LOCATION, ILLNESS)
AS SELECT ID, GENDER, CITY AS LOCATION, ILLNESS
FROM ILLNESS
WITH ANONYMIZATION (ALGORITHM 'K-ANONYMITY'
  PARAMETERS '{"k": 5}'
  COLUMN ID PARAMETERS '{"is_sequence": true}'
  COLUMN GENDER PARAMETERS '{"is_quasi_identifier":true, "hierarchy":{"embedded": [["F"], ["M"]]}}'
  COLUMN LOCATION PARAMETERS '{"is_quasi_identifier":true, "hierarchy":{"embedded": [["Paris", "France"], ["Munich", "Germany"], ["Nice", "France"]]}}');
  
--ビュー作成済み。まだ有効かされていない
SELECT VIEW_NAME, ANONYMIZATION_STATUS, REFRESH_RECOMMENDED FROM M_ANONYMIZATION_VIEWS;

--kが高すぎるため、有効かできません  
REFRESH VIEW ILLNESS_K_ANON ANONYMIZATION;
  
-- k=2に設定変更
ALTER VIEW ILLNESS_K_ANON (ID, GENDER, LOCATION, ILLNESS)
AS SELECT ID, GENDER, CITY AS LOCATION, ILLNESS
FROM ILLNESS
WITH ANONYMIZATION (ALGORITHM 'K-ANONYMITY'
PARAMETERS '{"k": 2}'
COLUMN ID PARAMETERS '{"is_sequence": true}'
COLUMN GENDER PARAMETERS '{"is_quasi_identifier":true, "hierarchy":{"embedded": [["F"], ["M"]]}}'
COLUMN LOCATION PARAMETERS '{"is_quasi_identifier":true, "hierarchy":{"embedded": [["Paris", "France"], ["Munich", "Germany"], ["Nice", "France"]]}}');
  
--ビュー有効化
REFRESH VIEW ILLNESS_K_ANON ANONYMIZATION;
SELECT VIEW_NAME, ANONYMIZATION_STATUS, REFRESH_RECOMMENDED FROM M_ANONYMIZATION_VIEWS;
  
--データ表示
SELECT * FROM ILLNESS_K_ANON;
  
--データアクセス権限付与
GRANT SELECT ON ILLNESS_K_ANON TO ANALYST_USER;

--データ削除
DROP TABLE ILLNESS;
DROP VIEW ILLNESS_K_ANON;

-----------------------------------------------

--顧客識別ID	IDを別IDに置き換え
--契約開始日 	年月に置き換え
--病名ＣＤ    希少疾患は「希少疾患」にまとめる（希少疾患の条件は変更できるできるようにしたい）
--生年月日  	生年月に置換、90歳以上はまとめる
--死亡時年齢  90歳以上はまとめる

CREATE COLUMN TABLE "疾患" (
  ID BIGINT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,  -- sequence column
  NAME NVARCHAR(10),   -- identifier
  GENDER NVARCHAR(1) NOT NULL,   -- quasi-identifiers (QIDs) (to be generalized)  
  CITY NVARCHAR(10) NOT NULL,
  ILLNESS NVARCHAR(10) NOT NULL);   -- sensitive data

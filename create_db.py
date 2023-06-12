import psycopg2
import pandas as pd

print('Выполняю подключение к базе данных')

conn = psycopg2.connect(
       host="localhost",
       database="labor_market",
       user="postgres",
       password="postgres")

print('Подключение выполнено')

organizations_path = 'C:/Users/isaev/MDP/done_files/organizations.csv'
region_path = 'C:/Users/isaev/MDP/done_files/regions.csv'
professions_path = 'C:/Users/isaev/MDP/done_files/professions.csv'
industries_path = 'C:/Users/isaev/MDP/done_files/industries.csv'
cv_path = 'C:/Users/isaev/MDP/done_files/cv.csv'
edu_path = 'C:/Users/isaev/MDP/done_files/edu.csv'
vacancies_path = 'C:/Users/isaev/MDP/done_files/vacancies.csv'

with conn:
    with conn.cursor() as curs:
        curs.execute("DROP TABLE IF EXISTS regions, professions, industries, cv, education, organizations, vacancies;")

print('Создаю и заполняю таблицу regions')     

with conn:
    with conn.cursor() as curs:
        curs.execute("CREATE TABLE regions (region_code bigint, \
                                            region_name text,\
                                            economic_growth numeric, \
                                            medium_salary_difference numeric,\
                                            unemployment_level numeric, \
                                            PRIMARY KEY (region_code)\
                                            );")
with conn:
    with conn.cursor() as curs:
        curs.execute(f"COPY regions(region_code, region_name, economic_growth, medium_salary_difference, \
                                   unemployment_level)\
                        FROM '{region_path}'\
                        DELIMITER ';'\
                        CSV HEADER;")
print('Создаю и заполняю таблицу professions')
with conn:
    with conn.cursor() as curs:
        curs.execute("CREATE TABLE professions (profession_code bigint, \
                                                profession_name text, \
                                                PRIMARY KEY (profession_code)\
                                                );")
with conn:
    with conn.cursor() as curs:
        curs.execute(f"COPY professions(profession_code, profession_name)\
                        FROM '{professions_path}'\
                        DELIMITER ';'\
                        CSV HEADER;")
        
print('Создаю и заполняю таблицу industries')

with conn:
    with conn.cursor() as curs:
        curs.execute("CREATE TABLE industries (industries_code text, \
                                               industries_name text, \
                                               PRIMARY KEY (industries_code)\
                                               );")
with conn:
    with conn.cursor() as curs:
        curs.execute(f"COPY industries(industries_code, industries_name)\
                        FROM '{industries_path}'\
                        DELIMITER ';'\
                        CSV HEADER;")
        
print('Создаю и заполняю таблицу cv')


with conn:
    with conn.cursor() as curs:
        curs.execute("CREATE TABLE cv (id_candidate text, \
                                      id_cv text, \
                                      birthday numeric,\
                                      date_creation text,\
                                      education_type text,\
                                      experience numeric,\
                                      gender text, \
                                      industry_code text, \
                                      position_name text,\
                                      profession_code bigint, \
                                      region_code bigint,\
                                      salary numeric, \
                                      skills text, \
                                      additional_skills text, \
                                      PRIMARY KEY (id_cv),\
                                      FOREIGN KEY (region_code) REFERENCES regions,\
                                      FOREIGN KEY (industry_code) REFERENCES industries,\
                                      FOREIGN KEY (profession_code) REFERENCES professions\
                                      );")
with conn:
    with conn.cursor() as curs:
        curs.execute(f"COPY cv(id_candidate,\
                                id_cv,\
                                birthday,\
                                date_creation,\
                                education_type,\
                                experience,\
                                gender,\
                                industry_code,\
                                position_name,\
                                profession_code,\
                                region_code,\
                                salary,\
                                skills,\
                                additional_skills)\
                        FROM '{cv_path}'\
                        DELIMITER ';'\
                        CSV HEADER;")
        
        
print('Создаю и заполняю таблицу education')


with conn:
    with conn.cursor() as curs:
        curs.execute("CREATE TABLE education (faculty text, \
                                              graduate_year numeric,\
                                              id_cv text, \
                                              legal_name text,\
                                              qualification text, \
                                              speciality text, \
                                              PRIMARY KEY (id_cv, legal_name,\
                                                           graduate_year),\
                                              FOREIGN KEY (id_cv) REFERENCES cv\
                                              );")
with conn:
    with conn.cursor() as curs:
        curs.execute(f"COPY education(faculty, graduate_year, id_cv, legal_name, \
                                      qualification, speciality)\
                        FROM '{edu_path}'\
                        DELIMITER ';'\
                        CSV HEADER;")
        
print('Создаю и заполняю таблицу organizations')


with conn:
    with conn.cursor() as curs:
        curs.execute("CREATE TABLE organizations (id_organization text, \
                                              business_size text,\
                                              inn text, \
                                              legal_name text,\
                                              region_code bigint, \
                                              description text, \
                                              PRIMARY KEY (id_organization),\
                                              FOREIGN KEY (region_code) REFERENCES regions\
                                              );")
with conn:
    with conn.cursor() as curs:
        curs.execute(f"COPY organizations(id_organization, business_size, inn, legal_name, \
                                          region_code, description)\
                        FROM '{organizations_path}'\
                        DELIMITER ';'\
                        CSV HEADER;")
        
print('Создаю и заполняю таблицу vacancies')


with conn:
    with conn.cursor() as curs:
        curs.execute("CREATE TABLE vacancies (date_posted date, \
                                              id_hiring_organization text, \
                                              identifier text,\
                                              title text,\
                                              industry text,\
                                              profession bigint,\
                                              region bigint, \
                                              base_salary_min numeric, \
                                              base_salary_max numeric,\
                                              education_academic_degree text, \
                                              education_requirements_education_type text,\
                                              education_requirements_speciality text, \
                                              experience_requirements numeric, \
                                              job_benefits text, \
                                              requirements_qualifications text, \
                                              responsibilities text, \
                                              work_hours text, \
                                              PRIMARY KEY (identifier),\
                                              FOREIGN KEY (region) REFERENCES regions(region_code),\
                                              FOREIGN KEY (industry) REFERENCES industries(industries_code),\
                                              FOREIGN KEY (profession) REFERENCES professions(profession_code),\
                                              FOREIGN KEY (id_hiring_organization) REFERENCES organizations(id_organization)\
                                              );")
with conn:
    with conn.cursor() as curs:
        curs.execute(f"COPY vacancies(date_posted, \
                                      id_hiring_organization, \
                                      identifier,\
                                      title,\
                                      industry,\
                                      profession,\
                                      region, \
                                      base_salary_min, \
                                      base_salary_max,\
                                      education_academic_degree, \
                                      education_requirements_education_type,\
                                      education_requirements_speciality, \
                                      experience_requirements, \
                                      job_benefits, \
                                      requirements_qualifications, \
                                      responsibilities, \
                                      work_hours)\
                        FROM '{vacancies_path}'\
                        DELIMITER ';'\
                        CSV HEADER;")
print('База данных создана. Разрываю соединение')
conn.close()
print('Скрипт выполнен успешно')
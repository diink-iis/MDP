import dask.dataframe as dd

# подгрузка данных и преобразовываем к единой структуре
df = dd.read_csv('C:/Users/isaev/MDP/parse_data/organizations.csv', sep=';', dtype='object')\
[['id_organization', 'business_size', 'inn', 'legal_name', 'region_code', 'description']]

df = df[df.description.isnull()==False]

df.to_csv('C:/Users/isaev/MDP/done_files/organizations.csv', single_file=True, index=False, sep=';')



df = dd.read_csv('C:/Users/isaev/MDP/parse_data/curricula_vitae.csv', sep=';', dtype='object')\
[['id_candidate', 'id_cv', 'birthday', 'date_creation', 'education_type', 'experience', 
  'gender', 'industry_code', 'inner_info_fullness_rate', 'position_name', 'profession_code', 'region_code', 'salary',
  'skills', 'additional_skills']]


df = df[df.inner_info_fullness_rate>='80'].drop('inner_info_fullness_rate', axis=1)
df = df[df.industry_code!='Culture']
df = df[df.industry_code!='NotQualification']
df = df[df.industry_code!='Food']
df = df[df.industry_code!='Resources']
df = df[df.industry_code!='WorkingSpecialties']
df = df[df.industry_code!='Forest']
df = df[df.industry_code!='Restaurants']
df = df[df.industry_code!='HomePersonal']
df = df[df.industry_code!='Industry']
df = df[df.industry_code!='Logistic']
df = df[df.industry_code!='CareerBegin']
df = df[df.education_type.isnull()==False]
df = df[df.skills.isnull()==False]
df = df[df.birthday.isnull()==False]

df = df.drop_duplicates(subset=['id_cv'], keep='last')

df.to_csv('C:/Users/isaev/MDP/done_files/cv.csv', single_file=True, index=False, sep=';')



df = dd.read_csv('C:/Users/isaev/MDP/parse_data/professions.csv', sep=';', dtype='object')[['profession_code', 'profession_name']]
df.to_csv('C:/Users/isaev/MDP/done_files/professions.csv', single_file=True, index=False, sep=';')



df = dd.read_csv('C:/Users/isaev/MDP/parse_data/regions.csv', sep=';', dtype='object')[['region_code', 'region_name',
                                                                           'economic_growth', 'medium_salary_difference', 
                                                                           'unemployment_level']]
df.to_csv('C:/Users/isaev/MDP/done_files/regions.csv', single_file=True, index=False, sep=';')



df = dd.read_csv('C:/Users/isaev/MDP/parse_data/edu.csv', sep=';', dtype='object')[['faculty', 'graduate_year',
                                                                       'id_cv', 'legal_name', 'qualification',
                                                                       'speciality']]
df = df[(df.qualification.isnull()==False)|(df.speciality.isnull()==False)]
df = df[df.legal_name.isnull()==False]
df = df[df.id_cv.isnull()==False]
df = df[df.graduate_year.isnull()==False]
cv_lst = list(dd.read_csv('C:/Users/isaev/MDP/done_files/cv.csv', sep=';',
                          dtype='object')['id_cv'].drop_duplicates(keep='last'))
df = df[df.id_cv.isin(cv_lst)]
df = df.drop_duplicates(subset=['id_cv', 'legal_name', 'graduate_year'], keep='last')
df.to_csv('C:/Users/isaev/MDP/done_files/edu.csv', single_file=True, index=False, sep=';')



df = dd.read_csv('C:/Users/isaev/MDP/parse_data/industries.csv', sep=';', dtype='object')

df = df[df.industry_code!='Culture']
df = df[df.industry_code!='NotQualification']
df = df[df.industry_code!='Food']
df = df[df.industry_code!='Resources']
df = df[df.industry_code!='WorkingSpecialties']
df = df[df.industry_code!='Forest']
df = df[df.industry_code!='Restaurants']
df = df[df.industry_code!='HomePersonal']
df = df[df.industry_code!='Industry']
df = df[['industry_code', 'industry_name']].drop_duplicates()

df.to_csv('C:/Users/isaev/MDP/done_files/industries.csv', single_file=True, index=False, sep=';')



df = dd.read_csv('D:/pn_lib_market/vacancies.csv', sep=';', dtype='object')[['date_posted',
                                                                             'id_hiring_organization', 
                                                                             'identifier',
                                                                             'title',
                                                                             'industry',
                                                                             'profession',
                                                                             'region',
                                                                             'base_salary_min',
                                                                             'base_salary_max',
                                                                             'education_academic_degree',
                                                                             'education_requirements_education_type',
                                                                             'education_requirements_speciality',
                                                                             'experience_requirements',
                                                                             'job_benefits',
                                                                             'requirements_qualifications',
                                                                             'responsibilities',
                                                                             'work_hours']]

df = df[df.education_requirements_education_type.isnull()==False]
df = df[df.responsibilities.isnull()==False]
df = df[df.education_requirements_education_type!='Среднее']
df = df[df.education_requirements_education_type!='Среднее профессиональное']

df = df[df.industry!='Culture']
df = df[df.industry!='NotQualification']
df = df[df.industry!='Food']
df = df[df.industry!='Resources']
df = df[df.industry!='WorkingSpecialties']
df = df[df.industry!='Forest']
df = df[df.industry!='Restaurants']
df = df[df.industry!='HomePersonal']
df = df[df.industry!='Industry']

org_lst = list(dd.read_csv('D:/pn_lib_market/done_files/organizations.csv', sep=';', 
                          dtype='object')['id_organization'].drop_duplicates(keep='last'))
df = df[df.id_hiring_organization.isin(org_lst)]
df = df.drop_duplicates(subset=['identifier'], keep='last')

df.to_csv('D:/pn_lib_market/done_files/vacancies.csv', single_file=True, index=False, sep=';')

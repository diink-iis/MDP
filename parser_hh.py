import requests
import pandas as pd

# количество страниц для выгрузки
number_of_pages = 100

# заголовки вакансий для парсинга
job_title = ["'Data Analyst' and 'data scientist'"]

# проходимся по каждой вакансии
for job in job_title:
    data = []
    # проходимся по каждой странице
    for i in range(number_of_pages):
        # API для парсинга
        url = 'https://api.hh.ru/vacancies'
        par = {'text': job, 'area': '113', 'per_page': '10', 'page': i}
        # с помощью бибилотеки выгружаем информацию с HTML кода страницы и сохраняем все в json строки
        r = requests.get(url, params=par)
        e = r.json()
        data.append(e)
        vacancy_details = data[0]['items'][0].keys()
        df = pd.DataFrame(columns=list(vacancy_details))
        ind = 0
        for k in range(len(data)):
            for j in range(len(data[k]['items'])):
                df.loc[ind] = data[k]['items'][j]
                ind += 1
    # выгрузка в csv
    csv_name = job + ".csv"
    df.to_csv(csv_name)

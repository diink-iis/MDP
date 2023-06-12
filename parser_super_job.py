import csv
import time
from typing import Optional, List, Dict, Any, Iterator, IO
import re

import click
import requests
import requests_html
import tqdm


@click.group()
def cli():
    """Run command line."""
    pass


MAX_TRY_NUM: int = 10
SLEEP_TIME_DEFAULT: float = 0.25
SLEEP_TIME_DISCONNECTED: float = 1


def request_get(
    session: requests_html.HTMLSession, url: str,
    params: Optional[Dict[str, Any]] = None
) -> requests_html.HTMLResponse:
    """Perform GET request and return response. Retry on error."""
    for i in range(MAX_TRY_NUM):
        try:
            response: requests_html.HTMLResponse = session.get(
                url, params=params
            )
            time.sleep(SLEEP_TIME_DEFAULT)
            return response
        except requests.ConnectionError:
            time.sleep(SLEEP_TIME_DISCONNECTED)
    raise ValueError('Max request try num exceeded')


class FullInternshipList:
    """Iterator of internships on SuperJob matching criteria."""

    city: str
    industry: Optional[int]
    include_archive: bool
    session: requests_html.HTMLSession
    page_count: int

    def __init__(
        self, city: str, industry: Optional[int], include_archive: bool
    ):
        """Initialize."""
        self.city = city
        self.industry = industry
        self.include_archive = include_archive

        request_params: Dict[str, Any] = dict()
        if industry is not None:
            request_params['industry'] = industry
            request_params['actualOnly'] = int(not include_archive)

        self.session = requests_html.HTMLSession()
        request_url: str = \
            'https://students.superjob.ru/stazhirovki/{}/'.format(city)

        # result_urls: List[str] = []

        r1 = request_get(
            self.session,
            request_url,
            params=request_params
        )

        if r1.status_code != 200:
            raise ValueError('Status code is {}'.format(r1.status_code))

        paginator_bins = r1.html.find('.sj_paginator_btn:last-child')
        if len(paginator_bins) != 2:
            raise ValueError('Can not find sj_paginator_bin')
        else:
            self.page_count = int(paginator_bins[0].text)

    def get_internship_data(
        self, url: str
    ) -> Dict[str, Any]:
        """Get data about internship."""
        r1 = request_get(self.session, url)

        is_archive: bool = \
            len(r1.html.find('.Traineeship_archived_label')) != 0
        requirements = r1.html.find('.Traineeship_requirements')
        requirements_text: Optional[str] = None
        if len(requirements) > 0:
            requirements_text = requirements[0].text

        result: Dict[str, Any] = {
            'is_archive': is_archive,
            'requirements': requirements_text,
            'min_grade': None,
            'max_grade': None,
            'allow_graduate': False,
            'employment_type': None,
        }

        if requirements_text is not None:
            requirements_grades_match = re.match(
                r'([0-9]+)—([0-9]+) курс', requirements_text
            )
            if requirements_grades_match is not None:
                result['min_grade'] = int(requirements_grades_match.group(1))
                result['max_grade'] = int(requirements_grades_match.group(2))

            requirements_graduate_match = re.match(
                r'выпускники', requirements_text
            )
            if requirements_graduate_match is not None:
                result['allow_graduate'] = True

            requirements_employment_match = re.match(
                r'(.*\n|^)(.+? занятость)', requirements_text
            )
            if requirements_employment_match is not None:
                result['employment_type'] = \
                    requirements_employment_match.group(2)

        return result

    def get_html_internship_elements(
        self, html
    ) -> List[Dict[str, Any]]:
        """Return list of internships in page."""
        result: List[Dict[str, Any]] = []

        elements: List[requests_html.Element] = html.find(
            '.TraineeshipList_item_right > a'
        )

        for element in elements:
            if 'href' not in element.attrs:
                continue
            item_url: str = element.attrs['href']
            item_result: Dict[str, Any] = self.get_internship_data(item_url)
            item_result['url'] = item_url
            if self.include_archive or (not item_result['is_archive']):
                result.append(item_result)

        return result

    def get_internship_list(
        self, page: Optional[int]
    ) -> List[Dict[str, Any]]:
        """Get list of internships on page matching criteria."""
        request_params: Dict[str, Any] = dict()
        if self.industry is not None:
            request_params['industry'] = self.industry
        if page is not None:
            request_params['page'] = page + 1

        request_url: str = \
            'https://students.superjob.ru/stazhirovki/{}/'.format(self.city)

        r1 = request_get(
            self.session,
            request_url,
            params=request_params
        )

        if r1.status_code != 200:
            raise ValueError('Status code is {}'.format(r1.status_code))

        return self.get_html_internship_elements(r1.html)

    def __iter__(self) -> Iterator[List[Dict[str, Any]]]:
        """Iterate over lists of internships on pages."""
        for i in range(self.page_count):
            yield self.get_internship_list(i)

    def __len__(self) -> int:
        """Return iterator length."""
        return self.page_count


@click.command()
@click.argument(
    'city', type=click.STRING
)
@click.option(
    '--industry', type=click.INT, default=None
)
@click.option(
    '--include-archive', type=click.BOOL, default=False
)
@click.option(
    '--output-file', type=click.File(mode='wt')
)
def list_internships(
    city: str, industry: Optional[int], include_archive: bool,
    output_file: IO
):
    """Get list of internship URLs on SuperJob matching criteria."""
    result_list: FullInternshipList = FullInternshipList(
        city, industry, include_archive
    )

    csv_writer = csv.DictWriter(
        output_file,
        [
            'url', 'is_archive', 'requirements', 'min_grade', 'max_grade',
            'allow_graduate', 'employment_type'
        ]
    )
    csv_writer.writeheader()

    for result in tqdm.tqdm(result_list):
        csv_writer.writerows(result)


cli.add_command(list_internships)

if __name__ == '__main__':
    cli()
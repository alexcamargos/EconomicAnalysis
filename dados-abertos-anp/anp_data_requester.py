#!/usr/bin/env python
# encoding: utf-8
#
#  ------------------------------------------------------------------------------
#  Name: anp_data_requester.py
#  Version: 0.0.1
#  Summary: ANPRequester
#           A a complete HTTP request to ANP url.
#
#  Author: Alexsander Lopes Camargos
#  Author-email: alcamargos@vivaldi.net
#
#  License: MIT
#  ------------------------------------------------------------------------------

from pathlib import Path
from typing import Iterator

import duckdb as db
import requests


class ANPRequester():
    """Represents a complete HTTP request.

    Supports context management protocol.

    Attributes:
        BASE_URL {str}: ANP GLP base URL for download CSV files.
        HEADERS {dict}: HTTP headers to avoid being blocked by the server.
        __download_dir {Path}: Directory to save the downloaded files.
        __session {requests.Session}: HTTP session object.
    """

    # ANP GLP base URL for download CSV files.
    BASE_URL = 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/glp'

    HEADERS = {
        'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/58.0.3029.110 Safari/537.36')
    }

    def __init__(self, download_dir: str = 'data') -> None:
        """Initializes the ANPRequester object.

        Arguments:
            download_dir {str} -- Directory to save the downloaded files.
                                  Defaults to 'anp_open_data'.
        """

        # Directory to save the downloaded files.
        self.__download_dir = Path(download_dir)
        # Create the directory to save the downloaded files.
        # If the directory already exists, it will not raise an exception.
        self.__download_dir.mkdir(parents=True, exist_ok=True)

        # HTTP session object.
        self.__session = None

    def __enter__(self) -> 'ANPRequester':
        """Enter the runtime context related to the object.

        Returns:
            ANPRequester: The ANPRequester object.
        """

        # Set the user-agent header to avoid being blocked by the server.
        self.__session = requests.Session()
        # Set the user-agent header to avoid being blocked by the server.
        self.__session.headers.update(self.HEADERS)
        print('HTTP session started.')

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the runtime context related to the object.

        Arguments:
            exc_type {type} -- Exception type.
            exc_val {Exception} -- Exception value.
            exc_tb {traceback} -- Traceback object.
        """

        if self.__session:
            self.__session.close()
            print('HTTP session closed.')

        if exc_type:
            print(f'Exception type: {exc_type}')

        # Return False to suppress the exception.
        return False

    def __generate_anp_glp_urls(self,
                                start_year: int = 2004,
                                end_year: int = 2024,
                                semesters_per_year: int = 2) -> Iterator[str]:
        """Generate the URLs for download the ANP GLP CSV files.

        Args:
            start_year (int): Year to start generating URLs. Default is 2004.
            end_year (int): Year to end generating URLs. Default is 2024.
            semesters_per_year (int): Number of semesters per year. Default is 2.

        Yields:
            Iterator[str]: URL for download the ANP GLP CSV files.
        """

        # The ANP historical series for the price of GLP begins in 2004.
        for year in range(start_year, end_year + 1):
            for semester in range(1, semesters_per_year + 1):

                if year == 2021 and semester == 1:
                    # The ANP GLP CSV file for the first semester of 2021 has another address.
                    csv_url = f'{self.BASE_URL}/precos-semestrais-glp{year}-0{semester}.csv'
                elif year == 2022 and semester == 1:
                    # The ANP GLP CSV file for the first semester of 2022 has another address.
                    csv_url = f'{self.BASE_URL}/precos-semestrais-glp-{year}-0{semester}.csv'
                else:
                    csv_url = f'{self.BASE_URL}/glp-{year}-0{semester}.csv'

                # Yield the URL for download the ANP GLP CSV files.
                yield csv_url

    def __make_request(self, url: str) -> requests.Response:
        """Make request to the url and return the response.

        Returns:
            requests.Response: Response of the request.

        Raises:
            RequestException: If the request fails.
        """

        try:
            response = self.__session.get(url)
            response.raise_for_status()

            return response
        except requests.HTTPError as http_err:
            print(f'HTTP error occurred while fetching {url}')
            print(http_err)
            raise
        except Exception as error:
            print(f'Unexpected error occurred while fetching {url}')
            print(error)
            raise

    def download_all_gpl_files(self,
                               start_year: int = 2004,
                               end_year: int = 2024,
                               semesters_per_year: int = 2) -> None:
        """Download all ANP GLP CSV files from 2004 to 2024."""

        for url in self.__generate_anp_glp_urls(start_year, end_year, semesters_per_year):
            file_name = Path(url).name
            csv_file_path = self.__download_dir / file_name

            try:
                # Download the ANP GLP CSV files.
                response = self.__make_request(url)

                # Check if the file already exists and has the same size.
                if (
                    csv_file_path.exists()
                    and csv_file_path.stat().st_size
                    == int(response.headers["Content-Length"])
                ):
                    print(f"{file_name} already downloaded, skipping...")
                    continue

                # Save the downloaded content to the file.
                csv_file_path.write_bytes(response.content)
            except requests.HTTPError:
                print(f'Failed to download {file_name}.')
            except Exception as error:  # pylint: disable=broad-except
                print(f'Failed to download {file_name}')
                print(error)


if __name__ == '__main__':

    # Download all ANP GLP CSV files from 2004 to 2024.
    with ANPRequester() as requester:
        requester.download_all_gpl_files(2004, 2024)

    # Consolidate all ANP GLP CSV files into a single DuckDB database,
    # and save it in a parquet file.
    consolidate_data = db.read_csv('data/' + '*.csv')
    consolidate_data.to_parquet('data/' + 'glp_anp_2004-2024.parquet')

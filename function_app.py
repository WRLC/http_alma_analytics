"""
Azure Functions Python HTTP Trigger for Alma Analytics API Call
"""
import json
import logging
from typing import Any
import urllib.parse
import azure.functions as func
from bs4 import BeautifulSoup  # type:ignore[import-untyped]
from dotenv import load_dotenv
import requests  # type:ignore[import-untyped]
from barcodecheck_models.area import get_area_by_name  # type:ignore[import-untyped]
from barcodecheck_models.apikey import get_api_key_by_area_and_iz  # type:ignore[import-untyped]
from barcodecheck_models.iz import get_iz_by_code  # type:ignore[import-untyped]
from barcodecheck_models.izanalysis import get_iz_analysis_by_iz_and_analysis  # type:ignore[import-untyped]
from barcodecheck_models.analysis import get_analysis_by_name  # type:ignore[import-untyped]

load_dotenv()  # Load environment variables from .env file

app = func.FunctionApp()  # Create an instance of the FunctionApp class


@app.route(route="httpalmaanalytics", methods=['POST'], auth_level=func.AuthLevel.FUNCTION)
def httpalmaanalytics(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function to handle HTTP requests for Alma Analytics API calls.

    :param req:
    :return: func.HttpResponse
    """
    # API call to Alma Analytics
    payload = set_payload(req)  # Set the payload for the API call
    if isinstance(payload, func.HttpResponse):
        return payload

    response = make_api_call(payload=payload)  # Make API call
    if isinstance(response, func.HttpResponse):
        return response

    soup = get_soup(response)  # Parse the XML response
    if isinstance(soup, func.HttpResponse):
        return soup

    columns = get_columns(soup)  # Get the columns from the XML response
    if isinstance(columns, func.HttpResponse):
        return columns

    rows = get_rows(soup, columns)  # Get the rows from the XML response
    if isinstance(rows, func.HttpResponse):
        return rows

    resume_data: Any = soup.find('ResumptionToken')  # Get the resume token

    return func.HttpResponse(  # Return the response
        json.dumps(
            {
                'status': 'success',
                'data': {
                    'resume': resume_data.text if resume_data else None,
                    'rows': rows
                },
            }
        ),
        mimetype='application/json',
        status_code=200
    )


def set_payload(req: func.HttpRequest) -> str | func.HttpResponse:
    """
    Set the payload for the API call.

    :param req: HTTP request
    :return: Payload string
    """
    # HTTP request body
    try:
        req_body: Any = req.get_json()  # Get the request body as JSON
    except ValueError:
        logging.error("Invalid JSON in request body")
        return func.HttpResponse("Invalid JSON in request body", status_code=400)

    if not req_body.get('iz') or not req_body.get('analysis'):  # Validate parameters
        logging.error("Missing required parameters in POST request body")
        return func.HttpResponse("Pass iz, and analysis in the request body", status_code=400)

    # Alma API area
    area = get_area_by_name('analytics')  # Get area by name

    if not area:  # If area is None, return 404
        logging.error("Area %s not found", req_body.get('area'))
        return func.HttpResponse("Area not found", status_code=404)

    # Alma IZ
    iz = get_iz_by_code(req_body.get('iz'))  # Get IZ by code

    if not iz:  # If iz is None, return 404
        logging.error("IZ %s not found", req_body.get('iz'))
        return func.HttpResponse("IZ not found", status_code=404)

    # Alma Analytics analysis
    analysis = get_analysis_by_name(req_body.get('analysis'))  # Get analysis by name

    if not analysis:  # If iz is None, return 404
        logging.error("Analytics Analysis %s not found", req_body.get('analysis'))
        return func.HttpResponse("Analysis not found", status_code=404)

    # IZ-specific Analytics analysis
    iz_analysis = get_iz_analysis_by_iz_and_analysis(iz, analysis)  # Get report path by IZ and analysis

    if not iz_analysis:  # If iz_analysis is None, return 404
        logging.error("Analytics Analysis %s for IZ %s not found", analysis.name, iz.code)
        return func.HttpResponse("Report path not found", status_code=404)

    # API key
    apikey = get_api_key_by_area_and_iz(area.id, iz.id, False)  # Get API key by area and IZ

    if not apikey:  # If apikey is None, return 404
        logging.error("Read-only Analytics API Key not found for %s", iz.code)
        return func.HttpResponse("API key not found", status_code=404)

    # Resume token
    resume: Any | None = req_body.get('resume') if 'resume' in req_body else None  # Get resume token

    payload = urllib.parse.urlencode(
        {
            "path": iz_analysis.path,  # report path
            "apikey": apikey.apikey,  # API key
            'limit': '1000',  # limit (max 1000)
            'col_names': 'true'  # include column names
        },
        safe=':%'  # noqa: WPS432
    )
    if resume:
        payload += f"&token={resume}"

    return payload


def make_api_call(payload: str) -> requests.Response | func.HttpResponse | None:
    """
    Make the API call.

    :param payload: Payload string
    :return: requests.Response | None
    """
    url: str = 'https://api-na.hosted.exlibrisgroup.com/almaws/v1/analytics/reports'

    try:
        response = requests.get(url, params=payload, timeout=600)
        response.raise_for_status()  # Check for HTTP errors
    except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:  # Handle exceptions
        return func.HttpResponse(f"API call failed: {e}", status_code=500)

    return response


def get_soup(response: requests.Response) -> BeautifulSoup | func.HttpResponse:
    """
    Parse the XML response

    :param response: requests.Response
    :return: BeautifulSoup
    """
    soup = BeautifulSoup(response.content, 'xml')  # Parse XML response

    if not soup:  # Check for empty or errors
        logging.error('Empty or invalid XML response')  # Log error
        return func.HttpResponse("Empty or invalid XML response", status_code=404)

    if soup.find('error'):  # Check for errors
        logging.error('Error: %s', soup.find('error').text)  # type:ignore[union-attr]
        return func.HttpResponse(f"Error: {soup.find('error').text}", status_code=500)  # type:ignore[union-attr]

    return soup


def get_columns(soup: BeautifulSoup) -> dict[str, str] | func.HttpResponse:  # type:ignore[valid-type]
    """
    Get the data rows from the report

    :param soup: BeautifulSoup
    :return: list or None
    """
    columnlist: Any = soup.find_all('xsd:element')  # Get the columns from the XML response

    if not columnlist:
        logging.error('No columns found')  # Log error
        return func.HttpResponse("No columns found", status_code=404)

    columns = {}  # Create a dictionary of columns

    for column in columnlist:  # Iterate through the columns
        columns[column['name']] = column['saw-sql:columnHeading']  # Add column to dictionary
        if 'CASE  WHEN Provenance Code' in column['saw-sql:columnHeading']:
            columns[column['name']] = 'Provenance Code'  # Change column name to Provenance Code

    return columns  # type: ignore # Return the dictionary of columns


def get_rows(soup: BeautifulSoup, columns: dict[str, str]) -> list[dict[str, str]] | func.HttpResponse:
    """
    Get the data rows from the report

    :param soup: BeautifulSoup
    :param columns: Columns dictionary
    :return: list or None
    """
    rowlist = soup.find_all('Row')  # Get the rows from the XML response

    if not rowlist:
        logging.error('No rows found')  # Log error
        return func.HttpResponse("No rows found", status_code=404)

    rows = []  # Create a list of rows

    for value in rowlist:  # Iterate through the rows
        values = {}  # Create a dictionary of values
        kids = value.findChildren()  # type:ignore[union-attr] # Get the children of the row

        for kid in kids:  # Iterate through the children
            if kid.name != 'Column0':  # Skip the first column
                if kid.name in columns:  # Use the column heading as the key
                    values[columns[kid.name]] = kid.text  # Add the child to the dictionary
                else:
                    values[kid.name] = kid.text  # Fallback to the original name if not in mapping

        rows.append(values)  # Add the dictionary to the list

    return rows  # Return the list of rows

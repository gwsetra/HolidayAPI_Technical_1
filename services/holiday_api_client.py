import json
import requests

class HolidayAPIClient:
    """
    Client for interacting with the Holiday API.

    This class provides methods to retrieve holiday and country data from the Holiday API.

    Attributes:
        key (str): API key for authenticating requests to the Holiday API.
    """
    def __init__(self, key):
        """Initialize with an API key."""
        self.key = key

    def get_holidays(self, parameters):
        """Fetch holiday data from the API with the given parameters."""
        url = 'https://holidayapi.com/v1/holidays?'

        if not parameters.get('key'):
            parameters['key'] = self.key
        else:
            assert self.key == parameters['key'], 'Keys supplied as an argument & in `parameters` differ. \n Provide at only one place'

        response = requests.get(url, params=parameters)
        data = response.json()

        if not response.ok:
            if not data.get('error'):
                data['error'] = 'Unknown error.'

        return data

    def get_countries(self, parameters):
        """Fetch country data from the API with the given parameters."""
        url = 'https://holidayapi.com/v1/countries?'

        if not parameters.get('key'):
            parameters['key'] = self.key
        else:
            assert self.key == parameters['key'], 'Keys supplied as an argument & in `parameters` differ. \n Provide at only one place'

        response = requests.get(url, params=parameters)
        data = response.json()

        if not response.ok:
            if not data.get('error'):
                data['error'] = 'Unknown error.'

        return data
from dateutil.parser import parse
from json.decoder import JSONDecodeError
from unittest import TestCase

from app.helpers import (
    max_timestamp,
    to_dict
)


class TestHelpers(TestCase):
    """
    Test the helper methods.
    """

    def test_max_timestamp_1(self):
        """
        Test to ensure the `max_timestamp` method returns correct result.
        """
        actual = max_timestamp([parse('2023-09-01'), parse('2023-09-05'), parse('2023-09-03')])
        expected = parse('2023-09-05')
        self.assertEqual(actual, expected)

    def test_max_timestamp_2(self):
        """
        Test to ensure the `max_timestamp` method returns correct result.
        """
        actual = max_timestamp([parse('2023-09-01'), None, parse('2023-09-03')])
        expected = parse('2023-09-03')
        self.assertEqual(actual, expected)

    def test_max_timestamp_3(self):
        """
        Test to ensure the `max_timestamp` method returns correct result.
        """
        actual = max_timestamp([parse('2023-09-01')])
        expected = parse('2023-09-01')
        self.assertEqual(actual, expected)

    def test_max_timestamp_4(self):
        """
        Test to ensure the `max_timestamp` method returns correct result.
        """
        actual = max_timestamp([])
        self.assertIsNone(actual)

    def test_to_dict_1(self):
        """
        Test to ensure the `to_dict` method returns correct result
        when the parameter is not stringify JSON.
        """
        actual = to_dict(1)
        expected = 1
        self.assertEqual(actual, expected)

    def test_to_dict_2(self):
        """
        Test to ensure the `to_dict` method returns correct result
        when the parameter is stringify JSON.
        """
        stringify_json = '{"key": "value", "boolean": True}'

        actual = to_dict(stringify_json)
        expected = {'key': 'value', 'boolean': 'true'}
        self.assertDictEqual(actual, expected)

    def test_to_dict_3(self):
        """
        Test to ensure the `to_dict` method raise `JSONDecodeError`
        when the parameter is not valid stringify JSON.
        """
        # Assert when parameter is an empty string
        with self.assertRaises(JSONDecodeError):
            to_dict('')

        # Assert when parameter is space(s)
        with self.assertRaises(JSONDecodeError):
            to_dict('    ')

        # Assert when parameter is a plain string
        with self.assertRaises(JSONDecodeError):
            to_dict('invalid')

        # Assert when parameter is invalid stringify JSON
        with self.assertRaises(JSONDecodeError):
            to_dict('{"key": $}')

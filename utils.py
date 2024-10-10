"""  General util functions for the test framework """
# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-arguments,duplicate-code
# pylint: disable-all
import datetime
import logging as log
import os
import json
import random
import re
import sys
import subprocess
import string
import time
import threading
import math
from pathlib import Path
from typing import List
from urllib.parse import urlparse, parse_qs
import pytz


def list_to_str(array: list, separator: str = ',') -> str:
    """ Converts lists to string, using passed separator (default is ',')

    :param list array:
    :param str separator:
    :return: Array entries joined into one string
    :rtype: str
    """
    return f'{separator} '.join(array)


def limit_str(original_str: str, char_limit: int = 255, suffix: str = "..") -> str:
    """ Limits string to passed length, also adds passed suffix to the end

    Defaults: 255 limit, '..' as suffix

    :param str original_str:
    :param int char_limit:
    :param str suffix:
    :return: Limited string with suffix if limit is reached, or original string
    :rtype: str
    """
    if suffix:
        return (original_str[: (char_limit - len(suffix))] + suffix) if len(original_str) > char_limit else original_str
    return (original_str[:char_limit]) if len(original_str) > char_limit else original_str


def concatenate_paths(path_a: str, path_b: str) -> str:
    """ Join two paths (string representation) together

    :param str path_a:
    :param str path_b:
    :return: Joined path as string
    :rtype: str
    """
    return os.path.join(path_a, path_b)


def get_zero_date_datetime() -> datetime.datetime:
    """ Get datetime object representing 0

    :return: Datetime object for 1970-01-01
    :rtype: datetime.datetime
    """
    return datetime.datetime(1970, 1, 1)


def get_current_date_datetime(in_utc: bool = True) -> datetime.datetime:
    """ Get current date and time as a datetime object

    :param bool in_utc:
    :return: datetime object as local or utc
    :rtype: datetime.datetime
    """
    return datetime.datetime.now(datetime.timezone.utc) if in_utc else datetime.datetime.now()


def get_current_utc_date_time_formatted(date_format: str) -> str:
    """ Get current date and time formatted

    N.B. With %f set in the formatting string the resulting string seemed needlessly long, hence the shortening

    :param str date_format:
    :return: Current utc date/time in requested format
    :rtype: str
    """
    current_formatted_time = format_datetime_into_str(get_current_date_datetime(in_utc=True), date_format)

    if date_format[-1] == "f":
        # to cut off needlessly accurate microseconds after 3 digits
        return current_formatted_time[:-3]
    return current_formatted_time


def get_current_timezone_date_time_formatted(date_format: str, country_code_iso_3166: str = 'gb') -> str:
    """ Get current date and time formatted from country timezone

    N.B. With %f set in the formatting string the resulting string seemed needlessly long, hence the shortening

    :param str date_format:
    :param str country_code_iso_3166:
    :return: Current utc date/time in requested format
    :rtype: str
    """
    timezone_str = ' '.join(pytz.country_timezones[country_code_iso_3166])
    utc = pytz.timezone('UTC')
    now = utc.localize(datetime.datetime.now(datetime.timezone.utc))
    timezone = pytz.timezone(timezone_str)
    local_time = now.astimezone(timezone)

    return local_time.strftime(date_format)


def format_datetime_into_str(date_time: datetime.datetime, date_format: str) -> str:
    """ Returns string representation of passed-in date/time in requested format

    Output format will be based on passed date format string

    Python 3.x documentation on strftime:
    https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes

    :param date_time.date_time date_time:
    :param str date_format:
    :return: Date/time as string in requested format
    :rtype: str
    """
    return date_time.strftime(date_format)


def parse_str_into_datetime(date_time_str: str, date_format: str) -> datetime.datetime:
    """ Parses date_time object into str based on passed-in date format

    Input format will be based on passed date format string

    Python 3.x documentation on strptime:
    https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes

    :param str date_time_str:
    :param str date_format:
    :return: Date_time object with value matching input, or matching 1970-01-01 in case of an error
    :rtype: datetime.datetime
    """
    try:
        return datetime.datetime.strptime(date_time_str, date_format)
    except (TypeError, ValueError):
        return get_zero_date_datetime()


def datetime_to_epoch(date_a: datetime.datetime) -> int:
    """ Converts date_time object into UNIX time 'epoch' integer

    The Unix epoch (or Unix time or POSIX time or Unix timestamp) is the number of seconds
    that have elapsed since January 1, 1970 (midnight UTC/GMT),
    not counting leap seconds (in ISO 8601: 1970-01-01T00:00:00Z).

    :param date_time.date_time date_a:
    :return: Int value matching seconds passed since 1970-01-01
    :rtype: int
    """
    # create 1,1,1970 in same timezone as d1
    date_b = datetime.datetime(1970, 1, 1, tzinfo=date_a.tzinfo)
    time_delta = date_a - date_b
    return int(time_delta.total_seconds())


def create_folder_if_does_not_exist(path_to_folder: str):
    """ Creates folder if one does not exist on the specified path

    :param str path_to_folder:
    """
    Path(path_to_folder).mkdir(parents=True, exist_ok=True)


def get_valid_filename(file_name: str) -> str:
    """
    Return the given string converted to a string that can be used for a clean
    filename. Remove leading and trailing spaces; convert other spaces to
    underscores; and remove anything that is not an alphanumeric, dash,
    underscore, or dot.

    e.g. get_valid_filename("john's portrait in 2004.jpg")

    'johns_portrait_in_2004.jpg'

    :param str file_name:
    :return: Sanitised string
    :rtype: str
    """
    file_name = file_name.strip().replace(' ', '_')
    return re.sub(r'(?u)[^-\w.]', '', file_name)


def get_file_name_from_end_of_file_path(separator: str, file_path: str) -> str:
    """ Returns the last entry of a split file path. split by passed-in separator

    :param str separator:
    :param str file_path:
    :return: Last entry of split path, or full path if split doesn't work
    :rtype: str
    """
    path_array = file_path.split(separator)
    return path_array[-1] if path_array else file_path


def get_file_name_from_path_with_pathlib(path: str) -> str:
    """ Returns file name from end of a path string, using pathlib.Path

    :param str path:
    :return: File name from end of path or empty string in case of a TypeError
    :rtype: str
    """
    try:
        return Path(path).name
    except TypeError:
        return ''


def get_last_created_files_in_folder(
        folder_path: str,
        with_path: bool = True,
        number_of_files_needed: int = -1,
        exception_file_name_list: tuple = (".DS_Store",)
) -> list:
    """ Returns list of last created files from a specified folder

    The logic sorts the found paths based on their os.path.getmtime attribute,
    more information: https://docs.python.org/3.10/library/os.path.html#os.path.getmtime

    Flag "with_path" appends full path to the return file names

    Parameter "number_of_files_needed" specifies number of returned entries (default: -1)

    Accepts blacklist of file names to be excluded from list (default: (".DS_Store",))

    :param str folder_path:
    :param bool with_path:
    :param int number_of_files_needed:
    :param tuple exception_file_name_list:
    :return: List of paths to files or empty list
    :rtype:
    """
    try:
        paths = sorted(Path(folder_path).iterdir(), key=os.path.getmtime)
        path_strings = []
        for path_entry in paths:
            if path_entry.name not in exception_file_name_list:
                if with_path:
                    path_strings.append(os.path.join(folder_path, path_entry.name))
                else:
                    path_strings.append(path_entry.name)
        if number_of_files_needed >= 0:
            return path_strings[min(len(path_strings), len(path_strings)-number_of_files_needed):]

        return path_strings
    except FileNotFoundError:
        return []


def fashion_url_with_slash(host: str, override_character: str = "/") -> str:
    """ Add a '/' at the end of a string if it isn't there already

    Parameter "override_character" can be used to specify character to attach if not there

    :param str host:
    :param str override_character:
    :return: String with character added to the end if not there yet
    :rtype: str
    """
    if not host:
        return host
    if host[-1] != override_character:
        return f"{host}{override_character}"
    return host


def remove_slash_from_the_end_of_url(host: str, override_character: str = "/") -> str:
    """ Remove a '/' character from the end of a string if it was there

    Parameter "override_character" can be used to specify character to remove

    :param str host:
    :param str override_character:
    :return: Substring without the character
    :rtype: str
    """
    return host[:-1] if host[-1] == override_character else host


def get_random_whole_number(number_a: int, number_b: int) -> int:
    """ Returns a random integer

    :param int number_a:
    :param int number_b:
    :return: Random integer greater than and less than parameter ints
    :rtype: int
    """
    return random.randint(number_a, number_b)


def get_random_list_items(array: list, number_of_items: int = 1, default=None) -> list:
    """ Returns a specified number of random items from parameter list

    :param list array:
    :param int number_of_items:
    :param default:
    :return: Random list item from parameter list or empty list
    :rtype: int
    """
    if default is None:
        default = []
    if not array:
        return default

    random_indexes = list(range(len(array)))
    random.shuffle(random_indexes)
    indexes = random_indexes[:number_of_items]

    return [array[i] for i in indexes]


def get_random_list_item(array: list, default=None):
    """ Returns a random item of list parameter

    :param list array:
    :param any default:
    :return: Random list item from parameter list or default
    :rtype: int
    """
    return get_random_list_items(array, 1)[0] if array else default


def question_or_ampersand(url: str) -> str:
    """ Returns '?' if a parameter url does not contain '?' yet, otherwise returns '&'

    :param str url:
    :return: '?' if '?' not in url or '&'
    :rtype: str
    """
    return "&" if "?" in url else "?"


def get_random_str_of_characters(str_of_letters: str, number_of_characters: int = 10) -> str:
    """ Returns random string from pool of characters (represented by a str parameter)

    :param str str_of_letters:
    :param int number_of_characters:
    :return: String made up of random characters chosen from passed in pool
    :rtype: str
    """
    return ''.join(random.choice(str_of_letters) for _ in range(number_of_characters))


def get_random_str(number_of_characters: int = 10, allow_upper_case: bool = False) -> str:
    """ Returns random string from a pool of ascii lower case and digits

    Setting the "allow_upper_case" bool param adds upper case characters to the pool

    :param int number_of_characters:
    :param bool allow_upper_case:
    :return: String made up of random characters chosen from lower case characters and digits (upper optional)
    :rtype: str
    """
    letters = string.ascii_lowercase + string.digits
    if allow_upper_case:
        letters += string.ascii_uppercase
    return get_random_str_of_characters(letters, number_of_characters)


def get_random_str_of_ints(number_of_characters: int = 10) -> str:
    """ Returns random string from a pool of digits

    :param int number_of_characters:
    :return: String made up of random characters chosen from digits
    :rtype: str
    """
    letters = string.digits
    return get_random_str_of_characters(letters, number_of_characters)


def get_random_int(minimum: int, maximum: int) -> int:
    """ Returns random integer

    :param int minimum:
    :param int maximum:
    :return: Random integer greater than passed minimum and less than maximum
    :rtype: int
    """
    return random.randint(minimum, maximum)


def get_random_email(domain: str = "example.com") -> str:
    """ Returns random email in the following format:

    <random_str_5_long>.<random_str_6_long>@<domain>

    e.g. a33uc.n91n29@example.com

    :param str domain:
    :return: Email matching format '<random_str_5_long>.<random_str_6_long>@<domain>'
    :rtype: str
    """
    return f'{get_random_str(5)}.{get_random_str(6)}@{domain}'


def change_str_casing_to_upper_or_lower(original_str: str, force: str = "") -> str:
    """ Change string casing from upper to lower and vice versa

    :param str original_str:
    :param str force:
    :return: Upper case str if input was lower and lower if input was upper
    :rtype: str
    """
    if original_str.islower() or force.lower() == "upper":
        return original_str.upper()
    if original_str.isupper() or force.lower() == "lower":
        return original_str.lower()
    return original_str


def get_string_from_text_matching_regex(regex: str, text: str, match_group_index: int = 1, default=''):
    """ Returns string from passed text matching specified regular expression

    Parameter "match_group_index" selects index of returned match

    :param str regex:
    :param str text:
    :param int match_group_index:
    :param any default:
    :return: Substring matching specified regular expression or default
    """
    regex_checker = re.compile(regex)
    try:
        if match := regex_checker.search(text):
            return match[match_group_index]
    except TypeError as err:
        print(f"TypeError occurred: {err}")
    return default


def get_multiple_strings_from_text_matching_regex(regex: str, text: str, default=''):
    """ Returns multiple string from passed text matching specified regular expression

    :param str regex:
    :param str text:
    :param any default: defaults to "" (empty str)
    :return: All substring matching specified regular expression, or default
    """
    regex_checker = re.compile(regex)
    try:
        match = regex_checker.search(text)
        return match.groups() if match and match.groups() else default
    except TypeError as err:
        print(f"TypeError occurred: {err}")
    return default


def check_str_matching_regex(regex: str, text: str) -> bool:
    """ Validates if string matches regex pattern

    :param str regex:
    :param str text:
    :return: True if str matches regex, False otherwise
    """
    log.debug(f'checking "{text}" against regex "{regex}"')
    pattern = re.compile(regex)
    return bool(pattern.match(text))


def convert_url_template_to_regex(url_template: str, escaped: bool = False) -> str:
    """ Converts url template to regular expression. Can return escaped or unescaped string.

    :param str url_template:
    :param bool escaped: defaults to False
    :return: Escaped or unescaped converted regex string
    """
    log.debug(f'converting template url into regex: {url_template}, escaped: {escaped}')
    # replace string template - e.g. {index} - with regex capturing group
    revised_url_template = re.escape(url_template) if escaped else url_template
    # Changed in version 3.7: Only characters that can have special meaning in a regular expression are escaped.
    # As a result, '!', '"', '%', "'", ',', '/', ':', ';', '<', '=', '>', '@', and "`" are no longer escaped.
    # source: https://docs.python.org/3/library/re.html#re.escape
    log.debug(f'revised url template: {revised_url_template}')
    url_regex = re.sub(r'\\{[a-zA-Z0-9_\-]+\\}', '(.*)', revised_url_template)

    log.debug(f'converted: {url_regex}')
    return url_regex


def get_item_from_split_text(text: str, separator: str, index: int) -> str:
    """ Returns item with index from split text array

    Method expects the separator to be passed as parameter

    :param str text:
    :param str separator:
    :param int index:
    :return: String from split text array matching index or empty string
    """
    try:
        return text.split(separator)[index]
    except IndexError:
        return ''


def get_length_or_default(obj, default):
    """ Returns length of object or default

    :param obj:
    :param default:
    :return: Length of object or default value
    """
    try:
        return len(obj)
    except TypeError:
        return default


def check_if_mac_os() -> bool:
    """ Returns True if the current platform is macOS (darwin)

    :return: True if the current platform is macOS (darwin) else False
    :rtype: bool
    """
    return bool(sys.platform.startswith('darwin'))


def check_if_linux_os() -> bool:
    """ Returns True if the current platform is Linux

    :return: True if the current platform is Linux else False
    :rtype: bool
    """
    return bool(sys.platform.startswith('linux'))


def check_if_windows_os() -> bool:
    """ Returns True if the current platform is Windows

    :return: True if the current platform is Windows else False
    :rtype: bool
    """
    return bool(sys.platform.startswith('win'))


def create_file_with_content(file_name: str, content: str, encoding: str = 'utf-8'):
    """ Creates a file with passed-in content and encoding (default: 'utf-8')

    :param str file_name:
    :param str content:
    :param str encoding:
    :return: None
    """
    with open(file=file_name, mode='w', encoding=encoding) as file:
        file.write(content)


def get_random_special_str(number_of_characters: int = 20, excluded_list: List[str] = (), forced_prefix: str = None):
    """ Returns random special string

    Supports excluded list of strings and a forced prefix

    :param int number_of_characters:
    :param List[str] excluded_list:
    :param str forced_prefix:
    :return: Random string made of lower, upper case str, digits and specials
    :rtype: str
    """
    specials = string.punctuation
    for excluded in excluded_list:
        specials = specials.replace(excluded, '')

    if forced_prefix:
        alphanumeric_prefix = forced_prefix[:number_of_characters]
        special_prefix = ''.join(random.choice(specials) for _ in range(number_of_characters - len(forced_prefix)))
    else:
        random_alphanumeric_length = 5
        alphanumeric_prefix = get_random_str(number_of_characters=random_alphanumeric_length, allow_upper_case=True)
        special_prefix = ''.join(
            random.choice(specials) for _ in range(number_of_characters - random_alphanumeric_length)
        )

    return f'{alphanumeric_prefix}{special_prefix}'


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    return obj.__dict__


def load_json_with_default(json_text: str, default):
    """ Return json as dict or default

    :param str json_text: Json string to parse/load
    :param default: Default to return in case text can't be parsed
    """
    try:
        if json_text.startswith('\''):
            json_text = f'"{json_text[1:]}'
        if json_text.endswith('\''):
            json_text = f'{json_text[:-1]}"'

        return json.loads(json_text)
    except (json.decoder.JSONDecodeError, AttributeError):
        return default


def unique(seq):
    """ Returns the unique elements of a sequence

    :param seq:
    :return: A list of unique elements from the input sequence
    :rtype: list
    """
    seen = set()
    return [x for x in seq if x not in seen and not seen.add(x)]


def get_unique_list_entries_from_list_of_dicts_for_key(list_of_dicts: List[dict], key: str) -> list:
    """ Returns the unique values for a given key from a list of dictionaries

    :param List[dict] list_of_dicts:
    :param str key:
    :return: A list of all unique values for a given key in a list of dictionaries
    :rtype: list
    """
    all_entries_for_key = [entry.get(key, []) for entry in list_of_dicts]
    return unique(all_entries_for_key)


def get_url_without_query_suffix(url: str) -> str:
    """ returns the part of the URL before any query parameters

    :param str url:
    :return: The url without the query suffix
    :rtype: str
    """
    return url.split('?')[0]


def get_last_value_from_url_params(url: str) -> str:
    """ Returns the last url parameter's value (after the '=')

    :param str url:
    :return: The last parameter value in a url string
    :rtype: str
    """
    split_url = url.split('=')

    return split_url[-1] if len(split_url) > 1 else ''


def get_query_params_from_url(url: str) -> dict:
    """ Returns a dictionary of the query parameters from a passed-in url

    For example, For url 'https://example.com/api?foo=bar&baz=qux' then this function will return
    {'foo': ['bar'], 'baz': ['qux']}

    :param str url:
    :return: A dictionary of query parameters and values
    :rtype: dict
    """
    parsed_url = urlparse(url)
    return parse_qs(parsed_url.query)


def get_path_from_url(url: str) -> str:
    """ Returns the path part of a passed-in url

    For example, For url 'https://example.com/pumpernickle/api?foo=bar&baz=qux' then this function will return
    '/pumpernickle/api'

    :param str url:
    :return: path part of the url
    :rtype: dict
    """
    parsed_url = urlparse(url)
    return parsed_url.path


def convert_str_to_boolean(str_value: str) -> bool:
    """ Converts a string to a boolean value.
    It accepts strings such as 'True', 'False', 'yes', and others, and returns the appropriate boolean value.

    :param str str_value:
    :return: boolean equivalent of passed string
    :rtype: bool
    """
    try:
        return str_value.lower() in {'true', '1', 't', 'y', 'yes', 'yeah', 'yup', 'certainly', 'uh-huh'}

    except AttributeError:
        return False


def extend_list_without_duplicates(list_to_extend: list, new_list: list):
    """ Extends list with items from a new list, after filtering the new list for duplicates

    :param list_to_extend:
    :param list new_list:
    """
    list_to_extend.extend([entry for entry in new_list if entry not in list_to_extend])


def search_list_of_substrings_in_string(
        list_of_substrings: List[str], search_in_string: str, ignored_substrings: List[str] = None) -> List[str]:
    """ Searches string for substrings and returns a list of not found substrings

    :param List[str] list_of_substrings:
    :param str search_in_string:
    :param List[str] ignored_substrings:
    :return: List of substrings NOT found in original string
    :rtype: List[str]
    """
    if ignored_substrings is None:
        ignored_substrings = []

    start_index = 0
    list_not_found_substrings = []
    log.debug(f'looking for {list_of_substrings} in container string: {search_in_string}')
    for substring in list_of_substrings:
        # ignoring substring
        if substring in ignored_substrings:
            log.debug(f'substring in ignore list "{ignored_substrings}", skipping: {substring}')
            continue

        found_index = search_in_string.find(substring, start_index)
        log.debug(f'{substring} in container string at index {found_index}')
        if found_index >= 0:
            start_index = found_index
        else:
            list_not_found_substrings.append(substring)
    log.debug(f'not found: {list_not_found_substrings}')
    return list_not_found_substrings


def get_cookie_with_name(driver, cookie_name):
    """ Gets cookies from webdriver and pulls out one matching the passed-in name

    :param driver:
    :param cookie_name:
    :return: Cookie entry
    """
    log.debug(f'getting cookie with name: {cookie_name}')
    for cookie_entry in driver.get_cookies():
        print(cookie_entry)


def execute_bash_script(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        universal_newlines=True, check=False, timeout=None, shell=False):
    """ This function will run a bash script
    :param cmd: list of cmd arguments
    :return: retval object of subprocess run
    """
    return subprocess.run(
        cmd,
        stdout=stdout,
        stderr=stderr,
        universal_newlines=universal_newlines,
        check=check,
        timeout=timeout,
        shell=shell
    )


def get_array_value_or_default(array, index, default):
    """ Returns array value at index, or default if IndexError occurs

    :param array: Array the index will be applied to
    :param index: Index of array entry to be returned
    :param default: Default return value in case of IndexError
    """
    try:
        return array[index]
    except IndexError:
        return default

def run_bash_w_live_output(cmd, log_file):
    """ This allows to run shell command redirecting output live, tailing the log file in a second thread
    :param str cmd: command to execute
    :param str log_file: log file where to write command output
    """
    log_output_run = threading.Event()
    log_output_run.set()
    log_output_thread = threading.Thread(target=log_to_stdout, args=(log_output_run, log_file))
    log_output_thread.start()
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True) as pro:
        returncode = None
        while returncode is None:
            returncode = pro.poll()
            time.sleep(5)
    log_output_run.clear()
    log_output_thread.join()
    return returncode == 0

def log_to_stdout(log_output_run, log_file):
    """ Stream the debug log to stdout
    :param threading.Event log_output_run: Event to stop logging
    :param str log_file: log file that will be read
    """
    with open(log_file, "r", encoding='utf8') as debug_log:
        while log_output_run.is_set():
            line = debug_log.readline()
            if not line:
                time.sleep(0.1)
                continue
            log.info(line.strip())

def filetime_to_string(file_time):
    """Converts a time_ns (in units of nanoseconds counting from
    Jan 1, 1970 into a human-readable string.
    """
    unix_time_secs = int(file_time / 1000000000)
    unix_time_ns = file_time - (unix_time_secs * 1000000000)
    # debug(f"file_time: {file_time}")
    # debug(f"unix_time_secs: {unix_time_secs}")
    # debug(f"unix_time_ns: {unix_time_ns}")
    # Match the time format of hydra logs
    l_time = time.localtime(unix_time_secs)
    ns_string = "{:<08d}".format(unix_time_ns)
    output = f"{time.strftime('%Y-%m-%d %H:%M:%S', l_time)}.{ns_string}"
    return output


def random_string(length, charset=string.ascii_letters):
    """Generate a random string of the specified length"""
    result = ""
    for _ in range(length):
        result += random.choice(charset)
    return result


def random_dir_string(depth=None, random_depth_range=(1, 4)):
    """Generate a random string that looks like directory path.

    If "depth" is specific a path with a directory depth of "depth" will be
    generated.  E.g. a depth of 3 could return "ABCD/LMNO/HIJK".
    If depth is not specified, a random depth in "random_depth_range"
    inclusive will be used.
    """
    if depth is None:
        depth = random.randint(*random_depth_range)
    path_str = ""
    for _ in range(depth):
        path_str = os.path.join(path_str, random_string(4))
    return path_str


def random_file_string(depth=None, random_depth_range=(1, 4)):
    """Generate a random string that looks like file path.

    If "depth" is specific a path with a directory depth of "depth" will be
    generated.  E.g. a depth of 3 could return "ABCD/LMNO/file_HIJK".
    If depth is not specified, a random depth in "random_depth_range"
    inclusive will be used.
    """
    if depth is None:
        dir_depth = random.randint(
            random_depth_range[0] - 1, random_depth_range[1] - 1
        )
    else:
        dir_depth = depth - 1
    path_str = random_dir_string(dir_depth)
    path_str = os.path.join(path_str, "file_" + random_string(4))
    return path_str


def convert_size(size_bytes):
    """
    Function to convert the size
    """
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB")
    try:
        i_size = int(math.floor(math.log(size_bytes, 1024)))
    except Exception as exc:
        log.exception(f"unable to convert size: {size_bytes}")
        raise AssertionError from exc
    p_size = math.pow(1024, i_size)
    s_size = round(size_bytes / p_size, 2)
    return "%s %s" % (s_size, size_name[i_size])


def compare_two_lists(list1: list, list2: list) -> bool:
    """Compare two lists of dictionaries and logs the difference.
    :param list1: first list.
    :param list2: second list.
    :return:      if there is difference between both lists.
    """
    diff = [i for i in list1 + list2 if i not in list1 or i not in list2]
    result = len(diff) == 0
    if not result:
        log.info(f"** {len(diff)} difference(s) found: {diff[:5]}")
    return result


def compare_xattr_settings(test_full_path, reference_full_path, file_or_dir):
    """Compare xattr settings between test full path and reference full path
    specified. It is a common method for both file and dir.
    You specify 'file' or 'dir' so that it will be referred correctly
    in the log messages.

    returns False if the comparison fails
    """
    # log(f"** Comparing {file_or_dir} xattr settings:")
    # log(f"** test {file_or_dir}: {test_full_path}")
    # log(f"**  ref {file_or_dir}: {reference_full_path}")
    reference_dict_list = []
    reference_names = os.listxattr(
        f"{reference_full_path}", follow_symlinks=False
    )
    value = b""
    temp_dict = {}
    for reference_name in reference_names:
        value = os.getxattr(
            reference_full_path, reference_name, follow_symlinks=False
        )
        temp_dict[reference_name] = value
        reference_dict_list.append(temp_dict.copy())
        temp_dict = {}
    test_dict_list = []
    test_names = os.listxattr(f"{test_full_path}", follow_symlinks=False)
    value = b""
    temp_dict = {}
    for test_name in test_names:
        value = os.getxattr(test_full_path, test_name, follow_symlinks=False)
        temp_dict[test_name] = value
        test_dict_list.append(temp_dict.copy())
        temp_dict = {}
    if not compare_two_lists(reference_dict_list, test_dict_list):
        log.info(f"** Mismatch comparing {file_or_dir} xattr settings:")
        log.info(f"** test {file_or_dir}: {test_full_path}")
        log.info(f"**  ref {file_or_dir}: {reference_full_path}")
        log.info(f"xattr {file_or_dir} mismatch:")
        log.info(f"** test xattr: {test_dict_list}")
        log.info(f"**  ref xattr: {reference_dict_list}")
        return False
    return True


def collect_diags(test_name, clusters=None):
    if clusters:
        for cluster in clusters:
            cluster.collect_diags(test_name)



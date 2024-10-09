# pylint: disable-all
import pytz
from calendar import timegm
import stat
from datetime import datetime, timedelta
import logging as log

# Section 1 - HELPER CONSTANTS DEFINITIONS
MAX_READDIR_ENTRIES = 1024000
EPOCH_AS_FILETIME = 116444736000000000  # January 1, 1970 as MS file time
HUNDREDS_OF_NANOSECONDS = 10000000
BLOCK_SIZE = 4096
NODE_PROPAGATION_DELAY = 0.50
MAX_STREAMS = 1024
MAX_STREAM_NAME_LENGTH = 255

STAT_INFO_ATTRS = [
    "accessed_time",
    "bytes_used",
    "created_time",
    "gid",
    "length",
    "metadata_modified_time",
    "nlink",
    "spec_data",
    "uid",
    "unix_mode",
    "userdata_modified_time",
]

EXTRA_INFO_ATTRS = [
    "dos_flags",
    "file_feature_flags",
    "file_type",
    "parent_cookie",
    "security_descriptor_object_number",
    "software_metadata_object_number",
    "virtual_volume_tag_number",
    "virus_scan_version_number",
    "volume_virus_scan_id",
]

STAT_TYPICAL_VALUES = {
    "accessed_time": 137919572471111111,
    "bytes_used": 4096,
    "created_time": 137919572472222222,
    "gid": 1000,
    "length": 0,
    "metadata_modified_time": 137919572473333333,
    "nlink": 1,
    "spec_data": 0,
    "uid": 1000,
    "unix_mode": 0o755 | stat.S_IFDIR,
    "userdata_modified_time": 137919572474444444,
}

EXTRA_INFO_TYPICAL_VALUES = {
    "dos_flags": 0,
    "file_feature_flags": 2,
    "file_type": stat.S_IFDIR,
    "parent_cookie": 4,
    "security_descriptor_object_number": 5,
    "software_metadata_object_number": 6,
    "virtual_volume_tag_number": 7,
    "virus_scan_version_number": 8,
    "volume_virus_scan_id": 9,
}

STAT_MAX_VALUES = {
    "accessed_time": 137919572470000000,
    "bytes_used": 0xFFFFFFFF,
    "created_time": 137919572470000000,
    "gid": 0xFFFFFFFF - 1,
    "length": 0xFFFFFFFF,
    "metadata_modified_time": 137919572470000000,
    "nlink": 0xFFFFFFFF,
    "spec_data": 0xFFFFFFFF,
    "uid": 0xFFFFFFFF - 1,
    "unix_mode": 0o755 | stat.S_IFDIR,
    "userdata_modified_time": 137919572470000000,
}

EXTRA_INFO_MAX_VALUES = {
    "dos_flags": 0xFFFFFFFF,
    "file_feature_flags": 0xFFFFFFFF,
    "file_type": 0xFFFF,
    "parent_cookie": 0x7FFFFFFFFFFFFFFF,
    "security_descriptor_object_number": 0xFFFFFFFF,
    "software_metadata_object_number": 0xFFFFFFFF,
    "virtual_volume_tag_number": 0x0FFFFFFF,
    "virus_scan_version_number": 0x0FFFFFFF,
    "volume_virus_scan_id": 0xFFFF,
}


# Section 2 - HELPER FUNCTIONS


def filetime_to_dt(filetime):
    """Converts a Microsoft filetime number to a Python datetime.
    :param int filetime: timestamp in filetime format
    It returns value in UTC (timezone aware)
    """
    usec = (filetime - EPOCH_AS_FILETIME) // 10
    utc_time = datetime(1970, 1, 1) + timedelta(microseconds=usec)
    return utc_time.replace(tzinfo=pytz.UTC)


def dt_to_filetime(dat_time):
    """Converts a python datetime into Microsoft filetime number.
    :param int dat_time: timestamp in datetime format
    """
    return EPOCH_AS_FILETIME + (timegm(dat_time.timetuple()) * 10000000)


def verify_returned_flag_field(flag, pre=None, stat_post=None, extra_post=None):
    """Verifies that flag corresponds to the case,
    depending if pre/post attr are set.
    :param AttributeFlags flag: Flag returned in Pre/Post attrs
    :param boolean pre: True if pre attr were requested
    :param boolean stat_post: True if stat_post attr were requested
    :param boolean extra_post: True if extra_post attr were requested
    """
    expected_result = 0
    if pre:
        expected_result |= AttributeFlags.PRE_OP
    if stat_post:
        expected_result |= AttributeFlags.STAT_POST_OP
    if extra_post:
        expected_result |= AttributeFlags.EXTRA_POST_OP

    if flag != expected_result:
        log.error(
            f"{flag} does not correspond to stat_post:{stat_post}, stat_post:{extra_post}, pre:{pre}"
        )
        return
    return True


# Section 3 - HELPER CLASSES


class OpFlags:
    CREATE_SHORT_NAME = 0x0001  # Create, Link, Mkdir, Rename
    EXCLUDE_SHORT_NAMES = 0x0002  # ReadDirEntries
    SMB_REPLACE_IF_EXISTS = 0x0004  # Rename
    SMB_SEMANTICS = 0x0008  # Rename
    DELETE_ON_CLOSE = 0x0010  # Close
    CASE_INSENSITIVE = 0x0020  # Create, Link, Lookup, Mkdir, Rename
    BYPASS_SHARING_VIOLATION = 0x0040  # Creat, Open, UpgradeOpen


class AccessModes:
    READ = 0x0001
    WRITE = 0x0002
    DELETE = 0x0004
    UNLINK = 0x0010

    @classmethod
    def mode_to_string(cls, mode):
        """Return a string representation of the mode
        :param int mode: access/shared mode
        """
        return ','.join(
            ac[0] for ac in [
                ("READ", cls.READ),
                ("WRITE", cls.WRITE),
                ("DELETE", cls.DELETE),
                ("UNLINK", cls.UNLINK)
            ] if mode & ac[1] == ac[1])

    @classmethod
    def existing_handlers_to_string(cls, existing_handlers):
        """Return a string representation of the existing handlers
        :param dict existing_handlers: Existing handlers dictionary
        """
        handler_string = ""
        for key, val in existing_handlers.items():
            handler_string += (f"Handler {key} - access_mode: {cls.mode_to_string(val[1])}"
                               f" - shared_mode: {cls.mode_to_string(val[2])}\n")
        return handler_string[:-1] if len(handler_string) > 0 else "No handlers open"

    @classmethod
    def existing_handlers_check(cls, existing_handlers, input_access_mode, input_share_mode):
        """Check if the existing handlers are compatible with the input access/share mode
        :param dict existing_handlers: Existing handlers dictionary
        :param int input_access_mode: Input access mode
        :param int input_share_mode: Input share mode
        """
        if len(existing_handlers) == 0:
            return True
        am_cum = 0
        sh_cum = 7
        for am in existing_handlers.values():
            am_cum |= am[1]
            sh_cum &= am[2]
        if input_access_mode & sh_cum != input_access_mode:
            log.debug(f"Access mode {cls.mode_to_string(input_access_mode)} "
                      f"not compatible with current cumulative shared_mode {cls.mode_to_string(sh_cum)}\n"
                      f"These are the current handlers open and the accesses they set on the call:\n"
                      f"{cls.existing_handlers_to_string(existing_handlers)}\n"
                      f"Current cumulative shared_mode: {cls.mode_to_string(sh_cum)}\n"
                      f"Current cumulative access_mode: {cls.mode_to_string(am_cum)}\n")
            return False
        if input_share_mode & am_cum != am_cum:
            log.debug(f"Share mode {cls.mode_to_string(input_share_mode)} "
                      f"not compatible with current cumulative access_mode {cls.mode_to_string(am_cum)}\n"
                      f"These are the current handlers open and the accesses they set on the call: \n"
                      f"{cls.existing_handlers_to_string(existing_handlers)}\n"
                      f"Current cumulative shared_mode: {cls.mode_to_string(sh_cum)}\n"
                      f"Current cumulative access_mode: {cls.mode_to_string(am_cum)}\n")
            return False
        return True


class AttributeFlags:
    PRE_OP = 0x00000001
    STAT_POST_OP = 0x00000002
    EXTRA_POST_OP = 0x00000004


# Section 2 - HELPER CLASSES
class OpRequirements:
    UPDATE_METADATA_MODIFIED = 0x02
    UPDATE_USERDATA_MODIFIED = 0x04
    UPDATE_ACCESSED_TIME = 0x08
    UPDATE_CREATED_TIME = 0x10
    SET_DOS_ARCHIVE_FLAG = 0x20
    RETURN_PRE_OP_ATTR = 0x40
    RETURN_STAT_POST_OP_ATTR = 0x80
    RETURN_EXTRA_POST_OP_ATTR = 0x01

    FIELD_FLAG_DICT = {
        "return_pre_op_attr": RETURN_PRE_OP_ATTR,
        "return_stat_post_op_attr": RETURN_STAT_POST_OP_ATTR,
        "return_extra_post_op_attr": RETURN_EXTRA_POST_OP_ATTR,
        "metadata_modified_time": UPDATE_METADATA_MODIFIED,
        "userdata_modified_time": UPDATE_USERDATA_MODIFIED,
        "accessed_time": UPDATE_ACCESSED_TIME,
        "created_time": UPDATE_CREATED_TIME,
        "dos_flags": SET_DOS_ARCHIVE_FLAG,
    }

    @classmethod
    def fields_to_reqs(cls, fields):
        reqs = 0
        for field in fields:
            reqs |= cls.FIELD_FLAG_DICT[field]
        return reqs


class WriteFlags:
    NO_CACHE = 0x1  # Hint to filesystem on whether to cache data
    WRITE_TRHOUGH = 0x2  # Data must be on stable-storage before return


class ReadFlags:
    NO_CACHE = 0x1  # Hint to filesystem on whether to cache data


class SetAttrControl:
    MODE = 0x00000001
    UID = 0x00000002
    GID = 0x00000004
    LENGTH = 0x00000008
    CREATED_TIME = 0x00000010
    ACCESSED_TIME = 0x00000020
    USERDATA_MODIFIED_TIME = 0x00000040
    METADATA_MODIFIED_TIME = 0x00000080
    DOS_FLAGS = 0x00000200
    FILE_FEATURE_FLAGS = 0x00000400
    SOFTWARE_METADATA_OBJECT = 0x00001000
    SEC_DESC_OBJECT = 0x00002000
    PARENT_COOKIE = 0x00004000
    VIRTUAL_VOLUME_TAG = 0x00008000
    VIRUS_SCAN_VERSION = 0x00010000
    VOLUME_VIRUS_SCAN_ID = 0x00020000
    FILE_TYPE = 0x00040000


class SeekType:
    next_data = 0x1
    next_hole = 0x2

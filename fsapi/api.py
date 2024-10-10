# pylint: disable-all
import sys
import logging as log
from datetime import datetime, timezone
from fsapi.filesystem import FileSystem
from fsapi.static import (
    OpRequirements,
    verify_returned_flag_field,
    STAT_INFO_ATTRS,
    EXTRA_INFO_ATTRS,
    filetime_to_dt,
)


class FsApiWrapper:
    """Class to use as fsapi Wraper for calls to C++ functions.
    On creation, we'll assign fsapi to the fsapi library that
    has been imported from GFS or SOFS depending on the mode.

    On the other hand, it has an atribute called api, which is a dictionary
    to store objects of type fsapi.GFSFSApi() or fsapi.SOFSFSApi(), again
    depending on the mode.
    """

    def __init__(self, fs_type, root_code_path):
        self.fs_type = fs_type
        # Load the corresponding library depending on fs type
        if self.fs_type == "gfs":
            # Append to python path the folder containing the pybind .so library compiled
            sys.path.append(f"{root_code_path}/bazel-bin/src")
            import libgfsfsapi as fsapi

        elif self.fs_type == "sofs":
            # Append to python path the folder containing the pybind .so library compiled
            sys.path.append(f"{root_code_path}/bazel-bin/src")
            import libhydrasofs as fsapi

        elif self.fs_type == "fsapi":
            # Append to python path the folder containing the pybind .so library compiled
            sys.path.append(f"{root_code_path}/bazel-bin/src/examples")
            import libpassthrufsapi as fsapi

        elif self.fs_type == "ufo":
            # Append to python path the folder containing the pybind .so library compiled
            sys.path.append(f"{root_code_path}/bazel-bin/src")
            import libufopybind as fsapi

        # This is to be able to access fsapi lib
        self.fsapi = fsapi

    def get_fsapi_version(self):
        """Returns fsapi version"""
        return self.fsapi.GetFSApiVersion()

    def get_fsapi_type(self):
        """Returns fsapi type"""
        return self.fsapi.GetFSApiType()

    def get_help(self):
        """Prints fsapi help"""
        help(self.fsapi)

    def list_filesystems(self, node_api):
        """Returns the list of current filesystems
        :param str node_name: name of the node, the same used as key in Init
        """
        return node_api.ListFilesystems()

    def open_filesystem(self, node_api, fs_id=None, subvolume_path=None):
        """Returns a FileSystem object in which operations can be applied
        :param str node_api: API object for the fs
        :param int fs_id: id of the filesystem
        """
        if self.fs_type == "gfs":
            fs_mount = self.fsapi.GfsMountInfo()
        elif self.fs_type == "sofs":
            fs_mount = self.fsapi.SofsMountInfo()
        elif self.fs_type == "fsapi":
            fs_mount = self.fsapi.PassthruMountInfo()
        else:
            raise AssertionError(f"type {self.fs_type} not recognized")
        return FileSystem(
            node_api, self.fsapi, fs_mount, self.fs_type, fs_id, subvolume_path
        )

    def shutdown(self, node_api):
        """Shutdown gfs client for a specific node
        :param str node_name: API object for the fs
        """
        node_api.Shutdown()

    def compare_file_info(self, f_info1, f_info2, exceptions=[], only_these=[]):
        """Compares each field of 2 FileInfo objects
        :param FileInfo f_info1: FileInfo object 1
        :param FileInfo f_info1: FileInfo object 1
        :param list exceptions: Fields to skip
        :param list only_these: If not empty, only those fields will be compared,
        exceptions will still apply if provided.
        """
        if len(only_these) > 0:
            attr_list = only_these
        else:
            if isinstance(f_info1, self.fsapi.StatInfo):
                attr_list = STAT_INFO_ATTRS
            else:
                attr_list = EXTRA_INFO_ATTRS
        attr_list = [att for att in attr_list if att not in exceptions]
        for attr in attr_list:
            a = f_info1.__getattribute__(attr)
            b = f_info2.__getattribute__(attr)
            if a != b:
                log.error(f"Attribute {attr} does not match: {a} Vs {b}")
                return
        return True

    def compare_all_attributes(
        self,
        req,
        attrs,
        f_info_in,
        f_info_out,
        current_time,
        extra_exceptions={"stat": [], "extra": []},
    ):
        """Helper function to verify that file info attributes
         are updated when they should and not updated when they shouldn't
        :param list req: list of requested flags
        :param PrePostAttributes attrs: FileInfo object 1
        :param FileInfo f_info_in: file_info before operation
        :param FileInfo f_info_out: file_info after operation
        :param datetime current_time: Time when op was executed,
        :param dict extra_exceptions: If not empty, only those fields will be verified,
        """
        # If pre/post attrs are requested to be returned we'll need to check them
        if "return_pre_op_attr" in req:
            pre_attr = attrs.pre_op
            # Compare returned pre-attributes with the obtained by get_attr before operation
            # All fields must match
            assert self.compare_file_info(
                f_info_in[0],
                pre_attr,
                only_these=[
                    "length",
                    "metadata_modified_time",
                    "userdata_modified_time",
                ],
                exceptions=extra_exceptions["stat"],
            )
        if "return_stat_post_op_attr" in req:
            # Compare returned stat post-attributes with the obtained by get_attr after operation
            # Non-requested fields must match, fields requested to update must differ
            if f_info_out:
                assert self.compare_file_info(
                    f_info_out[0],
                    attrs.stat_post_op,
                    exceptions=extra_exceptions["stat"],
                )

        if "return_extra_post_op_attr" in req:
            # Compare returned extra post-attributes with the obtained by get_attr after operation
            # Non-requested fields must match, fields requested to update must differ
            if f_info_out:
                assert self.compare_file_info(
                    f_info_out[1],
                    attrs.extra_post_op,
                    exceptions=extra_exceptions["extra"],
                )

        # Get what flags have been requested apart from return pre/post
        updated_fields = {"stat": [], "extra": []}
        for field in req:
            if field not in [
                "return_pre_op_attr",
                "return_stat_post_op_attr",
                "return_extra_post_op_attr",
            ]:
                if field in STAT_INFO_ATTRS:
                    updated_fields["stat"].append(field)
                elif field in EXTRA_INFO_ATTRS:
                    updated_fields["extra"].append(field)
                else:
                    raise AssertionError(f"field {field} not recognized")

        # Metadata modified time is always updated in sofs
        if self.fs_type == "sofs":
            updated_fields["stat"].append("metadata_modified_time")

        # Compare GetAttr before and after operation
        # Check that only requested attributes have been updated
        if f_info_out:
            assert self.compare_file_info(
                f_info_out[0],
                f_info_in[0],
                exceptions=updated_fields["stat"] + extra_exceptions["stat"],
            )
            for req_attr in updated_fields["stat"]:
                # The updated timestamp must be different after operation
                if not (
                    self.fs_type in ["fsapi", "sofs"]
                    and req_attr in ["metadata_modified_time"]
                ):
                    assert f_info_in[0].__getattribute__(
                        req_attr
                    ) != f_info_out[0].__getattribute__(
                        req_attr
                    ), f"{req_attr} does not change"
                    # The updated timestamp should be very close to current time
                    dt_out = filetime_to_dt(
                        f_info_out[0].__getattribute__(req_attr)
                    )
                    assert (current_time - dt_out).seconds == 0

        # Compare GetAttr before and after operation
        # Check that only requested attributes have been updated
        if f_info_out:
            assert self.compare_file_info(
                f_info_out[1],
                f_info_in[1],
                exceptions=updated_fields["extra"] + extra_exceptions["extra"],
            )

    def check_op_flag_attr(
        self,
        fs_obj,
        operation,
        parent,
        target,
        op_args,
        req_parent=[],
        req_target=[],
        op_flags=0,
        special_fields={},
    ):
        """Helper function to check that all Pre/post attributes are correctly returned
        :param list api: fsapi wrapper object
        :param func operation: operation under test
        :param str parent: parent path
        :param str target: target path
        :param args op_args: arguments to pass to operation
        :param list req_parent: list of flags set for parent,
        :param list req_target: list of flags set for target,
        :param int op_flags: req flags,
        :param dict special_fields: fields that need special comparison,
        """
        # Call get_attr for parent and target
        target_path = (
            target
            if operation.__name__ != "unlink" or parent in [None, "/", ""]
            else f"{parent}/{target}"
        )
        file_info_parent_before = (
            fs_obj.get_attr(file_path=parent) if parent else None
        )
        if operation.__name__ in ["create_file", "mkdir", "symlink"]:
            file_info_target_before = (op_args[-2], op_args[-1])
        else:
            file_info_target_before = (
                fs_obj.get_attr(file_path=target_path) if target else None
            )

        # Check special fields before operation
        for spf in special_fields.get("stat_parent", []):
            if spf != "bytes_used":
                val1 = file_info_parent_before[0].__getattribute__(spf)
                val2 = special_fields["stat_parent"][spf]["before"]
                assert val1 == val2, f"{spf} does not match {val1} vs {val2}"
        for spf in special_fields.get("extra_parent", []):
            if spf != "bytes_used":
                val1 = file_info_parent_before[1].__getattribute__(spf)
                val2 = special_fields["extra_parent"][spf]["before"]
                assert val1 == val2, f"{spf} does not match {val1} vs {val2}"
        for spf in special_fields.get("stat_target", []):
            if spf != "bytes_used":
                val1 = file_info_target_before[0].__getattribute__(spf)
                val2 = special_fields["stat_target"][spf]["before"]
                assert val1 == val2, f"{spf} does not match {val1} vs {val2}"
        for spf in special_fields.get("extra_target", []):
            if spf != "bytes_used":
                val1 = file_info_target_before[1].__getattribute__(spf)
                val2 = special_fields["extra_target"][spf]["before"]
                assert val1 == val2, f"{spf} does not match {val1} vs {val2}"

        # Call Operation using requested flags
        req_flags = self.fsapi.RequestFlags()
        req_flags.target_requirements = OpRequirements.fields_to_reqs(
            req_target
        )
        req_flags.parent_requirements = OpRequirements.fields_to_reqs(
            req_parent
        )
        req_flags.flags = op_flags

        op_ret = operation(*op_args, req_flags=req_flags)
        parent_attr, target_attr = op_ret[1:3]
        current_time = datetime.now(timezone.utc)

        # Call get_attr for parent and target
        if operation.__name__ in ["unlink", "rmdir"]:
            file_info_target_out = None
        else:
            file_info_target_out = (
                fs_obj.get_attr(file_path=target_path) if target else None
            )
        file_info_parent_out = (
            fs_obj.get_attr(file_path=parent) if parent else None
        )

        # First check that returned flags attr are consistent
        if parent_attr:
            assert verify_returned_flag_field(
                parent_attr.flags,
                stat_post="return_stat_post_op_attr" in req_parent,
                extra_post="return_extra_post_op_attr" in req_parent,
                pre="return_pre_op_attr" in req_parent,
            ), (
                f"Error verifying flag: {parent_attr.flags},"
                f" Pre provided: {'return_pre_op_attr' in req_parent},"
                f" Post stat provided: {'return_stat_post_op_attr' in req_parent}"
                f" Post extra provided: {'return_stat_post_op_attr' in req_parent}"
            )
        if target_attr:
            if (
                operation.__name__ not in ["unlink"]
                or file_info_target_before[0].nlink > 1
            ):
                assert verify_returned_flag_field(
                    target_attr.flags,
                    stat_post="return_stat_post_op_attr" in req_target,
                    extra_post="return_extra_post_op_attr" in req_target,
                    pre="return_pre_op_attr" in req_target,
                ), (
                    f"Error verifying flag: {target_attr.flags},"
                    f" Pre provided: {'return_pre_op_attr' in req_target},"
                    f" Post stat provided: {'return_stat_post_op_attr' in req_target}"
                    f" Post extra provided: {'return_extra_post_op_attr' in req_target}"
                )
        # Check special fields after operation
        for flag in req_parent:
            if special_fields.get("stat_parent"):
                special_fields["stat_parent"].pop(flag, None)
            if special_fields.get("extra_parent"):
                special_fields["extra_parent"].pop(flag, None)
        for flag in req_target:
            if special_fields.get("stat_target"):
                special_fields["stat_target"].pop(flag, None)
            if special_fields.get("extra_target"):
                special_fields["extra_target"].pop(flag, None)

        for spf in special_fields.get("stat_parent", []):
            if self.fs_type == "gfs" or spf not in ["bytes_used", "length"]:
                val1 = file_info_parent_out[0].__getattribute__(spf)
                val2 = special_fields["stat_parent"][spf]["after"]
                assert (
                    val1 == val2
                ), f"{spf} does not match after operation: {val1} vs {val2}"

        # Check values of returned parent_attr are correct if requested
        if "return_stat_post_op_attr" in req_parent:
            for field in STAT_INFO_ATTRS:
                val3 = parent_attr.stat_post_op.__getattribute__(field)
                val4 = file_info_parent_out[0].__getattribute__(field)
                assert (
                    val3 == val4
                ), f"{field} does not match after operation: {val3} vs {val4}"

        for spf in special_fields.get("extra_parent", []):
            val1 = file_info_parent_out[1].__getattribute__(spf)
            val2 = special_fields["extra_parent"][spf]["after"]
            assert (
                val1 == val2
            ), f"{spf} does not match after operation: {val1} vs {val2}"

        if operation.__name__ not in ["unlink", "rmdir"]:
            for spf in special_fields.get("stat_target", []):
                if self.fs_type == "gfs" or spf not in ["bytes_used", "length"]:
                    val1 = file_info_target_out[0].__getattribute__(spf)
                    val2 = special_fields["stat_target"][spf]["after"]
                    assert (
                        val1 == val2
                    ), f"{spf} does not match after operation: {val1} vs {val2}"

            # Check values of returned target_attr are correct if requested
            if "return_stat_post_op_attr" in req_target:
                for field in STAT_INFO_ATTRS:
                    if (
                        field not in ["length", "bytes_used"]
                        or "stream" not in operation.__name__
                    ):
                        val3 = target_attr.stat_post_op.__getattribute__(field)
                        val4 = file_info_target_out[0].__getattribute__(field)
                        assert (
                            val3 == val4
                        ), f"{field} does not match after operation: {val3} vs {val4}"
            for spf in special_fields.get("extra_target", []):
                val1 = file_info_target_out[1].__getattribute__(spf)
                val2 = special_fields["extra_target"][spf]["after"]
                assert (
                    val1 == val2
                ), f"{spf} does not match after operation: {val1} vs {val2}"

            if "return_extra_post_op_attr" in req_target:
                for field in EXTRA_INFO_ATTRS:
                    val3 = target_attr.extra_post_op.__getattribute__(field)
                    val4 = file_info_target_out[1].__getattribute__(field)
                    assert (
                        val3 == val4
                    ), f"{field} does not match after operation: {val3} vs {val4}"

        # Check the rest of fields
        extra_exceptions = {
            "stat": [spf for spf in special_fields.get("stat_parent", [])],
            "extra": [spf for spf in special_fields.get("extra_parent", [])],
        }
        if self.fs_type in ["fsapi", "sofs"]:
            if "metadata_modified_time" not in extra_exceptions["stat"]:
                extra_exceptions["stat"].append("metadata_modified_time")
            if "userdata_modified_time" not in extra_exceptions["stat"]:
                extra_exceptions["stat"].append("userdata_modified_time")
            if operation.__name__ == "symlink":
                extra_exceptions["stat"].append("unix_mode")

        self.compare_all_attributes(
            req_parent,
            parent_attr,
            file_info_parent_before,
            file_info_parent_out,
            current_time,
            extra_exceptions=extra_exceptions,
        )

        extra_exceptions = {
            "stat": [spf for spf in special_fields.get("stat_target", [])],
            "extra": [spf for spf in special_fields.get("extra_target", [])],
        }
        if self.fs_type in ["fsapi", "sofs"]:
            if "metadata_modified_time" not in extra_exceptions["stat"]:
                extra_exceptions["stat"].append("metadata_modified_time")
            if "userdata_modified_time" not in extra_exceptions["stat"]:
                extra_exceptions["stat"].append("userdata_modified_time")
            if operation.__name__ in ["create_file", "mkdir", "symlink"]:
                extra_exceptions["stat"].append("created_time")
                extra_exceptions["stat"].append("accessed_time")

        self.compare_all_attributes(
            req_target,
            target_attr,
            file_info_target_before,
            file_info_target_out,
            current_time,
            extra_exceptions=extra_exceptions,
        )

        return op_ret

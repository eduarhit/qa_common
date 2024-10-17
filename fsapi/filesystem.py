# pylint: disable-all
import json
import os
import stat
import errno
from fsapi.static import (
    MAX_READDIR_ENTRIES,
    STAT_TYPICAL_VALUES,
    EXTRA_INFO_TYPICAL_VALUES, AccessModes,
)
import json
from pathlib import Path
# from framework.functions_general import get_ceph_info, restart_vcm_mockoon
from fsapi.static import MAX_READDIR_ENTRIES
import subprocess
import pathlib
# from gfs.functions_gfs import run_command
import time
import logging as log


class FileSystem:
    """Class for filesystem objects in which fs operations can be applied.
    On creation, we'll get mount_info and fs_info from fsapi class, and we'll
    call OpenFilesystem to open the fs and LookupRoot to get the root inode

    :param int fs_id: dataset id of the filesystem
    :param fsapi.GFSFSApi
        or fsapi.SofsFSApi
        or fsapi.Passthru api: FS fsapi object created on Init for a node
    :param fsapi fsapi: fsapi object
    """

    def __init__(
            self, api, fsapi, fs_mount, fs_type, fs_id=1, subvolume_path=None
    ):
        self.fs_id = fs_id
        self.api = api
        self.fsapi = fsapi
        self.fs_type = fs_type
        if fs_type == "gfs":
            fs_mount.dataset_id = fs_id
        elif fs_type == "sofs":
            fs_mount.fsid = fs_id
            fs_mount.path = subvolume_path
        if fs_type == "ufo":
            self.fs_info = fs_id
        else:
            fs_info = self.api.OpenFilesystem(fs_mount)
            assert fs_info[0] == 0
            self.fs_info = fs_info[1]
        root_inode = api.LookupRoot(self.fs_info)
        # assert root_inode[0] == 0 # skip for now, not ready
        self.root_inode = root_inode[1]

    def get_handle_object_type(self):
        if self.fs_type == "gfs":
            return self.fsapi.GfsHandle
        elif self.fs_type == "sofs":
            return self.fsapi.SofsHandle
        elif self.fs_type == "fsapi":
            return self.fsapi.PassthruHandle

    def lookup(
            self,
            file_path,
            parent_inode,
            req_flags=None,
            verify=True,
    ):
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        ret_code, root_inode, parent_attr, target_attr = self.api.Lookup(
            self.fs_info, parent_inode, file_path, req_flags
        )
        if verify:
            assert ret_code == 0, f"Unable to get inode for {file_path}"
        return ret_code, parent_attr, target_attr, root_inode

    def get_inode(
            self, file_path, req_flags=None, verify=True, return_attrs=False
    ):
        """Get inode for a given path
        :param str file_path: path of the file/dir
        :param int req_flags: request flags
        :param bool verify: if True, check result,
            if False return either inode or error
        :param bool return_attrs: if True, it will return
            pre_attr and post_attr
        """
        if not file_path.startswith("/"):
            file_path = "/" + file_path
        levels = []
        parent = os.path.split(file_path)[0]
        if os.path.split(file_path)[1] != "":
            levels.insert(0, os.path.split(file_path)[1])
        while parent != "/":
            levels.insert(0, parent.split("/")[-1])
            parent = os.path.split(parent)[0]

        if len(levels) == 0:
            return self.root_inode
        root_inode = self.root_inode
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        for level in levels:
            ret_code, parent_attr, target_attr, root_inode = self.lookup(
                level, root_inode, req_flags, verify=False
            )
            if verify:
                assert ret_code == 0, f"Unable to get inode for {file_path}"
            if ret_code != 0:
                return ret_code
        return (
            root_inode
            if not return_attrs
            else (root_inode, parent_attr, target_attr)
        )

    def lookup_root(self):
        """Returns root inode"""
        root_inode = self.api.LookupRoot(self.fs_info)
        assert root_inode[0] == 0
        return root_inode[1]

    def sync(
            self,
            file_path=None,
            handle_id=None,
            req_flags=None,
            verify=True,
    ):
        """Sync for a given file
        :param str file_path: path of the file
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        :param bool verify: if True, check ret_code==0
        """
        autoclose = False
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        if not handle_id:
            inode = self.get_inode(file_path, verify=False)
            if not isinstance(inode, self.fsapi.Inode):
                return inode, None, None
            open_parameters = self.fsapi.OpenParameters()
            open_parameters.open_disposition = self.fsapi.OpenDisposition.open_existing
            open_parameters.access_mode = AccessModes.READ | AccessModes.WRITE | AccessModes.DELETE
            open_parameters.share_mode = AccessModes.READ | AccessModes.WRITE | AccessModes.DELETE
            ret_code, handle_id, target_attr = self.api.Open(
                self.fs_info,
                inode,
                req_flags=self.fsapi.RequestFlags(),
                open_flags=os.O_RDONLY,
                open_parameters=open_parameters
            )
            autoclose = True
        # Call Sync
        ret_code, target_attr = self.api.Sync(
            self.fs_info, handle_id, req_flags=req_flags
        )
        if handle_id and autoclose:
            self.close_file(handle_id)
        if verify:
            assert ret_code == 0
        return ret_code, None, target_attr

    def hole_punch(
            self,
            file_path,
            offset,
            length,
            req_flags=None,
            verify=True,
    ):
        """Deallocate the specified range of the file
        :param str file_path: path of the file
        :param int offset: beginning of the range
        :param int length: length of the range
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        :param bool verify: if True, check ret_code==0
        """
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()

        inode = self.get_inode(file_path, verify=False)
        if not isinstance(inode, self.fsapi.Inode):
            return inode, None, None
        open_parameters = self.fsapi.OpenParameters()
        open_parameters.open_disposition = self.fsapi.OpenDisposition.open_existing
        open_parameters.access_mode = AccessModes.READ | AccessModes.WRITE | AccessModes.DELETE
        open_parameters.share_mode = AccessModes.READ | AccessModes.WRITE | AccessModes.DELETE
        ret_code, handle, target_attr = self.api.Open(
            self.fs_info,
            inode,
            req_flags=self.fsapi.RequestFlags(),
            open_flags=os.O_WRONLY,
            open_parameters=open_parameters
        )
        # Call HolePunch
        ret_code, target_attr = self.api.HolePunch(
            self.fs_info, handle, req_flags=req_flags, offset=offset, len=length
        )
        if verify:
            assert ret_code == 0
        self.close_file(handle)
        return ret_code, None, target_attr

    def find_sparse_region(
            self,
            file_path,
            offset,
            seek_type,
            verify=True,
    ):
        """Find the desired file offset as determined by seek_type
        :param str file_path: path of the file
        :param int offset: beginning of the range
        :param int seek_type: seek type
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        :param bool verify: if True, check ret_code==0
        """
        inode = self.get_inode(file_path, verify=False)
        if not isinstance(inode, self.fsapi.Inode):
            return inode, None, None
        open_parameters = self.fsapi.OpenParameters()
        open_parameters.open_disposition = self.fsapi.OpenDisposition.open_existing
        open_parameters.access_mode = AccessModes.READ | AccessModes.WRITE | AccessModes.DELETE
        open_parameters.share_mode = AccessModes.READ | AccessModes.WRITE | AccessModes.DELETE
        ret_code, handle, target_attr = self.api.Open(
            self.fs_info,
            inode,
            req_flags=self.fsapi.RequestFlags(),
            open_flags=os.O_RDONLY,
            open_parameters=open_parameters
        )
        seek_type = self.fsapi.SeekType(seek_type)
        # Call FindSparseRegion
        ret_code, match_offset = self.api.FindSparseRegion(
            self.fs_info, handle, offset=offset, seek_type=seek_type
        )
        if verify:
            assert ret_code == 0
        self.close_file(handle)
        return ret_code, match_offset

    def open_dir(
            self,
            dir_path,
            req_flags=None,
            open_flags=os.O_DIRECTORY,
            open_parameters=None,
            verify=True,
    ):
        """Get dir_handle for a given folder
        :param str dir_path: path of the dir
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        :param int open_flags: open_flags
        :param bool verify: if True, check ret_code==0
        """
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        if not open_parameters:
            open_parameters = self.fsapi.OpenParameters()
            open_parameters.open_disposition = self.fsapi.OpenDisposition.open_existing
            open_parameters.access_mode = AccessModes.READ | AccessModes.WRITE | AccessModes.DELETE
            open_parameters.share_mode = AccessModes.READ | AccessModes.WRITE | AccessModes.DELETE
        dir_inode = self.get_inode(dir_path)

        ret_code, handle, target_attr = self.api.Open(
            self.fs_info, dir_inode, req_flags=req_flags, open_flags=open_flags, open_parameters=open_parameters
        )
        if verify:
            assert ret_code == 0
        return ret_code, None, target_attr, handle

    def upgrade_open(
            self,
            handle,
            req_flags=None,
            open_flags=os.O_DIRECTORY,
            open_parameters=None,
            verify=True,
    ):
        """Upgrade the open mode of an open handle for a file, directory or stream
        :param Handle handle: handle to downgrade
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        :param int open_flags: open_flags
        :param bool verify: if True, check ret_code==0
        """
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        if not open_parameters:
            open_parameters = self.fsapi.OpenParameters()
            open_parameters.open_disposition = self.fsapi.OpenDisposition.open_existing
            open_parameters.access_mode = AccessModes.READ | AccessModes.WRITE | AccessModes.DELETE
            open_parameters.share_mode = AccessModes.READ | AccessModes.WRITE | AccessModes.DELETE
        ret_code, target_attr = self.api.UpgradeOpen(
            self.fs_info, handle, req_flags, open_flags, open_parameters
        )
        if verify:
            assert ret_code == 0
        return ret_code, None, target_attr

    def downgrade_open(
            self,
            handle,
            access_mode,
            shared_mode,
            verify=True,
    ):
        """Downgrade the open mode of an open handle for a file, directory or stream
        :param Handle handle: handle to downgrade
        :param int access_mode: new access mode
        :param int shared_mode: new shared mode
        :param bool verify: if True, check ret_code==0
        """
        ret_code = self.api.DowngradeOpen(
            self.fs_info, handle, access_mode, shared_mode
        )
        if verify:
            assert ret_code == 0
        return ret_code

    def get_parent_objectNumber(self, object_path, verify=True):
        """Returns fsapi type"""
        inode = self.get_inode(object_path)
        ret_code, parent_id = self.api.GetParentObjectNumber(
            self.fs_info, inode
        )
        if verify:
            assert ret_code == 0
        return ret_code, parent_id

    def mkdir(
            self,
            folder_path,
            stat_info=None,
            extra_info=None,
            req_flags=None,
            unix_mode=None,
            verify=True,
    ):
        """Create folder defined by folder_path
        :param str folder_path: path of the dir
        :param int unix_mode: OS mode of the created folder
        :param StatInfo stat_info: input attributes for file
        :param ExtraInfo extra_info: input attributes for file
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        :param bool verify: if True, check result
        """
        folder_name = os.path.split(folder_path)[1]
        parent_folder = os.path.split(folder_path)[0]
        if not extra_info:
            extra_info = self.fsapi.ExtraInfo()
            for key, val in EXTRA_INFO_TYPICAL_VALUES.items():
                extra_info.__setattr__(key, val)
        if not stat_info:
            stat_info = self.fsapi.StatInfo()
            for key, val in STAT_TYPICAL_VALUES.items():
                stat_info.__setattr__(key, val)
            stat_info.unix_mode = unix_mode if unix_mode else 0o755
            stat_info.unix_mode |= stat.S_IFDIR
        try:
            parent_inode = self.get_inode(parent_folder)
        except AssertionError as e:
            return [-1, f"parent folder does not exist: {e}"]
            # if no flag object is passed, all flags are active by default
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()

        ret_code, inode, parent_attr, target_attr = self.api.MkDir(
            self.fs_info,
            parent_inode,
            folder_name,
            stat_info,
            extra_info,
            req_flags,
        )
        if verify:
            assert ret_code == 0
        return ret_code, parent_attr, target_attr, inode

    def close_dir(self, dir_handle, req_flags=None, verify=True):
        """Closes dir_handle
        :param fsapi.GfsHandle/fsapi.SofsHandle/fsapi.PassthruHandle dir_handle: file dir_handle
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        :param bool verify: if True, check result
        """
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        ret_val = self.api.Close(self.fs_info, dir_handle, req_flags)
        if verify:
            assert ret_val == 0
        return ret_val

    def read_dir_entries(
            self,
            dir_path,
            req_flags=None,
            cookie=0,
            max_entries=MAX_READDIR_ENTRIES,
    ):
        """Read dir on dir_path
        :param str dir_path: dir path
        :param int cookie: Return entries from that index
        :param int max_entries: max number of returned entries
        :param RequestFlags req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        Returns True if everything executes correctly, error otherwise
        """
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        # Get dir handle by path
        ret_code, _, _, dir_handle = self.open_dir(
            dir_path, verify=False, open_flags=os.O_DIRECTORY | os.O_RDONLY
        )
        if not isinstance(dir_handle, self.get_handle_object_type()):
            return ret_code, None, None, {}
        ret_code, dir_entries, names, target_attr = self.api.ReadDirEntries(
            self.fs_info, dir_handle, cookie, max_entries, req_flags
        )
        if ret_code != 0:
            self.close_dir(dir_handle)
            return ret_code, None, None, {}
        self.close_dir(dir_handle)
        dict_entries = {}
        if dir_entries:
            for entry, name in zip(dir_entries, names):
                dict_entries[name] = entry
        return ret_code, None, target_attr, dict_entries

    def create_file(
            self,
            file_path,
            stat_info=None,
            extra_info=None,
            req_flags=None,
            open_flags=0o100,
            open_parameters=None,
            unix_mode=None,
            verify=True,
            autoclose=True
    ):
        """Creates a file defined by file_path
        :param str file_path: path of the file
        :param StatInfo stat_info: input attributes for file
        :param ExtraInfo extra_info: input attributes for file
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        :param int open_flags: OS flags for file creation, default 100
        :param int unix_mode: OS mode of the created folder
        :param bool verify: if True, check result
        Returns tuple of inode, file_handle, parent_attr, target_attr
        if everything ok, error otherwise
        """
        if not open_parameters:
            open_parameters = self.fsapi.OpenParameters()
            open_parameters.open_disposition = self.fsapi.OpenDisposition.create_new
            open_parameters.access_mode = AccessModes.READ | AccessModes.WRITE | AccessModes.DELETE
            open_parameters.share_mode = AccessModes.READ | AccessModes.WRITE | AccessModes.DELETE

        folder_inode = self.get_inode(
            os.path.split(file_path)[0], verify=verify
        )
        if not isinstance(folder_inode, self.fsapi.Inode):
            return folder_inode
        if not stat_info:
            stat_info = self.fsapi.StatInfo()
            for key, val in STAT_TYPICAL_VALUES.items():
                stat_info.__setattr__(key, val)
            stat_info.unix_mode = unix_mode if unix_mode else 0o755
            stat_info.unix_mode |= stat.S_IFREG
        if not extra_info:
            extra_info = self.fsapi.ExtraInfo()
            for key, val in EXTRA_INFO_TYPICAL_VALUES.items():
                extra_info.__setattr__(key, val)

        # if no flag object is passed, all flags are active by default
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()

        ret_code, inode, handle, parent_attr, target_attr = self.api.Create(
            self.fs_info,
            folder_inode,
            os.path.split(file_path)[1],
            stat_info,
            extra_info,
            req_flags=req_flags,
            open_flags=open_flags,
            open_parameters=open_parameters
        )
        if handle and autoclose:
            self.close_file(handle)
        if verify:
            assert ret_code == 0
        return ret_code, parent_attr, target_attr, inode, handle

    def create_stream(
            self,
            inode,
            name,
            req_flags=None,
            verify=True,
    ):
        """Creates a stream
        :param Inode inode: file/dir inode to associate with the stream
        :param str name: name of the stream
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        :param bool verify: if True, check result
        Returns return code and stream id
        """
        if not isinstance(inode, self.fsapi.Inode):
            return False
        # if no flag object is passed, no flags are requested by default
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()

        ret_code, stream_id, base_attr = self.api.CreateStream(
            self.fs_info, inode, name, req_flags
        )
        if verify:
            assert ret_code == 0
        return ret_code, None, base_attr, stream_id

    def open_stream(
            self,
            inode,
            name=None,
            stream_id=None,
            open_flags=0o100,
            open_parameters=None,
            req_flags=None,
            verify=True,
    ):
        """Opens the stream defined by inode and name/id, either name or id must be provided, not both
        :param str name: name of the stream to open
        :param int stream_id: id of the stream to open
        :param Inode inode: file/dir inode associated with the stream
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        :param int open_flags: OS flags for file open
        Returns the stream id and handler if it is open correctly, error otherwise
        """
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        if not open_parameters:
            open_parameters = self.fsapi.OpenParameters()
            open_parameters.open_disposition = self.fsapi.OpenDisposition.open_existing
            open_parameters.access_mode = AccessModes.READ | AccessModes.WRITE | AccessModes.DELETE
            open_parameters.share_mode = AccessModes.READ | AccessModes.WRITE | AccessModes.DELETE
        if not isinstance(inode, self.fsapi.Inode):
            return False

        if stream_id and name:
            raise AssertionError("either name or id must be provided, not both")

        if not name:
            assert stream_id, f"Error, either name or id must be provided"
            ret_code, handle, base_attr = self.api.OpenStreamById(
                self.fs_info, inode, stream_id, req_flags, open_flags, open_parameters=open_parameters
            )
        elif not stream_id:
            assert name, f"Error, either name or id must be provided"
            ret_code, stream_id, handle, base_attr = self.api.OpenStreamByName(
                self.fs_info, inode, name, req_flags, open_flags, open_parameters=open_parameters
            )

        if verify:
            assert ret_code == 0
        return ret_code, None, base_attr, stream_id, handle

    def delete_stream(
            self,
            inode,
            name=None,
            req_flags=None,
            verify=True,
    ):
        """Deletes the stream defined by inode and name
        :param str name: name of the stream to delete
        :param Inode inode: file/dir inode associated with the stream
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        Returns 0 if delete is ok, error otherwise
        """
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        if not isinstance(inode, self.fsapi.Inode):
            return False

        ret_code, base_attr = self.api.DeleteStream(
            self.fs_info, inode, name, req_flags
        )
        if verify:
            assert ret_code == 0
        return ret_code, None, base_attr

    def list_streams(
            self, inode, resume_id=0, buffer_size=1024000, verify=True
    ):
        """Lists the streams associated with the provided inode
        :param Inode inode: file/dir inode associated with the stream
        :param int resume_id: resume from
        :param int buffer_size: size of the buffer to fill
        :param bool verify: if True, check result
        Returns stream entries if everything ok, error otherwise
        """
        (
            ret_code,
            stream_entries,
            stream_names,
        ) = self.api.ListStreams(self.fs_info, inode, resume_id, buffer_size)
        if verify:
            assert ret_code == 0
        return ret_code, stream_entries, stream_names

    def rename_stream(
            self,
            inode,
            name_from,
            name_to,
            req_flags=None,
            verify=True,
    ):
        """Renames the stream defined by inode and name,
        :param Inode inode: file/dir inode associated with the stream
        :param str name_from: name of the stream to rename from
        :param str name_to: name of the stream to rename to
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        Returns 0 if rename is ok, error otherwise
        """
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        if not isinstance(inode, self.fsapi.Inode):
            return False

        ret_code, base_attr = self.api.RenameStream(
            self.fs_info, inode, name_from, name_to, req_flags
        )
        if verify:
            assert ret_code == 0
        return ret_code, None, base_attr

    def get_stream_length(
            self,
            inode,
            stream_id,
            verify=True,
    ):
        """Gets the stream length for base inode and stream_id,
        :param Inode inode: file/dir inode associated with the stream
        :param int stream_id: name of the stream to get length from
        Returns 0 if call is ok, as well as the length and bytes_used,
        error otherwise
        """
        if not isinstance(inode, self.fsapi.Inode):
            return False

        ret_code, length, bytes_used = self.api.GetStreamLength(
            self.fs_info, inode, stream_id
        )
        if verify:
            assert ret_code == 0
        return ret_code, length, bytes_used

    def set_stream_length(
            self,
            inode,
            stream_id,
            new_length,
            verify=True,
    ):
        """Sets the stream length for base inode and stream_id to new_length
        :param Inode inode: file/dir inode associated with the stream
        :param int stream_id: id of the stream to get length
        :param int new_length: new length to set
        Returns 0 if call is ok, as well as the length and bytes_used,
        error otherwise
        """
        if not isinstance(inode, self.fsapi.Inode):
            return False

        ret_code = self.api.SetStreamLength(
            self.fs_info, inode, stream_id, new_length
        )[0]
        if verify:
            assert ret_code == 0
        return ret_code

    def write(
            self,
            data,
            file_path=None,
            handle_id=None,
            req_flags=None,
            offset=0,
            write_flags=0,
            verify=True,
    ):
        """Writes data into a file defined by file_path
        :param str file_path: path of the file
        :param str data: buffer to write
        :param Handle handle_id: alternative to filepath,
            handle can be provided directly
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        :param int offset: starting point to write
        :param int write_flags: write flags, default to no-cache
        :param bool verify: if True, check result
        Returns the return code of write operation
        """
        autoclose = False
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        if not handle_id:
            autoclose = True
            assert file_path, "Either file_path or handle_id must be provided"
            file_inode = self.get_inode(file_path, verify=False)
            if not isinstance(file_inode, self.fsapi.Inode):
                if file_inode == errno.ENOENT:
                    print(
                        f"file does not exist: {file_path}, it will be created"
                    )
                    handle_id = self.create_file(file_path)[-1]
                else:
                    return file_inode
            else:
                handle_id = self.open_file(file_path, open_flags=os.O_WRONLY)[
                    -1
                ]
        num_bytes = len(data)

        ret_code, target_attr = self.api.Write(
            self.fs_info,
            handle_id,
            req_flags=req_flags,
            offset=offset,
            length=num_bytes,
            buffer=data,
            write_flags=write_flags,
        )
        # Close write handle
        if autoclose:
            close_code = self.close_file(handle_id)
            assert close_code == 0, f"Error closing file handle: {close_code}"
        if verify:
            assert ret_code == 0
        return ret_code, None, target_attr

    def write_v(
            self,
            data_list,
            file_path=None,
            handle_id=None,
            req_flags=None,
            offset=0,
            write_flags=0,
            verify=True,
    ):
        """Writes data into a file defined by file_path
        :param str file_path: path of the file
        :param str data_list: list of buffers to write
        :param Handle handle_id: alternative to filepath,
            handle can be provided directly
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        :param int offset: starting point to write
        :param int write_flags: write flags, default to no-cache
        :param bool verify: if True, check result
        Returns the return code of write operation
        """
        autoclose = False
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        if not handle_id:
            autoclose = True
            assert file_path, "Either file_path or handle_id must be provided"
            file_inode = self.get_inode(file_path, verify=False)
            if not isinstance(file_inode, self.fsapi.Inode):
                if file_inode == errno.ENOENT:
                    print(
                        f"file does not exist: {file_path}, it will be created"
                    )
                    handle_id = self.create_file(file_path)[-1]
                else:
                    return file_inode
            else:
                handle_id = self.open_file(file_path, open_flags=os.O_WRONLY)[
                    -1
                ]

        ret_code, target_attr = self.api.WriteV(
            self.fs_info,
            handle_id,
            req_flags=req_flags,
            offset=offset,
            iov=data_list,
            write_flags=write_flags,
        )
        # Close write handle
        if autoclose:
            close_code = self.close_file(handle_id)
            assert close_code == 0, f"Error closing file handle: {close_code}"
        if verify:
            assert ret_code == 0
        return ret_code, None, target_attr

    def read(
            self,
            length,
            file_path=None,
            handle_id=None,
            req_flags=None,
            offset=0,
            read_flags=0,
            verify=True,
    ):
        """Reads data from a file defined by file_path
        :param str file_path: path of the file
        :param str length: how many bytes to read
        :param Handle handle_id: alternative to filepath,
            handle can be provided directly
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        :param int offset: starting point to read
        :param int read_flags: read flags, default to 0
        :param bool verify: if True, check result
        Returns the content (bytes) read on read operation
        """
        autoclose = False
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        if not handle_id:
            autoclose = True
            assert file_path, f"either file_path or handle must be provided"
            file_inode = self.get_inode(file_path, verify=False)
            if not isinstance(file_inode, self.fsapi.Inode):
                print(f"Error reading file:{file_path}: {file_inode}")
                return file_inode
            else:
                handle_id = self.open_file(file_path, open_flags=os.O_RDONLY)[
                    -1
                ]

        ret_code, read_result, target_attr = self.api.Read(
            self.fs_info,
            handle_id,
            req_flags=req_flags,
            offset=offset,
            length=length,
            read_flags=read_flags,
        )
        # Close read handle
        if autoclose:
            close_code = self.close_file(handle_id)
            assert close_code == 0, f"Error closing file handle: {close_code}"

        if verify:
            assert ret_code == length
        return ret_code, None, target_attr, read_result

    def close_file(self, file_handle, req_flags=None, verify=True):
        """Closes file_handle
        :param fsapi.GfsHandle file_handle: file handler
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        :param bool verify: if True, check result
        """
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        ret_val = self.api.Close(self.fs_info, file_handle, req_flags)
        if verify:
            assert ret_val == 0
        return ret_val

    def open_file(
            self,
            file_path,
            open_flags=os.O_RDWR,
            open_parameters=None,
            req_flags=None,
            verify=True,
    ):
        """Opens the file defined by file_path
        :param str file_path: path of the file
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        :param int open_flags: OS flags for file open
        :param int open_parameters: Shared access mode parameters
        Returns the file_handle if it is open correctly, error otherwise
        """
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        if not open_parameters:
            open_parameters = self.fsapi.OpenParameters()
            open_parameters.open_disposition = self.fsapi.OpenDisposition.open_existing
            open_parameters.access_mode = AccessModes.READ | AccessModes.WRITE | AccessModes.DELETE
            open_parameters.share_mode = AccessModes.READ | AccessModes.WRITE | AccessModes.DELETE
        file_inode = self.get_inode(file_path)
        parent_inode = self.get_inode(os.path.split(file_path)[0])
        if not isinstance(file_inode, self.fsapi.Inode):
            return file_inode
        if self.fs_type != "ufo":
            ret_code, handle, target_attr = self.api.Open(
                self.fs_info, file_inode, req_flags=req_flags, open_flags=open_flags, open_parameters=open_parameters
            )
        else:
            ret_code, handle, target_attr = self.api.Open(
                self.fs_info, parent_inode, file_inode, req_flags=req_flags, open_flags=open_flags,
                open_parameters=open_parameters
            )

        if verify:
            assert ret_code == 0
        return ret_code, None, target_attr, handle

    def get_attr(self, file_path, verify=True):
        """GetAttr operation
        :param str file_path: path of the file
        :param bool verify: if True, check result
        Returns get_attr result if everything ok, error otherwise
        """
        file_inode = self.get_inode(file_path, verify=verify)
        parent_inode = self.get_inode(os.path.split(file_path)[0], verify=verify)
        if not isinstance(file_inode, self.fsapi.Inode):
            return file_inode
        if self.fs_type != "ufo":
            ret_val = self.api.GetAttr(self.fs_info, file_inode)
        else:
            ret_val = self.api.GetAttr(self.fs_info, parent_inode, file_inode)
        if verify:
            assert ret_val[0] == 0
            return ret_val[1:]
        return ret_val[0]

    def set_attr(
            self,
            file_path,
            stat_info=None,
            extra_info=None,
            control=0,
            dos_flags_mask=0,
            req_flags=None,
            verify=True,
    ):
        """GetAttr operation
        :param str file_path: path of the file
        :param bool verify: if True, check result
        Returns get_attr result if everything ok, error otherwise
        """
        if not stat_info:
            stat_info = self.fsapi.StatInfo()
        if not extra_info:
            extra_info = self.fsapi.ExtraInfo()
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        file_inode = self.get_inode(file_path, verify=verify)
        parent_inode = self.get_inode(os.path.split(file_path)[0], verify=verify)
        if not isinstance(file_inode, self.fsapi.Inode):
            return file_inode
        if self.fs_type != "ufo":
            ret_code, target_attr = self.api.SetAttr(
                self.fs_info,
                file_inode,
                stat_info,
                extra_info,
                control,
                dos_flags_mask,
                req_flags,
            )
        else:
            ret_code, target_attr = self.api.SetAttr(
                self.fs_info,
                parent_inode,
                file_inode,
                stat_info,
                extra_info,
                control,
                dos_flags_mask,
                req_flags,
            )

        if verify:
            assert ret_code == 0, f"code returned is {ret_code}"
        return ret_code, target_attr

    def rename(self, src_path, dst_path, req_flags=None, verify=True):
        """Renames a file/dir defined by src_path and dst_path
        :param str src_path: path of the source
        :param bool verify: if True, check result
        Returns the return code of rename operation
        """
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        src_parent_inode = self.get_inode(os.path.split(src_path)[0])
        src_name = os.path.split(src_path)[1]
        dst_parent_inode = self.get_inode(os.path.split(dst_path)[0])
        dst_name = os.path.split(dst_path)[1]
        (
            ret_code,
            from_parent_attr,
            to_parent_attr,
            target_attr,
        ) = self.api.Rename(
            self.fs_info,
            src_parent_inode,
            src_name,
            dst_parent_inode,
            dst_name,
            req_flags,
        )
        if verify:
            assert ret_code == 0
        return ret_code, from_parent_attr, to_parent_attr, target_attr

    def setxattr(self, file_path, name, value):
        """setxattr operation
        :param str file_path: path of the file
        :param str name: name of the attribute to set
        :param str value: value to set for the attribute
        Returns setxattr result if everything ok, error otherwise
        """
        file_inode = self.get_inode(file_path)
        if not isinstance(file_inode, self.fsapi.Inode):
            return file_inode
        return self.api.Setxattr(self.fs_info, file_inode, name, value)

    def getxattr(self, file_path, name):
        """setxattr operation
        :param str file_path: path of the file
        :param str name: name of the attribute to read
        Returns content of attribute if everything ok, error otherwise
        """
        file_inode = self.get_inode(file_path)
        if not isinstance(file_inode, self.fsapi.Inode):
            return file_inode
        ret_val = self.api.Getxattr(self.fs_info, file_inode, name)
        assert ret_val[0] == 0
        return ret_val[1]

    def listxattr(self, file_path):
        """listxattr operation
        :param str file_path: path of the file
        Returns attributes for the file/dir if everything ok, error otherwise
        """
        file_inode = self.get_inode(file_path)
        if not isinstance(file_inode, self.fsapi.Inode):
            return file_inode
        ret_val = self.api.Listxattr(self.fs_info, file_inode)
        assert ret_val[0] == 0
        return ret_val[1]

    def removexattr(self, file_path, name):
        """removexattr operation
        :param str file_path: path of the file
        :param str name: name of the attribute to remove
        Returns removexattr result if everything ok, error otherwise
        """
        file_inode = self.get_inode(file_path)
        if not isinstance(file_inode, self.fsapi.Inode):
            return file_inode
        ret_val = self.api.Removexattr(self.fs_info, file_inode, name)
        return ret_val

    def link(
            self,
            src_path,
            dst_parent_folder,
            link_name,
            req_flags=None,
            verify=True,
    ):
        """link operation
        :param str src_path: path for the file to link
        :param str dst_parent_folder: path of the parent folder
         for the link to go
        :param str link_name: name of the link
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        :param bool verify: if True, check result
        Returns link result if everything ok, error otherwise
        """
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        dst_parent_inode = self.get_inode(dst_parent_folder, verify=False)
        if not isinstance(dst_parent_inode, self.fsapi.Inode):
            return dst_parent_inode, None, None
        target_inode = self.get_inode(src_path, verify=False)
        if not isinstance(target_inode, self.fsapi.Inode):
            return target_inode, None, None

        ret_code, parent_attr, target_attr = self.api.Link(
            self.fs_info,
            dst_parent_inode,
            link_name,
            target_inode,
            req_flags=req_flags,
        )
        if verify:
            assert ret_code == 0
        return ret_code, parent_attr, target_attr

    def unlink(self, file_path, req_flags=None, verify=True):
        """unlink operation
        :param str file_path: path of the file to unlink
        :param int req_flags: request flags
        :param bool verify: if True, check result,
            if False return either inode or error
        Returns unlink result if everything ok, error otherwise
        """
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        parent_folder = os.path.split(file_path)[0]
        parent_inode = self.get_inode(parent_folder)
        if not isinstance(parent_inode, self.fsapi.Inode):
            return parent_inode, None, None
        file_name = os.path.split(file_path)[1]

        ret_code, parent_attr, target_attr = self.api.Unlink(
            self.fs_info, parent_inode, file_name, req_flags
        )
        if verify:
            assert ret_code == 0
        return ret_code, parent_attr, target_attr

    def read_link(
            self,
            link_path,
            length=255,
            req_flags=None,
            verify=True,
    ):
        """Reads the contents of a symbolic link
        :param str link_path: path where the link is
        :param str length: how many bytes to read
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        :param bool verify: if True, check result
        Returns the content (bytes) read on ReadLink operation
        """
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        link_inode = self.get_inode(link_path, verify=False)
        if not isinstance(link_inode, self.fsapi.Inode):
            print(f"Error getting inode of: {link_path}: {link_inode}")
            return link_inode, None, None, None

        ret_code, read_result, target_attr = self.api.ReadLink(
            self.fs_info, link_inode, req_flags=req_flags, length=length
        )
        if verify:
            assert ret_code == length
        return ret_code, None, target_attr, read_result

    def symlink(
            self,
            link_target,
            dst_parent_folder,
            link_name,
            stat_info=None,
            extra_info=None,
            req_flags=None,
            verify=True,
    ):
        """symlink operation
        :param str link_target: path for the file to link
        :param str dst_parent_folder: path of the parent folder
         for the link to go
        :param str link_name: name of the link
        :param StatInfo stat_info: input attributes for file
        :param ExtraInfo extra_info: input attributes for file
        :param RequestFlag req_flags: flags to request post/pre attributes
            all flags will be disabled if not provided
        Returns link result if everything ok, error otherwise
        """
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        dst_parent_inode = self.get_inode(dst_parent_folder, verify=False)
        if not isinstance(dst_parent_inode, self.fsapi.Inode):
            return dst_parent_inode, None, None
        if not stat_info:
            stat_info = self.fsapi.StatInfo()
            for key, val in STAT_TYPICAL_VALUES.items():
                stat_info.__setattr__(key, val)
            stat_info.unix_mode = stat.S_IFLNK | 0o777
        if not extra_info:
            extra_info = self.fsapi.ExtraInfo()
            for key, val in EXTRA_INFO_TYPICAL_VALUES.items():
                extra_info.__setattr__(key, val)

        ret_code, inode, parent_attr, target_attr = self.api.SymLink(
            self.fs_info,
            dst_parent_inode,
            link_name,
            stat_info,
            extra_info,
            req_flags,
            link_target,
        )
        if verify:
            assert ret_code == 0
        return ret_code, parent_attr, target_attr, inode

    def rmdir(self, folder_path, req_flags=None, verify=True):
        """rmdir operation
        :param str folder_path: path of the folder to remove
        :param int req_flags: request flags
        :param bool verify: if True, check result,
            if False return either inode or error
        Returns rmdir result if everything ok, error otherwise
        """
        if not req_flags:
            req_flags = self.fsapi.RequestFlags()
        folder_name = os.path.split(folder_path)[1]
        parent_folder = os.path.split(folder_path)[0]
        parent_inode = self.get_inode(parent_folder)
        if not isinstance(parent_inode, self.fsapi.Inode):
            return parent_inode

        ret_code, parent_attr = self.api.RmDir(
            self.fs_info, parent_inode, folder_name, req_flags
        )
        if verify:
            assert ret_code == 0
        return ret_code, parent_attr, None

    def create_snapshot(
            self, snap_name, app_search_id="app1", reason="1", verify=True
    ):
        if self.fs_type == "gfs":
            # create snapshot using console cmd
            log.info(
                f"calling socket command to create a snapshot on dataset 1"
            )
            retcode, retval = run_command(
                f"take_snapshot 1 {snap_name} {app_search_id} {reason}",
                timeout=5,
                port=55551,
            )
            if verify:
                assert retcode == 0
            return retcode, retval
        elif self.fs_type == "sofs":
            import cephfs

            fs = cephfs.LibCephFS()
            fs.conf_read_file("/etc/ceph/ceph.conf")
            fs.mount(b"/", "cephfs")
            volume, subvol, subv_path = get_ceph_info()
            log.info(
                f"calling socket command to list available snapshots on dataset 1"
            )
            try:
                subprocess.check_output(
                    f"ceph fs subvolume snapshot create {volume} {subvol} {snap_name}",
                    stderr=subprocess.PIPE,
                    shell=True,
                    timeout=20,
                )
            except (
                    subprocess.TimeoutExpired,
                    subprocess.CalledProcessError,
            ) as exp:
                log.error(exp)
                return False

            svpath = pathlib.Path(subv_path)
            parent_path = svpath.parent
            # Get the parent stat to get the inode number
            # eg : inode number of /volumes/_nogroup/test1
            parent_stat = fs.stat(str(parent_path))

            # Now once we have the bits to build the snapshot path
            # eg: /volumes/_nogroup/test1/d5052b71-39ec-4d0a-9b0b-2091e1723538/.snap/_snap1_1099511627778
            snapshot_path = (
                f"{subv_path}/.snap/_{snap_name}_{parent_stat.st_ino}"
            )
            snapshot_stat = fs.stat(snapshot_path)

            retcode, retval = 0, {"snapshotId": snapshot_stat.st_dev}
            # update VCM mockoon
            for key, val in {
                "testsnap-44": 22,
                "testsnap-45": 23,
                "testsnap-46": 24,
            }.items():
                out = subprocess.call(
                    f'cat /tmp/vcmLocalRESTForSOFS.json | grep "{key}"',
                    stderr=subprocess.PIPE,
                    shell=True,
                    timeout=20,
                )
                if out == 0:
                    replace_snap_name = key
                    replace_snap_id = val
                    break
            subprocess.check_output(
                f"sed -i \"s/: {replace_snap_id}/: {retval['snapshotId']}/g\" /tmp/vcmLocalRESTForSOFS.json",
                stderr=subprocess.PIPE,
                shell=True,
                timeout=20,
            )
            subprocess.check_output(
                f'sed -i "s/{replace_snap_name}/_{snap_name}_{parent_stat.st_ino}/g" /tmp/vcmLocalRESTForSOFS.json',
                stderr=subprocess.PIPE,
                shell=True,
                timeout=20,
            )
            subprocess.check_output(
                f'sed -i "s/test-apid/app1/g" /tmp/vcmLocalRESTForSOFS.json',
                stderr=subprocess.PIPE,
                shell=True,
                timeout=20,
            )
            subprocess.check_output(
                f"sed -i \"s/snapshotId={replace_snap_id}/snapshotId={retval['snapshotId']}/g\" /tmp/vcmLocalRESTForSOFS.json",
                stderr=subprocess.PIPE,
                shell=True,
                timeout=20,
            )
            subprocess.check_output(
                f"sed -i 's/\"value\": \"{replace_snap_id}\"/\"value\": \"{retval['snapshotId']}\"/g' /tmp/vcmLocalRESTForSOFS.json",
                stderr=subprocess.PIPE,
                shell=True,
                timeout=20,
            )
            restart_vcm_mockoon(default=False)
            return retcode, retval, f"_{snap_name}_{parent_stat.st_ino}"
        elif self.fs_type == "fsapi":
            # create snapshot calling function
            retcode, snap_id = self.api.CreateSnapshot(
                "/tmp/passthru_fs", snap_name, app_search_id, reason
            )
            if verify:
                assert retcode == 0
            retval = {"snapshotId": snap_id}

        return retcode, retval

    def delete_snapshot(self, snap_id, verify=True):
        if self.fs_type == "gfs":
            # create snapshot using console cmd
            retcode, retval = run_command(
                f"delete_snapshot 1 {snap_id}",
                timeout=5,
                port=55551,
            )
            if verify:
                assert retcode == 0
            return retcode, retval
        elif self.fs_type == "sofs":
            volume, subvol, subv_path = get_ceph_info()
            log.info(
                f"calling socket command to list available snapshots on dataset 1"
            )
            snap_name = snap_id.split("_")[1]
            svpath = pathlib.Path(f"/mnt/{volume}{subv_path}")
            parent_path = svpath.parent
            snapshot_name = (
                    "_" + snap_name + "_" + str(os.stat(f"{parent_path}").st_ino)
            )
            snapshot_path = str(svpath) + "/.snap/" + snapshot_name
            snapshot_id = os.stat(f"{snapshot_path}").st_dev
            try:
                subprocess.check_output(
                    f"ceph fs subvolume snapshot rm {volume} {subvol} {snap_name}",
                    stderr=subprocess.PIPE,
                    shell=True,
                    timeout=20,
                )
            except (
                    subprocess.TimeoutExpired,
                    subprocess.CalledProcessError,
            ) as exp:
                log.error(exp)
                return False
            # update VCM mockoon
            for key, val in {
                "testsnap-44": 22,
                "testsnap-45": 23,
                "testsnap-46": 24,
            }.items():
                out = subprocess.call(
                    f'cat /tmp/vcmLocalRESTForSOFS.json | grep "{key}"',
                    stderr=subprocess.PIPE,
                    shell=True,
                    timeout=20,
                )
                if out != 0:
                    replace_snap_name = key
                    replace_snap_id = val
                    break
            subprocess.check_output(
                f'sed -i "s/: {snapshot_id}/: {replace_snap_id}/g" /tmp/vcmLocalRESTForSOFS.json',
                stderr=subprocess.PIPE,
                shell=True,
                timeout=20,
            )
            subprocess.check_output(
                f'sed -i "s/{snapshot_name}/{replace_snap_name}/g" /tmp/vcmLocalRESTForSOFS.json',
                stderr=subprocess.PIPE,
                shell=True,
                timeout=20,
            )
            subprocess.check_output(
                f'sed -i "s/app1/test-apid/g" /tmp/vcmLocalRESTForSOFS.json',
                stderr=subprocess.PIPE,
                shell=True,
                timeout=20,
            )
            subprocess.check_output(
                f'sed -i \'s/"value": "{snapshot_id}"/"value": "{replace_snap_id}"/g\' /tmp/vcmLocalRESTForSOFS.json',
                stderr=subprocess.PIPE,
                shell=True,
                timeout=20,
            )
            subprocess.check_output(
                f'sed -i "s/snapshotId={snapshot_id}/snapshotId={replace_snap_id}/g" /tmp/vcmLocalRESTForSOFS.json',
                stderr=subprocess.PIPE,
                shell=True,
                timeout=20,
            )
            restart_vcm_mockoon(default=False)
            retcode, retval = 0, None
            return retcode, retval
        elif self.fs_type == "fsapi":
            snap_id = int(1)
            # create snapshot calling function
            retcode = self.api.DeleteSnapshot(1)
            if verify:
                assert retcode == 0
            retval = None
        return retcode, retval

    def list_snapshots(self, offset=0, buffer_size=1024000000, verify=True):
        """list snapshots operation
        Returns the list of current snapshots for the fs
        """
        (
            ret_code,
            snap_entries,
            app_search_ids,
            snapshot_names,
        ) = self.api.ListSnapshots(self.fs_info, offset, buffer_size)
        if verify:
            assert ret_code == 0
        return ret_code, snap_entries, app_search_ids, snapshot_names

    def get_snapshot_info(self, snapshot_id, buffer_size=1024, verify=True):
        """get snapshot info operation
        :param int snapshot_id: snapshot id
        :param int buffer_size: buffer_size
        :param bool verify: if True, check result
        Returns the info of a specific snapshot if everything ok, error otherwise
        """
        ret_code, snap_info, app_search_id, name = self.api.GetSnapshotInfo(
            self.fs_info, snapshot_id, buffer_size
        )
        if verify:
            assert (
                    ret_code == 0
            ), f"Unable to get snapshot info for id {snapshot_id}"
        return ret_code, snap_info, app_search_id, name

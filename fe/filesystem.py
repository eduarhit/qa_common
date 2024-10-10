# pylint: disable-all
import logging as log
import errno
import random
import os.path


class FileSystem:
    """Class for filesystem objects in which fs operations can be applied.
    Operations are applied when the fs is mounted, either fuse, nfs, smb, etc
    operations wrap the python filesystem functions under the hood.
    """
    def __init__(self, mount_path=None):
        self.mount_path = mount_path

    def create_file(self, file_path):
        """
        Function to create file on given path
        """
        log.debug(f"Creating file {file_path}, root_mount is: {self.mount_path}")

        target_path = os.path.join(self.mount_path, file_path)
        log.debug(f"  ** open({target_path})")
        try:
            with open(target_path, "x") as fd:
                print(fd)
        except IOError as e:
            if e.errno == errno.EROFS:
                return e
            if e.errno not in frozenset(
                    [errno.ENOENT, errno.EEXIST, errno.EISDIR, errno.ENOTDIR]
            ):
                log.debug(f"{target_path} open error({e.errno}): {e.strerror}")
                raise AssertionError
        log.debug(f"Exit create_file for: {target_path}")

    def create_dir(self, dir_path):
        """
        Function to create directory on a given path
        """
        target_path = os.path.join(self.mount_path, dir_path)

        log.debug(f"Creating dir {target_path}, root_mount is: {self.mount_path}")

        log.debug(f"  ** mkdir({target_path})")
        try:
            os.mkdir(f"{target_path}")
        except IOError as e:
            if e.errno == errno.EROFS:
                return e
            if e.errno not in frozenset(
                    [errno.ENOENT, errno.EEXIST, errno.ENOTDIR]
            ):
                log.debug(f"{target_path} mkdir error({e.errno}):  {e.strerror}")
                raise AssertionError
        log.debug(f"Exit create_dir for: {target_path}")

    def read_file(self, file_path, io_size=None):
        """
        Function to read a range of a given file defined by io_size
        if io_size is not defined, the whole file is read
        """
        # Test path
        target_path = os.path.join(self.mount_path, file_path)
        try:
            old_size = os.stat(f"{target_path}").st_size
            if not io_size:
                io_size = old_size
        except IOError as e:
            if e.errno not in frozenset([errno.ENOENT, errno.ENOTDIR]):
                log.debug(f"{target_path} stat error({e.errno}): {e.strerror}")
                raise AssertionError
            else:
                return
        read_len = random.randrange(1, io_size)
        if old_size > 0:
            # Pick an offset which may result in a short read
            offset = random.randrange(0, old_size)
        else:
            offset = 0
        log.debug(f"Reading from file '{target_path}' file_size: {old_size} offset: {offset} "
                  f"len: {read_len}"
                  )
        log.debug(f"** read({target_path})")
        try:
            with open(f"{target_path}", "r") as test_fd:
                test_fd.seek(offset)
                read_buffer = test_fd.read(read_len)
                log.debug(f"  Length read returned: {len(read_buffer)}")
                # We will see either string.ascii_letters or NULL in the data.
                # While we don't write NULL, if a file is truncated to be smaller
                # in between write_file fetching the old file size and writing,
                # we'll get NULL chars in the data.
        except IOError as e:
            # If we got EIO, check for the file having been replaced with a dir or
            # deleted, since this can cause FUSE to return EIO internally, even if
            # the test filesystem did not return any error.
            if e.errno == errno.EIO:
                if os.path.exists(target_path) and os.path.isfile(target_path):
                    log.debug(f"{file_path} read error({e.errno}): {e.strerror} '{target_path}'")
                    raise AssertionError
                else:
                    log.debug(f"Ignoring EIO since '{target_path}' is not a file")
                    return
            if e.errno not in frozenset(
                    [errno.ENOENT, errno.EISDIR, errno.ENOTDIR]
            ):
                log.debug(f"{target_path} read error({e.errno}): {e.strerror}")
                raise AssertionError
        log.debug(f"Exit read_file for: {target_path}")

    def listdir(self, dir_path):
        """
        Function to list dir
        """
        target_path = os.path.join(self.mount_path, dir_path)
        log.debug(f"listdir: '{target_path}'")
        list_dir = os.listdir(target_path)
        log.debug(f"Exit listdir for: {target_path}")
        return list_dir

    def write_file(self, file_path, towrite, split=None):
        """
        Function to write some data to a file
        """
        # towrite is a string
        target_path = os.path.join(self.mount_path, file_path)
        data = towrite.encode()
        log.debug(f"write_file: '{target_path}' length:{len(data)}")
        try:
            fd = -1
            fd = os.open(target_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC)
            if split:
                split_index = int(len(data) / split)
                os.write(fd, data[0:split_index])
                os.write(fd, data[split_index:])
            else:
                os.write(fd, data)
            os.close(fd)
        except:
            if fd != -1:
                os.close(fd)
            raise
        log.debug(f"Exit write_file for: {target_path}")

    def write_file_offset(self, file_path, writedata, offset):
        """
        Function to write specific offset from a file
        """
        target_path = os.path.join(self.mount_path, file_path)
        data = writedata.encode()
        log.debug(f"write_file_offset: '{target_path}' length:{len(data)} offset:{offset}")
        with open(target_path, "ab") as fd:
            fd.seek(offset, os.SEEK_SET)
            fd.write(data)
        fd.close()
        log.debug(f"Exit write_file in offset {offset} for: {target_path}")

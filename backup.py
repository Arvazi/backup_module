"""
backup.py
Create an archive which will be suitable for rsyncing to a remote backup.
 - will not copy data unnecessarily for common operations such as
   renaming a file or reorganising the directory structure.
   (assumes large files are generally going to be immutable, e.g. audio/video)
 - doesn't try to do anything fancy with permissions etc.
   just a simple copy of each file it can read.
 - will do some basic file-level de-duplication.
 
Usage:
 backup.py source-paths-file destination-path [exclude-patterns-file] [--purge]
 source-directories-file should be a text file with paths to be backed up, one per line.
 e.g.
  /first/path/to/backup
  /second/path/to/backup
 
 exclude-patterns-file is an optional text file of (python) regular expressions
 used to exclude matching files from the backup.
 e.g.
  /path/with/big/files
  \\.dont-backup-ext
 --purge will allow space to be recovered by deleting blobs from the backup that
 are not referenced by any files in the manifest.
"""

import hashlib
import re
import sys
import datetime
import tarfile
from shutil import rmtree
from path import path  # available from http://tompaton.com/resources/path.py

# TODO
# - Log to which point backup has proceeded (in case of interruption)
# - Version Control (only backup files which have been modified)
# - Max Backup Size


def backup(sources, excludes, dest, purge=False):
    """Backup the directories in sources to the destination.
    exclude any files that match the patterns in the exclude list.
    store files with names based on a hash of their contents.
    write a manifest mapping the hash to the original file path.
        
    if purge is True, blobs will be removed from the dest folders
    if they are no longer used by any files in the manifest."""

    manifest = {}           # filename --> hash
    collision_check = {}    # hash --> filename
                            # all files with the same hash will have the same contents, so only need one name

    dest = path(dest)
    blobs_path = dest / "blobs"
    exclude = make_predicate(excludes)

    for source in map(path.normpath, map(path, sources)):
        print(f"Backing up {source} ({datetime.datetime.now()})...")
        for fn in source.walkfiles(errors="warn"):

            if exclude(str(fn)):
                continue

            try:
                hsh = file_hash(fn)
            except Exception as e:
                print(e)
                continue

            if hsh in collision_check:
                if not files_identical(fn, collision_check[hsh]):
                    raise Exception('Hash collision!!! Aborting backup')

            blob_path = blobs_path / hsh[:2] / hsh
            if not blob_path.exists():
                if not blob_path.parent.exists():
                    blob_path.parent.makedirs()
                try:
                     # no point copying attrs, as there could be multiple files using this blob
                    fn.copy(blob_path)
                except Exception as e:
                    print(f'Error copying file, skipping.\n{fn}\n{e}\n' % (fn, e))
                    continue

            manifest[str(fn)] = hsh
            # all files with the same hash will have the same contents, so only need one name
            collision_check[hsh] = fn

    print("Writing manifest...")
    (blobs_path / "manifest").write_lines("%s\t%s" % (hsh, fn)
                                    for fn, hsh in sorted(manifest.items()))

    # remove unreferenced blobs
    if purge:
        for d in blobs_path.dirs():
            for f in d.files():
                if f.name not in collision_check:
                    f.unlink()

    print("Compressing Backup..")
    backup_archive = backup_compress(blobs_path, dest)

    rmtree(blobs_path)

    print(f"Backup done {backup_archive}")

def file_hash(fn):
    """sha256 hash of file contents."""
    return file_hash_py(fn).hexdigest()
    
def file_hash_py(fileobj):
    """sha256 hash of file contents, without reading entire file into memory."""
    hsh = hashlib.sha256()
    f = fileobj.open('rb')
    while True:
        chunk = f.read(8192)
        if not chunk:
            break
        hsh.update(chunk)
    return hsh

def files_identical(f1, f2):
    """check if files are really the same."""
    return files_identical_py(f1, f2)
    
def files_identical_py(f1, f2):
    """check if files are really the same."""
    # if they are equal, then adding an extra character to both will generate the same hash
    # if they are different, then the extra character will generate two different hashes this time
    hsh1, hsh2 = file_hash_py(f1), file_hash_py(f2)
    hsh1.update('0'.encode())
    hsh2.update('0'.encode())
    return hsh1.hexdigest() == hsh2.hexdigest()


def backup_compress(source_path, dest):
    """compress and pack backup to .tar.gz"""
    name = str(datetime.datetime.now()) + ".tar.gz"
    # name = "backup.tar.gz"
    tar = tarfile.open(f"{path(dest/name).normpath()}", "w:gz")
    tar.add(source_path, name)
    tar.close()
    return name


def backup_decompress(archive, dest):
    """uncompress and unpack backup from .tar.gz"""

    if not tarfile.is_tarfile(archive):
        raise ValueError(f"File seems to be no tarfile: {archive}")

    tar = tarfile.open(archive, "r:gz")
    tar.extractall(dest)
    tar.close()


def make_predicate(tests):
    """return function that tests a filename against a list of regular expressions and returns True if any match."""
    tests = map(re.compile, tests)

    def _inner(fn):
        for test in tests:
            if test.search(fn):
                return True
        return False
    return _inner


def restore(archive, dest, subset=None):
    """Restore all files to their original names in the given target directory.
    optionally restoring only the subset that match the given list of regular expressions."""
    dest = path(dest).normpath()
    blobs = dest / "blobs/"

    if not blobs.exists():
        blobs.makedirs()

    backup_decompress(archive, str(blobs))

    manifest = path(blobs / (path(archive).namebase + ".gz") / "manifest").normpath()

    if subset:
        matches = make_predicate(subset)
    else:
        matches = lambda fn: True

    for line in manifest.lines():
        hsh, fn = line.strip().split("\t")
        if matches(fn):
            print(f"Restoring: {fn} ({datetime.datetime.now()})")
            if fn[0] == '/':
                fn = fn[1:]
            fn = dest / fn
            if not fn.parent.exists():
                fn.parent.makedirs()
            hsh = manifest.parent / hsh[:2] / hsh
            hsh.copy(fn)

    rmtree(str(blobs))
    
    print(f"Restored Backup {datetime.datetime.now()}")


if __name__ == "__main__":
    # TODO
    # arg exlcudes in restore can't be used right now

    if '--purge' in sys.argv:
        purge = True
        sys.argv.remove('--purge')
    else:
        purge = False


    if len(sys.argv) == 5:
        excludes = filter(
            None, map(str.strip, path(sys.argv.pop()).lines()))
    else:
        excludes = []
    if len(sys.argv) != 4:
        raise Exception('Invalid arguments.')
    dest = path(sys.argv.pop()).normpath()
    sources = path(sys.argv.pop()).normpath()
    mode = sys.argv.pop()

    if mode == "-b":
        sources = filter(None, map(str.strip, path(sources).lines()))
        backup(sources, excludes, dest, purge)
    elif mode == "-r":
        manifest = sources
        excludes = None
        restore(manifest, dest, excludes)
    else:
        print("Unknown Mode: " + mode)

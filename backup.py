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
from os import unlink, linesep, path
from shutil import rmtree, copy
from pathlib import Path

# TODO
# - Log to which point backup has proceeded (in case of interruption)
# - Version Control (only backup files which have been modified)
# - Max Backup Size


def backup(sourceFile, excludes, dest, purge=False, lastBackup=None):
    """Backup the directories in sources to the destination.
    exclude any files that match the patterns in the exclude list.
    store files with names based on a hash of their contents.
    write a manifest mapping the hash to the original file path.

    if purge is True, blobs will be removed from the dest folders
    if they are no longer used by any files in the manifest."""

    manifest = {}           # filename --> hash
    collision_check = {}    # hash --> filename
                            # all files with the same hash will have the same contents, so only need one name

    dest = Path(dest)
    blobs_path = dest / "blobs"
    exclude = make_predicate(excludes)

    if not Path(sourceFile).is_file():
        print(f"Sources File is no File: {sourceFile}")
        return

    with open(sourceFile) as f:
        sources = f.readlines()
    sources = map(Path, map(str.strip, sources))

    for source in sources:
        print(f"Backing up {source} ({datetime.datetime.now()})...")
        for fn in source.glob("**/*.*"):

            if fn.is_dir():
                continue

            if exclude(str(fn)):
                continue

            lastMod = path.getmtime(str(fn))
            if lastMod > int(lastBackup):
                print(
                    f"Skipping file because wasn't modified since last backup: {fn}")
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
                    blob_path.parent.mkdir(parents=True)
                try:
                     # no point copying attrs, as there could be multiple files using this blob
                    copy(str(fn), str(blob_path))
                except Exception as e:
                    print(
                        f'Error copying file, skipping.\n{fn}\n{e}\n' % (fn, e))
                    continue

            manifest[str(fn)] = hsh
            # all files with the same hash will have the same contents, so only need one name
            collision_check[hsh] = fn

    print("Writing manifest...")
    if not blobs_path.exists():
        blobs_path.mkdir(parents=True)
    with open(blobs_path / "manifest", "a") as f:
        for fn, hsh in sorted(manifest.items()):
            f.write(f"{hsh}\t{fn}" + linesep)
        f.write(f"lastBackup:{lastBackup}" + linesep)

    # remove unreferenced blobs
    if purge:
        for d in blobs_path.glob("*/"):
            if d.is_dir():
                for f in d.glob("*.*"):
                    if f.name not in collision_check:
                        unlink(f)

    print("Compressing Backup..")
    backup_archive = backup_compress(blobs_path, dest)

    rmtree(blobs_path)

    print(f"Backup done {backup_archive}")


def restore(archive, dest, subset=None):
    """Restore all files to their original names in the given target directory.
    optionally restoring only the subset that match the given list of regular expressions."""
    dest = Path(dest)
    blobs = dest / "blobs/"

    if not blobs.exists():
        blobs.mkdir(parents=True)

    backup_decompress(archive, str(blobs))

    manifest = blobs / Path(Path(archive).name) / "manifest"

    if subset:
        matches = make_predicate(subset)
    else:
        matches = lambda fn: True

    with open(manifest) as f:
        lines = f.readlines()

    if "lastBackup" in sources[-1]:
        lastBackup = sources.pop().split(":")[-1]

    for line in lines:
        hsh, fn = line.strip().split("\t")
        if matches(fn):
            print(f"Restoring: {fn} ({datetime.datetime.now()})")
            if fn[0] == '/':
                fn = fn[1:]
            fn = dest / fn
            if not fn.parent.exists():
                fn.parent.mkdir(parents=True)
            hsh = manifest.parent / hsh[:2] / hsh
            copy(str(hsh), str(fn))

    rmtree(str(blobs))

    print(f"Restored Backup {datetime.datetime.now()}")


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
    name = str(round(datetime.datetime.timestamp(datetime.datetime.now()))) + ".tar.gz"
    # name = "backup.tar.gz"
    tar=tarfile.open(f"{Path(dest/name)}", "w:gz")
    tar.add(source_path, name)
    tar.close()
    return name


def backup_decompress(archive, dest):
    """uncompress and unpack backup from .tar.gz"""

    if not tarfile.is_tarfile(archive):
        raise ValueError(f"File seems to be no tarfile: {archive}")

    tar=tarfile.open(archive, "r:gz")
    tar.extractall(dest)
    tar.close()

def check_prev_existence(last):


def make_predicate(tests):
    """return function that tests a filename against a list of regular expressions and returns
    True if any match."""
    tests=map(re.compile, tests)

    def _inner(fn):
        for test in tests:
            if test.search(fn):
                return True
        return False
    return _inner


if __name__ == "__main__":
    # TODO
    # arg exlcudes in restore can't be used right now

    if '--purge' in sys.argv:
        purge=True
        sys.argv.remove('--purge')
    else:
        purge=False

    lastModified=None
    if len(sys.argv) == 5:
        lastModified=sys.argv.pop()

    # if len(sys.argv) == 5:
    #     excludes = filter(
    #         None, map(str.strip, open(Path(sys.argv.pop())).readlines()))
    # else:
    #     excludes = []
    excludes=[]
    if len(sys.argv) != 4:
        raise Exception('Invalid arguments.')
    dest=sys.argv.pop()
    sources=sys.argv.pop()
    mode=sys.argv.pop()

    if mode == "-b":
        backup(sources, excludes, dest, purge, lastModified)
    elif mode == "-r":
        manifest=sources
        excludes=None
        restore(manifest, dest, excludes)
    else:
        print("Unknown Mode: " + mode)

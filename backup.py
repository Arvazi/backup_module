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
# - manifest should link diretctly to the backup containing the not modified version of file instead of linking to predecessor
# - Max Backup Size
# - Check for maxbackup size if backups are deleted which are linked to newer ones


def backup(sourceFile, excludes, dest, purge=False, last_backup=None):
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

            # check if mod date is older than lastbackup --> search if backup still exists --> skip file
            lastMod = path.getmtime(str(fn))
            if lastMod < int(last_backup):
                if recursive_search_for_file(fn, dest, last_backup):
                    manifest[str(fn)] = f"lb:{last_backup}"

                    print(
                        f"Skipping: file wasn't modified since last backup: {fn}")
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
        f.write(f"lastBackup:{last_backup}:" + linesep)

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

    manifests_path = (Path(dest) / "manifests")
    if manifests_path.exists:
        rmtree(str(manifests_path))

    print(f"Backup done {backup_archive}")


def restore(archive, dest, subset=None):
    """Restore all files to their original names in the given target directory.
    optionally restoring only the subset that match the given list of regular expressions."""
    dest = Path(dest)
    blobs_path = dest / "blobs/"

    if not blobs_path.exists():
        blobs_path.mkdir(parents=True)

    backup_decompress(archive, str(blobs_path))

    manifest = blobs_path / Path(Path(archive).name) / "manifest"

    if subset:
        matches = make_predicate(subset)
    else:
        matches = lambda fn: True

    with open(manifest) as f:
        lines = f.readlines()
        lines.pop()

    for line in lines:
        hsh, fn = line.strip().split("\t")

        if not matches(fn):
            continue

        print(f"Restoring: {fn} ({datetime.datetime.now()})")

        if "lb:" in hsh:
            lb_name = hsh.split(":")[1]
            hsh = recursive_restore(
                fn, Path(archive).parent, lb_name, blobs_path / Path(Path(archive).name))
            if not hsh:
                print(
                    f"Error restoring file {fn} ({datetime.datetime.now()})! Couldn't find it recursive")
                continue

        hsh_path = manifest.parent / hsh[:2] / hsh

        if fn[0] == '/':
            fn = fn[1:]

        f_path = dest / fn
        if not f_path.parent.exists():
            f_path.parent.mkdir(parents=True)
        copy(str(hsh_path), str(f_path))

    rmtree(str(blobs_path))

    manifests_path = (Path(Path(archive).parent) / "manifests")
    if manifests_path.exists:
        rmtree(str(manifests_path))

    print(f"Restored Backup {datetime.datetime.now()}")


def recursive_restore(fn, backup_path, last_backup, blobs_path):
    zip_path = backup_path / (last_backup + ".tar.gz")

    if not Path(zip_path).exists():
        return None

    lines = read_compressed_manifest(zip_path)
    search_res = search_manifest_file(lines, fn)
    if len(search_res) == 64:
        hsh = search_res
        hsh_path = Path(hsh[:2]) / hsh

        tar = tarfile.open(zip_path, "r:gz")

        f_path_in_zip = Path(zip_path).name / hsh_path

        if not (blobs_path / hsh_path).parent.exists():
            (blobs_path / hsh_path).parent.mkdir(parents=True)
        f = open(str(blobs_path / hsh_path), "wb")
        f.write(tar.extractfile(str(f_path_in_zip)).read())
        f.close()
        tar.close()

        return hsh
    elif not search_res:
        return None
    else:
        return recursive_restore(fn, backup_path, search_res, blobs_path)
    return None


def recursive_search_for_file(fn, backup_path, last_backup):
    """recursive through all backups
    beginning with specified last_backup
    searches for the fn name and if there is a backup with a hash for the file"""
    zip_path = backup_path / (last_backup + ".tar.gz")

    if not Path(zip_path).exists():
        return False

    lines = read_compressed_manifest(zip_path)
    search_res = search_manifest_file(lines, fn)

    if not search_res:
        return False
    elif len(search_res) == 64:
        return True
    elif recursive_search_for_file(fn, backup_path, search_res):
        return True
    return False


def search_manifest_file(lines, fn):
    """Search in manifest as lines array
     if there is linked a give filename (or path)
      and give back the hash or the lastbackup file."""
    for line in lines:
        if str(fn) in line:
            leftside = line.split("\t")[0]
            if ":" in leftside:
                # retrieve last backup time from line
                return leftside.split(":")[1]
            else:
                return leftside
    return None


def read_compressed_manifest(zip_path):
    """Read from a .tar.gz compressed backup the manifest and return the lines as array"""

    # Check if manifest was already unpacked in this session and read from there
    manifests_path = Path(zip_path).parent / "manifests"
    manifest = manifests_path / re.search(r"^\d+", Path(zip_path).name).group()
    if manifests_path.exists() and manifest.exists():
        with open(manifest) as f:
            return f.readlines()

    tar = tarfile.open(zip_path, "r:gz")
    manifest_path = Path(zip_path).name / Path("manifest")
    f = tar.extractfile(str(manifest_path))
    lines = map(bytes.decode, f.readlines())
    tar.close()

    # temp save manifest in case it'd be opened more times this session
    if not manifests_path.exists():
        manifests_path.mkdir(parents=True)
    if not manifest.exists():
        with open(manifest, "w") as f:
            f.writelines(lines)

    return lines

def get_manifest_lastbackup(lines):
    if "lastBackup" in lines[-1]:
        return lines.pop().split(":")[1]

def file_hash(fn):
    """sha256 hash of file contents."""
    return file_hash_py(fn).hexdigest()


def file_hash_py(fileobj):
    """sha256 hash of file contents, without reading entire file into memory."""
    hsh=hashlib.sha256()
    f=fileobj.open('rb')
    while True:
        chunk=f.read(8192)
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
    hsh1, hsh2=file_hash_py(f1), file_hash_py(f2)
    hsh1.update('0'.encode())
    hsh2.update('0'.encode())
    return hsh1.hexdigest() == hsh2.hexdigest()


def backup_compress(source_path, dest):
    """compress and pack backup to .tar.gz"""
    name=str(round(datetime.datetime.timestamp(
        datetime.datetime.now()))) + ".tar.gz"
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

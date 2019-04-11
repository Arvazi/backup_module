"""
backup.py

Create an archive which will be suitable for rsyncing to a remote backup.
 - will not copy data unnecessarily for common operations such as
   renaming a file or reorganising the directory structure.
   (assumes large files are generally going to be immutable, e.g. audio/video)

 - doesn't try to do anything fancy with permissions etc. Just a simple copy of each file.

Usage:
 backup.py source-paths-file destination-path [exclude-patterns-file]

 source-directories-file should be a text file with paths to be backed up, one per line.

 exclude-patterns-file is an optional text file of (python) regular expressions
 used to exclude matching files from the backup.
"""

import hashlib
import re
import sys
from path import path  # available from http://tompaton.com/resources/path.py

# TODO
# - Log to which point backup has proceeded (in case of interruption)
# - Version Control
# - Max Backup Size
# - Compression


def backup(sources, excludes, dest):
    """Backup the directories in sources to the destination.
    exclude any files that match the patterns in the exclude list.
    store files with names based on a hash of their contents.
    write a manifest mapping the hash to the original file path."""

    manifest = {}         # filename --> hash
    collision_check = {}  # hash --> filename

    dest = path(dest)
    exclude = make_predicate(excludes)

    for source in map(path, sources):
        print(f"Backing up {source}...")
        for fn in source.walkfiles():
            if exclude(str(fn)):
                continue

            hsh = file_hash(fn)
            digest = hsh.hexdigest()
            if digest in collision_check:
                # check if files are really the same
                # if they are equal, then adding an extra character to both will generate the same hash
                # if they are different, then the extra character will generate two different hashes this time
                hsh2 = file_hash(collision_check[digest])
                hsh.update('0'.encode())
                hsh2.update('0'.encode())

                if hsh.hexdigest() != hsh2.hexdigest():
                    raise Exception('Hash collision!!! Aborting backup')

            blob_path = dest / digest[:2] / digest
            if not blob_path.exists():
                if not blob_path.parent.exists():
                    blob_path.parent.makedirs()
                try:
                    fn.copy(blob_path)
                except Exception as e:
                    print('Error copying file, skipping.\n%s\n%s\n' % (fn, e))
                    continue

            manifest[str(fn)] = digest
            # all files with the same hash will have the same contents, so only need one name
            collision_check[digest] = fn

    print("Writing manifest...")
    (dest / "manifest").write_lines("%s\t%s" % (hsh, fn)
                                    for fn, hsh in sorted(manifest.items()))

    # remove unreferenced blobs
    for d in dest.dirs():
        for f in d.files():
            if f.name not in collision_check:
                f.unlink()

    print("Done.")


def file_hash(fileobj):
    """sha256 hash of file contents, without reading entire file into memory."""
    hsh = hashlib.sha256()
    # with fileobj.open('rb') as f:
    #     for chunk in iter(lambda: f.read(8192), ''):
    #         hsh.update(chunk)
    f = fileobj.open('rb')
    while True:
        chunk = f.read(8192)
        if not chunk:
            break
        hsh.update(chunk)
    return hsh


def make_predicate(tests):
    """return function that tests a filename against a list of regular expressions and returns True if any match."""
    tests = map(re.compile, tests)

    def _inner(fn):
        for test in tests:
            if test.search(fn):
                return True
        return False
    return _inner


def restore(manifest, dest, subset=None):
    """Restore all files to their original names in the given target directory.
    optionally restoring only the subset that match the given list of regular expressions."""
    dest = path(dest)
    manifest = path(manifest)
    if subset:
        matches = make_predicate(subset)
    else:
        def matches(fn): return True

    for line in manifest.lines():
        hsh, fn = line.strip().split("\t")
        if matches(fn):
            if fn[0] == '/':
                fn = fn[1:]
            fn = dest / fn
            if not fn.parent.exists():
                fn.parent.makedirs()
            hsh = manifest.parent / hsh[:2] / hsh
            hsh.copy(fn)


if __name__ == "__main__":
    # TODO
    # arg exlcudes in restore can't be used right now

    if len(sys.argv) == 5:
        excludes = filter(
            None, map(str.strip, path(sys.argv.pop()).lines()))
    else:
        excludes = []
    if len(sys.argv) != 4:
        raise Exception('Invalid arguments.')
    dest = sys.argv.pop()
    sources = sys.argv.pop()
    mode = sys.argv.pop()

    if mode == "-b":
        sources = filter(None, map(str.strip, path(sources).lines()))
        backup(sources, excludes, dest)
    elif mode == "-r":
        manifest = sources
        excludes = None
        restore(manifest, dest, excludes)
    else:
        print("Unknown Mode: " + mode)

# backup_module
**Advanced Fork from tompaton.**
--

  * **+** Switched to python3 syntax

  * **+** Compression of backup to .tar.gz

  * **+** skipping files which weren't modified since last backup

    * in manifest recursive link to previous backup all the way to the one storing the file
    
    * when restoring, traverse these links recursive

  * **-**  tom pathon's own path.py library and use official python3's pathlib

# backup_module
**Advanced Fork from tompaton.**
--

  * *o Switched to python3 syntax*

  * *+ Added Compression of backup to .tar.gz*

  * *+ Added skipping files which weren't modified since last backup*

    * *in manifest recursive link to previous backup all the way to the one storing the file*
    
    * *when restoring, traverse these links recursive*

*- removed tom pathon's own path.py library and use pathlib*
